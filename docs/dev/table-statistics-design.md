# Table Statistics — Design, Decisions, Roadmap

Living design document for the `table-statistics` branch. Captures the current POC design, the decisions behind it, the roadmap, and a narrative work log (per-commit history is in `git log`).

---

## 1. Problem statement

Apache Calcite's cost-based optimizer needs per-table and per-column statistics (row count, distinct-value cardinality, min/max) to produce good plans. Without them:

- `OpenSearchIndex.getStatistic()` returns `Statistics.UNKNOWN` → cost falls back to `maxResultWindow = 10 000` regardless of actual index size.
- `RelMdRowCount.getRowCount(Aggregate)` falls back to `inputRowCount / 10` when no `DistinctRowCount` handler is present → `stats count() by status` on a 137-doc/5-value index estimates `13.7` rows instead of `5`.
- Join reorder, filter pushdown ordering, and limit placement all make worse choices.

We need a statistics subsystem that (a) collects field-level stats asynchronously, (b) persists them durably, (c) serves them to Calcite with sub-millisecond latency during planning.

---

## 2. Architecture

```
                ┌─────────────────────────────────────────────────────┐
                │          SQL plugin (opensearch module)             │
                │                                                     │
 PPL query ─────┼──► Calcite optimizer                                │
                │     │                                               │
                │     │ getStatistic()            unwrap(Handler)     │
                │     ▼                                               │
                │   OpenSearchIndex ◄──────┐   ┌──────────────────┐   │
                │     │ (per-query inst.)  │   │ DistinctRowCount │   │
                │     │                    │   │     .Handler     │   │
                │     ▼                    │   └──────────────────┘   │
                │   TableStatisticStorage ─┘                          │
                │     │  (get/put + race protection)                  │
                │     │                                               │
                │     ▼                                               │
                │   TableStatisticCollector ─► NodeClient ─► sampler  │
                │     (refreshAsync)             aggregation          │
                │                                                     │
                │   REST: /_plugins/_sql/_statistics/{index}          │
                │   REST: /_plugins/_sql/_statistics/{index}/analyze  │
                └─────────────────────────────────────────────────────┘
                                        │
                                        ▼
                       ┌───────────────────────────────┐
                       │  .opensearch-statistics (SI)  │
                       │  doc_id = sha256(indexName)   │
                       └───────────────────────────────┘
```

### 2.1 Modules & wiring

The entire stack lives in the `opensearch/` module — no cross-plugin dependency. `SQLPlugin.createComponents` pre-instantiates the storage + collector singletons and threads them through every path that can build a `StorageEngine`:

- `OpenSearchDataSourceFactory` (4-arg ctor) — used by the **default** OpenSearch datasource, which is where production PPL queries live. This is not Guice-managed, so a `@Provides`-only design fails silently: the factory builds engines with null stats and `getStatistic()` returns `UNKNOWN` despite green unit tests (caught live in `199c16c91`). **Lesson:** unit tests verify code correctness; only a live cluster verifies feature correctness.
- `OpenSearchPluginModule` (3-arg ctor, Guice `@Provides`) — for injector-managed paths.
- `RestTableStatisticsAction` — for the REST handler.

### 2.2 Data model

```java
record FieldStatistic(
    String type,            // "keyword" | "long" | "date" | ...
    long   cardinality,     // approx distinct count (HLL)
    Object minValue,        // numeric / date, null otherwise
    Object maxValue,
    List<Object> topTerms,  // slot reserved; collector writes [] today
    double nullRatio        // consumed by SelectivityHandler; collector writes 0.0 today
)

class TableStatistic implements org.apache.calcite.schema.Statistic {
    long docCount;
    Map<String, FieldStatistic> fields;
    Instant lastUpdatedTime;
    // Double getRowCount() → (double) docCount
}
```

Stored document layout (in `.opensearch-statistics`):

```json
{
  "status": "COMPLETED" | "GENERATING" | "FAILED",
  "last_updated_time": "2026-04-21T11:57:17.927264Z",
  "doc_count": 137,
  "fields": {
    "status": { "type": "text",    "unique_count": 5,   "null_ratio": 0.0 },
    "latency":{ "type": "long",    "unique_count": 132, "min_value": 7.0, "max_value": 996.0, "null_ratio": 0.0 }
  }
}
```

- `fields` is `type: object` with `enabled: false` in the index mapping → no per-key Lucene indexing, just opaque source retrieval.
- `doc_id = sha256(indexName)` — opaque, collision-free, alias-safe.

### 2.3 Collection pipeline

A single search covers the entire flat field schema. The collector is fire-and-forget:

```
refreshAsync(name, fieldTypes)
  │
  ▼
storage.getRaw(name)
  ├── status==GENERATING && age < 10 min  →  skip (race protection)
  └── else
       ▼
     storage.putStatus(GENERATING)        // noop listener
       ▼
     nodeClient.search( matchAll + size=0 + trackTotalHits, with:
        - top-level min_X / max_X aggregations for every eligible X
          (Lucene BKD short-circuit → O(1) per segment, exact global extrema)
        - sampler{ shard_size=100 000, cardinality_X for every eligible X }
          (HLL on a bounded sample → approximate distinct count)
     )
       ▼
     ActionListener.onResponse
       ├── parseSearchResponse → TableStatistic
       │    └── storage.put(name, stat)
       └── onFailure → storage.putStatus(FAILED)
```

Why split: `MinAggregator` / `MaxAggregator` on numeric / date fields read `PointValues.getMinPackedValue` / `getMaxPackedValue` directly (BKD root), so they're cheap to run over the full data set and produce exact extrema; sampling them would have only given extrema of the sample. Cardinality is the only genuinely expensive part, and it's the only part that still needs the sampler.

Field-type handling:

| Mapping type | Aggregations issued |
|---|---|
| `Keyword`, `Long/Int/Short/Byte` | cardinality (sampler) + min + max (top-level) |
| `Float/HalfFloat/ScaledFloat/Double`, `Date/DateNanos` | min + max (top-level) |
| `Text` with `.keyword` sub-field | cardinality (sampler, on sub-field) |
| everything else | skipped |

Empty eligible set → sampler is omitted entirely (sampler with 0 sub-aggs is rejected by OpenSearch as `all shards failed`); we still get `trackTotalHits` for `doc_count` and any top-level min/max.

### 2.4 Read pipeline (optimizer side)

`OpenSearchIndex.getStatistic()` (memoized for the life of the instance — OpenSearchStorageEngine creates a fresh one per query):

1. Flag off → cache `UNKNOWN` and return.
2. `nodeClient` unavailable or storage null → cache `UNKNOWN`.
3. `storage.get(name)` with a 500 ms `CountDownLatch` budget.
4. Timeout / not-found / interrupt → cache `UNKNOWN` and trigger `refreshAsync`.
5. Stale (> 24 h) → return the stale stat **and** trigger `refreshAsync` in the background.
6. Fresh → return as-is.

The 500 ms latch bounds worst-case planning latency; any subsequent rule fire within the same query hits the in-memory cache.

### 2.5 Calcite metadata hook

Instead of registering a custom `RelMetadataProvider`, we use Calcite's built-in dispatch: `RelMdDistinctRowCount.getDistinctRowCount(TableScan, ...)` (and the analogous `RelMdSelectivity`) already calls `scan.getTable().unwrap(<Handler>.class)`. So `OpenSearchIndex.unwrap` returns a handler on demand — zero infrastructure change, no Janino code-gen, and the same extension point covers every future metadata kind.

- **`DistinctRowCount.Handler`**: single-column group → `min(fieldCard, tableRowCount)`; multi-column → `RelMdUtil.numDistinctVals(product, tableRowCount)`. Returns `null` when any predicate is present, letting Calcite fall back to its catch-all.
- **`Selectivity.Handler`**: `col = literal → 1/cardinality`, `IS NULL → nullRatio`, `IS NOT NULL → 1 - nullRatio`; AND conjuncts multiply; unsupported predicates fall back to `RelMdUtil.guessSelectivity`.

Together these feed `RelMdRowCount.getRowCount(Aggregate)` and `RelMdRowCount.getRowCount(Filter)`, so both `LogicalAggregate` and filtered scans track real stored values instead of Calcite's `inputRowCount / 10` / `0.15` defaults.

### 2.6 REST API

| Method | Path | Purpose |
|---|---|---|
| `GET`  | `/_plugins/_sql/_statistics/{index}` | Read persisted stat document (or 404) |
| `POST` | `/_plugins/_sql/_statistics/{index}/analyze` | Synchronous trigger of `refreshAsync` (request returns immediately, work runs in background) |

Both reject wildcards, comma-lists, `-/+/<>` prefixes — only a single concrete index name is accepted. This avoids ambiguity about what "stats for `logs-*`" even means.

### 2.7 Settings

- `plugins.calcite.enabled` (existing) — Calcite engine master switch.
- `plugins.calcite.table_statistics.enabled` — default `false`. Must be `true` *and* Calcite enabled for stats to affect planning.

---

## 3. Key decisions & trade-offs

Only the decisions that shape the design live here. Standard calls (async > sync, default-off flag, system-index > in-memory, status-marker > distributed lock, hardcoded TTL for POC) are taken for granted; they're mentioned in §2 where they apply.

### 3.1 Own the subsystem inside the SQL plugin (not consume ml-commons Index Insight)

The original plan was to read `STATISTICAL_DATA` from ml-commons' Index Insight. Two triggers forced a rewrite on first real-cluster test: (a) ml-commons objects crossing the plugin boundary fail `instanceof` — OpenSearch's per-plugin classloader loads the class twice; (b) sharing classloaders via `extendedPlugins = 'opensearch-ml;optional=true'` pulls in ml-commons' older Guava 32.1.3, which is missing `InternalFutureFailureAccess`.

Beyond the triggers, the coupling direction was wrong: statistics are fuel for the query optimizer, so the consumer shouldn't depend on an unrelated producer. Owning the collector in-plugin removes the classloader boundary, lets `FieldStatistic` evolve alongside the Calcite hook, and decouples our release cadence from ml-commons'. The cost is ~1k LOC of sampler + persistence code we now maintain, plus a system index to lifecycle-manage. In POC phase, control beats reuse.

### 3.2 Store `unique_count` only, not the HLL sketch

The sampler's `cardinality` agg is HLL++ under the hood and the internal sketch is serializable, so in principle we could persist the sketch bytes and `merge(HLL(A), HLL(B))` later — that's the ClickHouse `uniq` pattern. Today we store only the final `unique_count` long.

The Calcite handlers we've built only consume a number, so sketch bytes would be dead weight right now. At HLL precision 14 a sketch is ~16 KB per field, vs ~8 bytes for the number; committing to a persisted sketch format also creates forward-compat obligation in `StreamOutput`. Revisit when one of three consumers appears, all M3+: cross-shard reduction of per-shard sketches, filter-aware NDV (`status` distinct count under a region filter), or cross-index join cardinality via sketch intersection.

### 3.3 Refresh trigger: cron, not refresh-listener

A segment-refresh-triggered collector would be the "right" architecture — recollect exactly when data changes. It's not reachable from a plugin: `IndexModule` only exposes index/search/indexing event listeners, not `ReferenceManager.RefreshListener`; getting that listener requires replacing `EngineConfig` via `EnginePlugin.getEngineFactory`, which means shipping a full custom Engine.

Chosen: `ThreadPool.schedule` tick (default 60 s) iterates known indices and calls `refreshAsync` on anything past TTL. Same code path as the on-query trigger; stats aren't latency-critical so a 30–60 s lag is invisible to Calcite. Explicitly rejected: per-write HLL maintenance via `IndexingOperationListener.postIndex` — ~1% CPU continuously burned for freshness we don't need.

Could core do this better? Yes — `ReferenceManager.RefreshListener` on `EngineConfig.externalRefreshListener` would unlock a per-refresh callback. But with default `refresh_interval=1s` on write-heavy indices, you'd still set a dirty bit and consume it from a background loop — i.e. a cron with a dirty-set optimization, just installed one layer deeper. Upstreaming that loses the SQL plugin's release cadence and adds a cross-subsystem API to maintain, for a freshness gain Calcite can't exploit. Revisit only if a non-Calcite consumer emerges that needs per-second stats.

### 3.4 Range selectivity: min/max formula first, histogram later, `_count` probably never

Phase 1b extends `TableStatisticSelectivityHandler` to cover `>`, `>=`, `<`, `<=`, `BETWEEN` via the existing `FieldStatistic.rangeSelectivity(low, high)`, assuming uniform distribution over `[min, max]`.

| Option | Accuracy uniform / skewed | Latency | Storage | Status |
|---|---|---|---|---|
| **A.** min/max linear formula | ±5% / awful (≤50× off on 99-1 tail) | ~1 µs | 2 longs/field | chosen |
| **B.** per-plan `_count` RPC | exact / exact | 5–50 ms per predicate | 0 | rejected |
| **C.** equi-height histogram | ±1% / ±3% | µs (O(log B)) | ~1 KB/field | deferred |
| **D.** t-digest | ±1% tails / ±1% tails | µs | ~4 KB/field | rejected |

A beats B because Calcite asks selectivity 3–5× per planning session; a per-predicate `_count` adds 30–150 ms to every query and shifts load onto the cluster during burst planning — and it still doesn't give post-filter NDV, so it isn't a terminal answer. A's skew blind-spot only hurts plan *quality*, not correctness, and with today's single-table-pushdown-only workloads Calcite has no alternative plans to pick between anyway.

C is the long-term answer (buckets absorb skew, µs lookup, ~1 KB/field, same sampler collection path just swap min/max → percentiles) but is deferred until multi-plan candidate generation (M3+) makes the accuracy matter. D costs the same to implement with no meaningful win over C because OpenSearch has BKD min/max at the Lucene level — ClickHouse chose t-digest partly because it doesn't. B may come back in M4 as a tie-breaker when the cost model is on a knife-edge between plans; paying the RPC only on those queries avoids the per-query tax.

---

## 4. Roadmap

### Milestone 1 — POC (DONE, 2026-04-21)

On branch `table-statistics`. 13 commits + 1 follow-up fix.

- [x] `FieldStatistic` record + parse/serialize
- [x] `TableStatistic` implements `Calcite Statistic`
- [x] `TableStatisticStorage` — CRUD on `.opensearch-statistics` (sha256 doc_id)
- [x] `TableStatisticCollector` — async sampler-backed collection, race protection
- [x] `OpenSearchIndex.getStatistic()` — 500 ms read, UNKNOWN cache, async refresh on miss/stale
- [x] REST `/_plugins/_sql/_statistics/{index}` GET + POST /analyze
- [x] Flag `plugins.calcite.table_statistics.enabled` (default false)
- [x] Guice + `OpenSearchDataSourceFactory` wiring (commit 199c16c91)
- [x] Aggregate row count fix via `unwrap`-based `DistinctRowCount.Handler` (commit eef8697c1)
- [x] Live cluster verification: `stats count() by status` → rowcount 5.0 (was 13.7)

### Milestone 2 — Producer-side hardening

M2 is split into phases by ROI and implementation cost.

#### Phase 1a — Pull min/max out of the sampler (DONE)

- [x] `TableStatisticCollector.buildAggregationRequest`: emit `min` / `max` as **top-level** aggregations, keep `cardinality` inside the sampler.
- [x] `parseSearchResponse`: read min/max from the top-level `Aggregations`, cardinality from the sampler's sub-aggs.
- [x] Unit tests updated; all pass.
- [x] Live cluster verify on `spike-big` (50 k docs): `latency.min_value = 0.0`, `max_value = 999.0` — exact (true extrema). Previously sampler-bounded.
- [x] Deleted `statistics/spike/`, `RestSegmentStatSpikeAction`, `ShardRegistry`, and the `SQLPlugin.onIndexModule` wiring. They served their purpose — the spike measurements in §5 (2026-04-22) validated that `PointValues.getMin/MaxPackedValue` is O(1) per segment — but the production path doesn't need our own transport layer since `MinAggregator` / `MaxAggregator` already call those same Lucene APIs.

**ROI:** exact min/max at the full data scale, no new transport action, no plugin-API surface to maintain. ~50 LOC delta.

*(Earlier plan routed min/max through a self-built broadcast reader over `IndexShard.acquireSearcher`. The spike proved the Lucene short-circuit exists, but it also turns out `MinAggregator` / `MaxAggregator` already use that short-circuit, so lifting them out of the sampler is enough — we never needed the broadcast.)*

#### Phase 1b — Range selectivity in `TableStatisticSelectivityHandler` (DONE)

- [x] Handle `>`, `>=`, `<`, `<=` via linear interpolation over stored `[min, max]`. Literal-on-left form flips the operator. Single-value columns and missing stored min/max fall back to Calcite's `guessSelectivity`.
- [x] `RexLiteral.getValueAs(Number.class)` for literal coercion — works for numeric and date-epoch-millis. String ranges deferred (we don't collect keyword min/max yet).
- [x] `RexUtil.expandSearch` at the top of `getSelectivity` — Calcite's `RexSimplify` collapses multiple comparisons on the same column into `SEARCH(col, Sarg[...])`, and without expansion the conjunct decomposition would treat the whole Sarg as one opaque predicate. Expanding first means `BETWEEN` (and any `AND(<=, >=)` on the same column) gets the stat-backed treatment on both bounds.
- [x] Unit tests: 8 new cases covering `>`, `>=`, `<`, `<=`, literal-on-left, outside-range clamp, missing stats, single-value column, SEARCH/Sarg expansion.
- [x] Live verify on `poc-v4` (`latency ∈ [7, 996]`, 137 docs): `latency > 500` → `rowcount 68.71`; `latency < 200` → `26.74`; `latency BETWEEN 200 AND 800` → `88.41`; `latency > 200 AND latency < 800 AND status = 'OK'` → `17.68`. All match the expected `doc_count × stat_selectivity` product to 3 decimals.

**Known limitation:** uniform-distribution assumption is poor on skewed data (see §3.4). Histogram support is the fix; gated on multi-plan candidate generation in M3+.

#### Phase 2 — Cron-driven refresh

- [ ] `ThreadPool.schedule` tick (default interval 60 s, configurable via `plugins.calcite.table_statistics.refresh_interval`).
- [ ] Track known indices via `ClusterStateListener` or enumerate on tick.
- [ ] Per-index TTL staleness check (reuses existing `TableStatistic.isStale(ttl)`).
- [ ] Concurrent-refresh throttling: cap per node (default 4), backpressure-aware.
- [ ] See §3.3 for why this is NOT a refresh-listener-based design.

#### Phase 3 — Collector quality-of-service

- [ ] **Sampler tuning.** `shard_size = 100 000` is arbitrary. Benchmark + document, or make it a setting.
- [ ] **Null ratio + top terms.** Slots exist in `FieldStatistic`; collector ignores them. Populate via additional sub-aggs.
- [ ] **System-index lifecycle.** Currently lazy-created, no version / mapping migration. Needs ISM policy or bootstrap versioning.
- [ ] **TTL → setting.** Promote hardcoded 24 h to `plugins.calcite.table_statistics.ttl`.

#### Deferred (not in M2)

- **HLL sketch storage** — see §3.2. Until we need cross-shard reduction, per-partition stats, or sketch-based filter-aware NDV, `unique_count` as a number is sufficient.
- **Refresh-listener-based triggering** — see §3.3. Plugin API doesn't expose it, and cron gives us sufficient freshness without write-path tax.

### Milestone 3 — Consumer-side (optimizer integration)

These were captured in the POC plan's "Consumer-side Future Work" section. #2 and #3 (equality+null+range) are done; the rest are open.

- [x] **#2 Aggregate row count** — `DistinctRowCount.Handler` via `unwrap`. Shipped (`eef8697c1`).
- [x] **#3 Filter selectivity (equality/null)** — `Selectivity.Handler` via `unwrap`. Shipped (`545bc7d63`): `col = literal → 1/cardinality`, `IS NULL → nullRatio`, `IS NOT NULL → 1 - nullRatio`.
- [x] **#3 Filter selectivity (range)** — M2 Phase 1b, linear interpolation over stored `[min, max]` + `RexUtil.expandSearch` for `BETWEEN` / Sarg.
- [ ] **#1 TableScan row count override.** Right now `AbstractCalciteIndexScan.estimateRowCount` reads `TableStatistic.getRowCount()` via `getStatistic()`. That is good. But `maxResultWindow` is still the fallback — promote the stored `doc_count` higher up so even non-Calcite paths benefit.
- [ ] **#4 Histogram-backed selectivity** — replace the uniform-distribution min/max formula with equi-height histograms (~50 buckets per numeric/date field). See §3.4. Gated on real ROI, expected to be driven by multi-plan candidate generation in JOIN scenarios.
- [ ] **#5 Join reorder hints.** With both sides carrying stats, we can reorder joins by smaller-input-first. Requires the metadata handlers above plus verified handling of non-null join predicates.
- [ ] **#6 `RareTop` / `top_n` quality.** `RareTopDigest` currently uses a fixed factor; with cardinality we can short-circuit pushdown decisions.

### Milestone 4 — Productionization

- [x] **Integration tests.** Covered by `TableStatisticsIT` (`9fed71941`). Exercises flag off / flag on without analyze / after analyze / REST read.
- [ ] **Replace ml-commons plan references.** The `docs/superpowers/plans/2026-04-20-index-insight-ppl-integration*.md` files are historical. Either archive or convert into a retrospective.
- [ ] **Permission model.** Who is allowed to call `POST /_statistics/{index}/analyze`? Today it inherits the SQL plugin ACL; a dedicated security action may be warranted.
- [ ] **Metrics.** Expose `stat_refresh_total{status}`, `stat_refresh_latency_ms`, `stat_read_timeout_total` through the existing SQL metrics surface.
- [ ] **Documentation.** User-facing doc under `docs/user/` describing the flag, the REST API, and operational guidance. Dev doc (this file) lives under `docs/dev/`.

### Milestone 5 — Upstream PR

- [ ] Rebase to clean up the exploratory commits (the `IndexInsight*` → `TableStatistic*` rename path makes the history longer than it needs to be).
- [ ] Split into reviewable chunks if the reviewer prefers: (1) collector + storage, (2) Calcite wiring, (3) REST API, (4) metadata handlers.
- [ ] Open upstream PR against `opensearch-project/sql:main`.

---

## 5. Work log

Narrative only — per-commit history is on the `table-statistics` branch (`git log --oneline`). Entries here capture decisions, measurements, and pivots that don't fit in a commit message.

### 2026-04-23 — M2 Phase 1a + 1b shipped + consolidation

- **Phase 1a (exact min/max).** Original plan was to build a `ShardRegistry` + broadcast transport action so the collector could drive `PointValues.getMin/MaxPackedValue` directly from each node's Lucene segments. On re-reading the aggregation framework, realized `MinAggregator` / `MaxAggregator` already call those same BKD APIs — the only reason our old results looked sampled was that we'd nested them under the sampler. Pulled min/max out to top-level aggregations, kept cardinality under the sampler. ~50 LOC change instead of ~400. `spike-big` (50 k docs) now reports `latency ∈ [0, 999]` (exact) — previously sampler-bounded. Deleted the spike code and the `onIndexModule` wiring now that we no longer need direct shard access.
- **Phase 1b (range selectivity).** Extended `TableStatisticSelectivityHandler` to handle `>`, `>=`, `<`, `<=` via linear interpolation over stored `[min, max]`. Landmine surfaced during the cluster verify: Calcite's `RexSimplify` collapses `x > 200 AND x < 800` into `SEARCH(x, Sarg[(200..800)])`, which made the handler see the whole range as one opaque conjunct and return a useless 0.25 guess. Fix: call `RexUtil.expandSearch` at the top of `getSelectivity` before conjunct decomposition. Without this, Phase 1b would only have fired on single-sided predicates (`x > 500`) — in practice most range queries are two-sided, so the expansion is load-bearing. Live verify on `poc-v4`: `latency BETWEEN 200 AND 800` → `rowcount 88.41` (was `34.25` from guess); `latency > 200 AND latency < 800 AND status = 'OK'` → `17.68` (mixes three stat-backed factors).
- **Doc.** Collected the scattered decision rationale from session transcripts, plan files, and commit messages into this document. Extended with explicit rejection analysis for three calls that had only been discussed ad-hoc: storing HLL sketch bytes (§3.2), cron vs refresh-listener (§3.3), and range-selectivity estimation (§3.4). Rewrote M2 into explicit phases.

### 2026-04-22 — Spike: Lucene segment reads from a plugin

Built `spike/ShardRegistry` + `spike/SegmentStatReader` + `POST /_plugins/_sql/_statistics/{index}/_segment_read` to measure what a plugin can cheaply pull from Lucene directly. Measurements on `poc-v4` (137 docs) and `spike-big` (50 k docs):

- **`doc_count`**: exact, O(1).
- **Numeric `min`/`max`** via `PointValues.getMinPackedValue`/`getMaxPackedValue`: exact, sub-µs per shard. Previously the sampler gave extrema of the *sample*, not the data.
- **Keyword cardinality** via `sum(Terms.size())`: upper bound, tight only when per-segment cardinality ≈ global cardinality. A 7-segment index with 5 distinct statuses summed to 34; force-merging to 1 segment returned the exact 5.
- **Wall-clock**: ~100 µs per shard multi-segment, ~70 µs single-segment — two orders of magnitude faster than the sampler RPC.

Conclusion: segment reads are strictly better than sampler for doc_count and numeric min/max (drives Phase 1a); keyword NDV still needs HLL (motivates keeping the sampler path). Also validated the cron-trigger decision in §3.3 — segment reads are cheap enough to rescan the whole node every 30 s.

### 2026-04-22 — First integration test

`TableStatisticsIT` (`9fed71941`) covers flag off / on-before-analyze / on-after-analyze / REST GET. Writing the test exposed that the POC REST `/analyze` handler passed an empty field map to the collector, producing doc-count-only stats. Root cause: mapping lookup was happening on the transport thread where a blocking call is illegal. Fix resolves mappings on `ThreadPool.GENERIC` before calling `refreshAsync`.

### 2026-04-21 — End-to-end working

Two consumer-side hooks landed and were verified on the live cluster:

- **Aggregate row count** (`eef8697c1`, §2.5). `stats count() by status` on `poc-v4` went from `rowcount=13.7` (Calcite default `input/10`) to `5.0` (stored `status.unique_count`). Multi-column `by status, latency` → `123.8` via `numDistinctVals(5×132, 137)`.
- **Filter selectivity (equality / null)** (`545bc7d63`). Using `OpenSearchIndex.unwrap` again for `Selectivity.Handler`. Live numbers on `poc-v4`: `status = 'OK'` 20.55 → 27.4 (1/5 instead of default 0.15); `status = 'OK' AND latency > 100` 10.275 → 13.7 (stat 0.2 × heuristic 0.5); `isnotnull(status)` 123.3 → 137 (1 − nullRatio).

The wiring fix to get this working in production (`199c16c91`) is called out as a lesson in §2.1: Guice `@Provides` never fires for the default OpenSearch datasource, so the two singletons had to be pre-instantiated in `SQLPlugin.createComponents` and threaded through `OpenSearchDataSourceFactory` directly.

### 2026-04-20 — Pivot

`docs/superpowers/plans/2026-04-20-index-insight-ppl-integration{,-context}.md` describe the original ml-commons-consumer design. After cluster verification hit the classloader + Guava issues captured in §3.1, the work moved to this branch with a SQL-internal implementation. Those plan documents remain as historical context.

### 2026-04-17 — Hackathon commitment

Chose "OpenSearch Statistics Metadata Framework" as the hackathon project, originally framed as "build ml-commons Index Insight consumer." Pivoted after the 04-20 spike.

---

## 6. Appendix A — Lucene segment metadata access from a plugin

Reference for the plugin-API boundary. Measurements and the `sum(Terms.size())` skew rule-of-thumb are in §5 (2026-04-22 spike entry).

### What a plugin can reach

| API | What it gives us | Latency on warm reader |
|---|---|---|
| `IndexModule.addIndexEventListener` → `afterIndexShardStarted(IndexShard)` / `afterIndexShardClosed` | Live `IndexShard` reference per shard with lifecycle events. | n/a (event) |
| `IndexShard.acquireSearcher(String)` → `Engine.Searcher.getIndexReader()` | Ref-protected read handle. Must be closed; holds segments from being merged away. | ~µs to acquire |
| `LeafReader.terms(field).size()` | Per-segment distinct term count (keyword/text). Sum across segments is an upper bound on global NDV — tight only when per-segment NDV ≈ global NDV; force-merging gives exactness. | O(1) |
| `LeafReader.terms(field).getDocCount()` | Per-segment docs with ≥ 1 value for the field. Sum is exact. | O(1) |
| `LeafReader.getPointValues(field).getMin/MaxPackedValue` | Per-segment BKD root min/max for numeric/date. Decode with `NumericUtils.sortableBytesToLong`. Global extrema = extrema across segments. | O(1) |
| `IndexReader.maxDoc()` | Per-segment doc count including deleted. | O(1) |

### What a plugin cannot reach

- `IndicesService` — not exposed to any plugin extension point.
- `ReferenceManager.RefreshListener` — only installable via `EngineConfig`, which requires `EnginePlugin.getEngineFactory` (i.e. shipping a full Engine).
- `EngineConfig.{internal,external}RefreshListener` — package-private.
- Per-doc refresh-completion hook — doesn't exist; refresh is shard-level.

### Numeric NDV caveat

Numeric fields in OpenSearch are indexed via `PointValues` only, with no inverted-index `Terms`. Segment reads give exact min/max but no distinct count — numeric NDV still has to go through the sampler's HLL (or a full `NumericDocValues` scan, same cost as a full agg). Phase 1a therefore uses segment reads for min/max only and keeps the sampler for cardinality.

---

*Maintained alongside the `table-statistics` branch. Update Section 4 (Roadmap) as milestones complete, and append to Section 5 (Work log) after each substantive commit or decision.*
