# Table Statistics POC (SQL-native, Calcite cost integration)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the ml-commons-dependent Index Insight integration with a SQL-internal table statistics subsystem. The PPL Calcite optimizer consumes real per-index/per-field statistics via Calcite's `Statistic` interface. Data is collected by issuing aggregation requests directly against the target index, persisted in a new system index `.opensearch-statistics`, and served asynchronously — the optimizer never blocks on collection.

**Background:** An earlier plan integrated with ml-commons' Index Insight feature. Local cluster validation exposed two structural problems: (1) cross-plugin ClassLoader isolation breaks strongly-typed transport calls; (2) `extended.plugins` to share classes triggers Guava version conflicts. A deeper review also showed ml-commons' LLM-filter behaviour (keeps only the "top 30 important columns") is wrong for cost-based optimization — CBO needs statistics for any field that might appear in a predicate, not just the LLM-important ones. This plan moves collection in-house and drops the ml-commons dependency entirely.

**Tech Stack:** Java 21, Apache Calcite (`Statistic`), OpenSearch `NodeClient` + aggregations (matchAll + sampler + cardinality + min/max), Gson (content JSON), JUnit 5 + Mockito.

**Worktree:** `/Volumes/workplace/OpenSearch/index-insight/` (branch `table-statistics`, forked from `f9c6c812a` of the earlier `index-insight` branch). All implementation changes and `./gradlew` commands happen here.

**Design decisions (already aligned with user):**

| Decision | Choice |
|---|---|
| Feature flag | `plugins.calcite.table_statistics.enabled`, default `false` (opt-in during POC) |
| Collection timing | **Async only** — `getStatistic()` returns `Statistics.UNKNOWN` on first hit, triggers background collect; second hit onward returns persisted stats |
| Persistence | System index `.opensearch-statistics` (per-node in-memory cache also kept for hot path) |
| TTL | 24h hardcoded |
| Field filtering | **No filtering in POC** (all fields get aggregations); add soft limits later if needed |
| REST API | `POST /_plugins/_sql/_statistics/{index}/analyze` (trigger async collect) + `GET /_plugins/_sql/_statistics/{index}` (read stored stat) |
| Naming | All classes named `TableStatistic*` (not `IndexInsight*`). Drop ml-commons terminology entirely. |
| ml-commons dependency | **None.** No imports of `org.opensearch.ml.*`. No `extended.plugins` entry for opensearch-ml. |

**Future Work (explicitly out of scope for POC):**

1. Cron-based scheduled refresh (REST endpoint makes this trivial to bolt on later)
2. Field filtering / cardinality caps for very wide indices (>1000 fields)
3. Incremental / delta refresh (only re-compute changed fields)
4. Multi-index patterns (this POC keeps the single-index assumption)
5. Per-field null ratio (ml-commons' `not_null` aggregation — skipping for simplicity)

---

## File Structure

Legend: **Create** = new file. **Modify** = edit existing file. **Delete** = remove.

| Action | File (relative to worktree root) | Responsibility |
|--------|----------------------------------|----------------|
| **Create** | `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/TableStatistic.java` | Calcite `Statistic` impl backed by collected per-field stats + row count |
| **Create** | `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticStorage.java` | CRUD against `.opensearch-statistics` system index |
| **Create** | `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticCollector.java` | Orchestrator: build/send aggregation request, parse response, write to storage |
| **Modify** | `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/FieldStatistic.java` | Keep as-is (may add a builder helper for direct construction) |
| **Modify** | `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/OpenSearchIndex.java` | Rewrite `getStatistic()` to load from storage (sync, fast) + trigger async refresh on miss/stale |
| **Modify** | `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/scan/AbstractCalciteIndexScan.java` | Change `instanceof IndexInsightStatistic` → `instanceof TableStatistic` |
| **Modify** | `common/src/main/java/org/opensearch/sql/common/setting/Settings.java` | Replace `INDEX_INSIGHT_STATISTICS_ENABLED` enum constant with `TABLE_STATISTICS_ENABLED` |
| **Modify** | `opensearch/src/main/java/org/opensearch/sql/opensearch/setting/OpenSearchSettings.java` | Replace `INDEX_INSIGHT_STATISTICS_ENABLED_SETTING` registration with `TABLE_STATISTICS_ENABLED_SETTING` |
| **Create** | `plugin/src/main/java/org/opensearch/sql/plugin/rest/RestTableStatisticsAction.java` | REST handler for `/_plugins/_sql/_statistics/{index}` (GET + POST/analyze) |
| **Modify** | `plugin/src/main/java/org/opensearch/sql/plugin/SQLPlugin.java` | Register `RestTableStatisticsAction` + wire `TableStatisticCollector` singleton |
| **Delete** | `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/IndexInsightStatistic.java` | Replaced by `TableStatistic` |
| **Delete** | `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/IndexInsightStatisticProvider.java` | Replaced by `TableStatisticCollector` |
| **Create** | `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticTest.java` | Unit tests for `TableStatistic` |
| **Create** | `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticStorageTest.java` | Unit tests for the storage layer |
| **Create** | `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticCollectorTest.java` | Unit tests for the async collector |
| **Modify** | `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/OpenSearchIndexTest.java` | Rewrite `getStatistic*` tests for new async path |
| **Modify** | `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/scan/CalciteIndexScanCostTest.java` | Change test stubs to use `TableStatistic` |
| **Modify** | `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/IndexInsightStatisticIntegrationTest.java` | Rename + update to `TableStatisticIntegrationTest` |
| **Delete** | `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/IndexInsightStatisticTest.java` | Replaced by `TableStatisticTest` |
| **Delete** | `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/IndexInsightStatisticProviderTest.java` | Replaced by `TableStatisticCollectorTest` |

---

## Persisted document schema (`.opensearch-statistics`)

Doc id: `sha256(indexName)` (hex lowercase).

```json
{
  "index_name":       "logs-2024-04",
  "status":           "COMPLETED",       // GENERATING | COMPLETED | FAILED
  "last_updated_time": "2026-04-21T05:17:42.123Z",
  "doc_count":        123456,
  "fields": {
    "status":  { "type": "keyword", "cardinality": 5,    "terms": ["200","301","404"] },
    "latency": { "type": "long",    "cardinality": 8500, "min": 1.0, "max": 30000.0 }
  }
}
```

The top-level `fields` map mirrors `Map<String, FieldStatistic>` but uses compact key names for storage efficiency. `TableStatistic.fromStoredDoc(Map)` is responsible for parsing this back into objects.

Schema is created lazily by `TableStatisticStorage.ensureIndexExists()` on first write. Mapping is defined inline in the storage class (no separate JSON resource file — keeps the POC self-contained).

---

## Task 1: `TableStatistic` — Calcite Statistic implementation

**Why first:** It has no dependencies beyond `FieldStatistic` (already exists). Gives us the typed shape that storage and collector produce/consume.

**Files:**
- Create: `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/TableStatistic.java`
- Create: `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticTest.java`

Responsibilities:

1. Implement `org.apache.calcite.schema.Statistic` (returns `rowCount`, empty `keys` and `collations` lists — same shape as current `IndexInsightStatistic`).
2. Provide static factory `fromFields(long docCount, Map<String, FieldStatistic> fields)`.
3. Provide static factory `fromStoredDoc(Map<String, Object> sourceMap)` that parses the persisted schema back into a `TableStatistic`. Defensive: missing `fields` → empty map; missing `doc_count` → throw `IllegalArgumentException` (unlike the ml-commons version, our own persisted docs MUST have doc_count — we control the writer).
4. Provide `toStoredDocSource()` returning a `Map<String, Object>` ready for `IndexRequest.source(...)`. Includes `status` field set to `"COMPLETED"` and `lastUpdatedTime` set to `Instant.now().toString()`.
5. Provide `isStale(Duration ttl)` returning `true` if `Instant.now() - lastUpdatedTime > ttl`. For POC, callers pass `Duration.ofHours(24)`.

Tests:

- [ ] Step 1: Write failing test covering: `fromFields` roundtrip, `fromStoredDoc` success + missing keys, `toStoredDocSource` shape, `isStale` logic, `getRowCount` / `getKeys` / `getCollations` defaults
- [ ] Step 2: Run `./gradlew opensearch:test --tests "...TableStatisticTest" --no-build-cache` — expect compilation failure
- [ ] Step 3: Implement `TableStatistic`
- [ ] Step 4: Run tests — expect all pass
- [ ] Step 5: `./gradlew :opensearch:spotlessApply`
- [ ] Step 6: Commit:
  ```
  feat: add TableStatistic (Calcite Statistic backed by collected per-table stats)
  ```

---

## Task 2: `TableStatisticStorage` — `.opensearch-statistics` CRUD

**Why second:** Collector depends on it. Pure I/O component, easily unit-testable.

**Files:**
- Create: `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticStorage.java`
- Create: `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticStorageTest.java`

Responsibilities:

1. Constants: `STORAGE_INDEX = ".opensearch-statistics"`; doc id = `sha256(indexName)`.
2. `ensureIndexExists(ActionListener<Boolean>)`: check if the index exists; if not, create it with the mapping defined inline. Idempotent. Invoked before every write; invoked lazily.
3. `get(String indexName, ActionListener<Optional<TableStatistic>>)`: `GetRequest(STORAGE_INDEX, docId(indexName))`. On success: parse via `TableStatistic.fromStoredDoc`; if doc missing, return `Optional.empty()`. On failure (including `IndexNotFoundException`), return `Optional.empty()` (and log at `debug`).
4. `put(String indexName, TableStatistic stat, ActionListener<Void>)`: ensure index exists, then `IndexRequest(STORAGE_INDEX).id(docId).source(stat.toStoredDocSource())` with `opType=INDEX` (upsert-style). Forward success/failure.
5. `putStatus(String indexName, String status, ActionListener<Void>)`: lightweight write used by collector to mark `GENERATING` at the start of a refresh (prevents duplicate concurrent collects).

Status key reference:

- `"GENERATING"`: a collect is in progress for this index. Set at start of `collect()`, cleared by overwriting with `"COMPLETED"` on success or `"FAILED"` on error.
- `"COMPLETED"`: usable result.
- `"FAILED"`: last collect failed; `lastUpdatedTime` indicates when so we can re-try after a delay.

Tests (all use mocked `NodeClient`):

- [ ] `get_docExists_returnsStatistic`
- [ ] `get_docMissing_returnsEmpty`
- [ ] `get_indexNotFound_returnsEmpty` (simulates `IndexNotFoundException`)
- [ ] `put_ensuresIndexExists_thenIndexesDocument`
- [ ] `put_indexAlreadyExists_skipsCreation`
- [ ] `docId_sha256OfIndexName_64HexChars`

Tasks:

- [ ] Step 1: Write failing tests
- [ ] Step 2: Run tests — expect compile failure
- [ ] Step 3: Implement storage
- [ ] Step 4: Run tests — expect all pass
- [ ] Step 5: `spotlessApply`
- [ ] Step 6: Commit:
  ```
  feat: add TableStatisticStorage for .opensearch-statistics CRUD
  ```

---

## Task 3: `TableStatisticCollector` — async collect + persist

**Files:**
- Create: `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticCollector.java`
- Create: `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticCollectorTest.java`

Responsibilities:

1. Constructor: `TableStatisticCollector(NodeClient, TableStatisticStorage)`.
2. Public API:
   - `refreshAsync(String indexName)`: fire-and-forget. Returns `void`. Internally:
     1. Call `storage.get(indexName)`. If returns a record with status `GENERATING` and `lastUpdatedTime` within the last 10 minutes → skip (race protection). Otherwise proceed.
     2. `storage.putStatus(indexName, "GENERATING")` (best-effort; ignore failure).
     3. Build aggregation request: `matchAllQuery` + `size(0)` + `trackTotalHits(true)` + sampler + per-field aggs (cardinality for keyword/long/int/short/text, min/max for long/int/short/float/double/date).
     4. Execute search. On response: parse to `TableStatistic`; call `storage.put(indexName, stat)`; log success at DEBUG.
     5. On failure: call `storage.putStatus(indexName, "FAILED")`; log at WARN.
3. Private `buildAggregationQuery(Map<String, String> fieldsToType)` method that composes the same aggregation shape documented above. Unit-testable via package-private visibility.
4. Private `parseAggregationResponse(SearchResponse)` returns `TableStatistic`. Extracts `totalHits.value()` as `rowCount` and iterates the sub-aggregations to build `Map<String, FieldStatistic>`.
5. Requires index mapping to know field types. Either:
   - (a) Accept a `Map<String, String> fieldsToType` from the caller (simpler; caller is `OpenSearchIndex` which already has `getFieldTypes()`).
   - (b) Fetch mapping internally via `GetMappingsRequest` (more self-contained).
   - **Choice: (a).** OpenSearchIndex already has the type map; passing it in avoids another network round-trip.
   - Updated signature: `refreshAsync(String indexName, Map<String, String> fieldsToType)`.

Tests:

- [ ] `refreshAsync_buildsCorrectAggregationRequest` — ArgumentCaptor on `nodeClient.search(...)`, verify shape: matchAll, size=0, trackTotalHits, expected sub-aggs
- [ ] `refreshAsync_onSuccess_writesCompletedToStorage` — mock `NodeClient.search` success; verify `storage.put` called with `TableStatistic` containing expected `rowCount`
- [ ] `refreshAsync_onFailure_writesFailedStatus`
- [ ] `refreshAsync_whileGenerating_isNoop` — pre-populate storage with `GENERATING` + recent timestamp, verify no search issued
- [ ] `refreshAsync_staleGenerating_triggersNewCollect` — `GENERATING` > 10 min old → re-runs

Tasks:

- [ ] Step 1: Write failing tests
- [ ] Step 2: Run tests — expect compile failure
- [ ] Step 3: Implement collector
- [ ] Step 4: Run tests — expect all pass
- [ ] Step 5: `spotlessApply`
- [ ] Step 6: Commit:
  ```
  feat: add TableStatisticCollector for async collection + persistence
  ```

---

## Task 4: New setting key — replace `INDEX_INSIGHT_STATISTICS_ENABLED`

**Files:**
- Modify: `common/src/main/java/org/opensearch/sql/common/setting/Settings.java`
- Modify: `opensearch/src/main/java/org/opensearch/sql/opensearch/setting/OpenSearchSettings.java`

Changes:

1. Rename enum constant `INDEX_INSIGHT_STATISTICS_ENABLED("plugins.calcite.index_insight_statistics.enabled")` → `TABLE_STATISTICS_ENABLED("plugins.calcite.table_statistics.enabled")`.
2. In `OpenSearchSettings.java`: rename `INDEX_INSIGHT_STATISTICS_ENABLED_SETTING` → `TABLE_STATISTICS_ENABLED_SETTING`, key `plugins.calcite.table_statistics.enabled`, default `false`, NodeScope + Dynamic.
3. Update `register(...)` call and `pluginSettings()` list.

No new tests for this task — it's a mechanical rename; end-to-end coverage comes in Task 5.

- [ ] Step 1: Make the renames
- [ ] Step 2: `./gradlew :opensearch:compileJava :common:compileJava` — expect BUILD SUCCESSFUL
- [ ] Step 3: `spotlessApply`
- [ ] Step 4: Commit:
  ```
  feat: rename INDEX_INSIGHT_STATISTICS_ENABLED → TABLE_STATISTICS_ENABLED
  ```

---

## Task 5: Rewire `OpenSearchIndex.getStatistic()` for async path

**Files:**
- Modify: `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/OpenSearchIndex.java`
- Modify: `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/scan/AbstractCalciteIndexScan.java`
- Modify: `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/OpenSearchIndexTest.java`
- Modify: `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/scan/CalciteIndexScanCostTest.java`
- Modify: `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/IndexInsightStatisticIntegrationTest.java` → rename to `TableStatisticIntegrationTest.java`

New `getStatistic()` flow:

```
if (cachedStatistic != null) return cachedStatistic;
if (flag off) return Statistics.UNKNOWN;
if (NodeClient absent) return Statistics.UNKNOWN;

// Sync read (fast — a single document GET)
storage.get(indexName).whenComplete(result -> { ... });
// In POC we use a CountDownLatch with a short (500 ms) timeout for the read only.
// If storage read times out or returns empty/stale → fire async refresh, return UNKNOWN.
// If storage read returns fresh data → cache and return.
```

Concrete logic:

1. Keep `cachedStatistic` field (`Statistic`, non-volatile — consistent with existing in-memory caches in this class).
2. Synchronous storage read with 500 ms budget (uses `CountDownLatch` + `AtomicReference<Optional<TableStatistic>>`).
3. If read surfaces a non-stale `TableStatistic`, cache + return.
4. If read surfaces stale data: cache the stale result (optimizer still gets something useful), **and** trigger `collector.refreshAsync(...)`. Next query after refresh finishes will pick up the fresh data (new `OpenSearchIndex` instance created by storage engine — see caveat below).
5. If read returns empty or fails: trigger `collector.refreshAsync(...)`, return `Statistics.UNKNOWN`.

**Caveat:** `OpenSearchIndex` instance is cached per-query by `OpenSearchStorageEngine`, so `cachedStatistic` only lives for the duration of one query. Subsequent queries construct a new `OpenSearchIndex`, which re-runs the storage read. That's the whole point of persistence — the storage layer is the authoritative cache, `cachedStatistic` is a within-query memoization.

Wiring:

- `OpenSearchIndex` gains two new constructor parameters: `TableStatisticStorage storage` and `TableStatisticCollector collector`. Both nullable for backwards-compat with existing tests that construct `OpenSearchIndex` directly.
- If either is `null`, `getStatistic()` returns `Statistics.UNKNOWN` unconditionally.
- `OpenSearchStorageEngine` (constructor-wires `OpenSearchIndex`) is updated to pass them through — **both instances come from `SQLPlugin` via Guice** (Task 6).

`AbstractCalciteIndexScan.getBaselineRowCount()`:

- Change `instanceof IndexInsightStatistic` → `instanceof TableStatistic`.

Tests to update:

- [ ] `OpenSearchIndexTest.getStatistic_whenFlagOff_returnsUnknown` (unchanged semantics)
- [ ] `OpenSearchIndexTest.getStatistic_whenStorageMiss_returnsUnknownAndTriggersRefresh`
- [ ] `OpenSearchIndexTest.getStatistic_whenStorageHit_returnsTableStatistic`
- [ ] `OpenSearchIndexTest.getStatistic_whenStorageStale_returnsStaleButRefreshes`
- [ ] `OpenSearchIndexTest.getStatistic_whenStorageTimesOut_returnsUnknown`
- [ ] `CalciteIndexScanCostTest.test_cost_with_table_statistic_baseline` (rename)
- [ ] `CalciteIndexScanCostTest.test_estimateRowCount_with_table_statistic` (rename)
- [ ] `CalciteIndexScanCostTest.test_cost_fallback_when_no_statistic` (unchanged semantics; uses `Statistics.UNKNOWN`)
- [ ] `TableStatisticIntegrationTest` (rename + update)

Tasks:

- [ ] Step 1: Rewrite `getStatistic()` + update callers (`AbstractCalciteIndexScan`, storage engine wiring stub)
- [ ] Step 2: Update tests (one file at a time; re-run each to catch issues early)
- [ ] Step 3: Run `./gradlew opensearch:test --no-build-cache` — expect all pass
- [ ] Step 4: `spotlessApply`
- [ ] Step 5: Commit:
  ```
  refactor: wire OpenSearchIndex.getStatistic() to TableStatisticStorage + async refresh
  ```

---

## Task 6: REST API — `/_plugins/_sql/_statistics/{index}`

**Files:**
- Create: `plugin/src/main/java/org/opensearch/sql/plugin/rest/RestTableStatisticsAction.java`
- Modify: `plugin/src/main/java/org/opensearch/sql/plugin/SQLPlugin.java`
- Create: `plugin/src/test/java/org/opensearch/sql/plugin/rest/RestTableStatisticsActionTest.java`

REST handler:

- `GET /_plugins/_sql/_statistics/{index}`: calls `storage.get(indexName)`; returns 200 with `TableStatistic.toStoredDocSource()` JSON; returns 404 if not found.
- `POST /_plugins/_sql/_statistics/{index}/analyze`: calls `collector.refreshAsync(indexName, fieldsToType)` (fetches mapping internally if not supplied via body). Returns 202 with `{"status":"triggered"}` (or `{"status":"already_generating"}` if race-protection short-circuited).

Wiring (`SQLPlugin.java`):

- Instantiate `TableStatisticStorage` and `TableStatisticCollector` as singletons during plugin init (in `createComponents`).
- Return `RestTableStatisticsAction` from `getRestHandlers`.
- Pass `storage` + `collector` into `OpenSearchStorageEngine` so that every `OpenSearchIndex` it creates gets the same singletons.

Tests (rest handler only — storage/collector already covered):

- [ ] `GET_returnsStoredStat_whenPresent`
- [ ] `GET_returns404_whenAbsent`
- [ ] `POST_analyze_triggersRefresh_andReturns202`

Tasks:

- [ ] Step 1: Create REST handler
- [ ] Step 2: Wire singletons in `SQLPlugin.createComponents` + `getRestHandlers`
- [ ] Step 3: Write handler unit tests
- [ ] Step 4: Run `./gradlew :plugin:test :opensearch:test` — all pass
- [ ] Step 5: `spotlessApply`
- [ ] Step 6: Commit:
  ```
  feat: add /_plugins/_sql/_statistics/{index} REST API (GET + POST analyze)
  ```

---

## Task 7: Delete obsolete ml-commons-era code

**Files:**
- Delete: `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/IndexInsightStatistic.java`
- Delete: `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/IndexInsightStatisticProvider.java`
- Delete: `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/IndexInsightStatisticTest.java`
- Delete: `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/IndexInsightStatisticProviderTest.java`

**Verify via grep that no production code references any `IndexInsight*` symbol or `MLIndexInsightType`, `MLIndexInsightGetAction`, etc.** If any lingers, fix it.

Tasks:

- [ ] Step 1: `rg 'IndexInsight|MLIndexInsight' --type java` — list of offenders
- [ ] Step 2: Delete obsolete files
- [ ] Step 3: Fix any remaining references
- [ ] Step 4: Full opensearch + plugin build:
  ```
  ./gradlew :opensearch:test :plugin:test --no-build-cache
  ```
  Expect all pass.
- [ ] Step 5: Commit:
  ```
  chore: remove IndexInsight* classes and ml-commons-era glue
  ```

---

## Task 8: End-to-end cluster verification

**Goal:** Prove the entire pipeline works on a real cluster. This mirrors today's session validation but uses the new implementation.

**Prerequisites:**
- Cluster: `~/Downloads/opensearch-3.6.0` (local install from today)
- Ensure ml-commons is **reset to the bundled 3.6.0.0** (remove the locally-built 3.6.0.0-SNAPSHOT first if present).
- New SQL plugin zip from this branch installed.
- `plugins.calcite.enabled: true` + `plugins.calcite.table_statistics.enabled: true` in the cluster.

Steps:

- [ ] Step 1: Rebuild SQL plugin: `./gradlew :opensearch-sql-plugin:assemble -x test`
- [ ] Step 2: Reinstall on cluster
- [ ] Step 3: Start cluster
- [ ] Step 4: Seed a test index with known doc count (e.g. 137 docs):
  ```bash
  for i in $(seq 1 137); do
    curl -sX POST "localhost:9200/poc-stats/_doc" -H 'Content-Type: application/json' \
      -d "{\"status\":\"$((200+RANDOM%5*100))\"}" > /dev/null
  done
  curl -X POST "localhost:9200/poc-stats/_refresh"
  ```
- [ ] Step 5: Enable DEBUG logging for our package:
  ```bash
  curl -X PUT "localhost:9200/_cluster/settings" \
    -H 'Content-Type: application/json' \
    -d '{"transient":{"logger.org.opensearch.sql.opensearch.storage.statistics":"DEBUG"}}'
  ```
- [ ] Step 6: First PPL query — should return `rowcount=10000` (maxResultWindow fallback, collect not finished yet):
  ```bash
  curl -X POST "localhost:9200/_plugins/_ppl/_explain?mode=cost" \
    -H 'Content-Type: application/json' \
    -d '{"query":"source=poc-stats | stats count() by status"}'
  ```
- [ ] Step 7: Wait ~2 seconds for async collect; check storage:
  ```bash
  curl "localhost:9200/_plugins/_sql/_statistics/poc-stats"
  ```
  Expect a JSON doc with `"doc_count": 137` and `"status": "COMPLETED"`.
- [ ] Step 8: Second PPL query — should now return `rowcount=137`:
  ```bash
  curl -X POST "localhost:9200/_plugins/_ppl/_explain?mode=cost" \
    -H 'Content-Type: application/json' \
    -d '{"query":"source=poc-stats | stats count() by status"}'
  ```
  Success criterion: `CalciteLogicalIndexScan(...) ... rowcount = 137.0`.
- [ ] Step 9: Test manual-analyze endpoint:
  ```bash
  curl -X POST "localhost:9200/_plugins/_sql/_statistics/poc-stats/analyze"
  ```
  Expect `202` + `{"status":"triggered"}`.
- [ ] Step 10: Document results (terminal output + any surprises). No commit needed for this task — it's verification.

---

## Summary

| Task | What it does | Key artifact |
|------|-------------|--------------|
| 1 | `TableStatistic` Calcite `Statistic` impl | Typed in-memory shape |
| 2 | `TableStatisticStorage` for `.opensearch-statistics` CRUD | Persistence layer |
| 3 | `TableStatisticCollector` async collect + persist | Aggregation orchestrator |
| 4 | New setting `plugins.calcite.table_statistics.enabled` | Feature flag |
| 5 | Rewire `OpenSearchIndex.getStatistic()` for async | Consumer integration |
| 6 | REST API for manual trigger + read | Future cron hook |
| 7 | Delete `IndexInsight*` | Cleanup |
| 8 | End-to-end cluster verification | Proof |

---

## POC Validation Findings (2026-04-21)

End-to-end cluster verification confirmed the collect → persist → consume pipeline:

- **Seed:** 137 docs into `poc-v4` with `status` (5 distinct values) + `latency` (≈132 distinct)
- **First PPL explain cost:** `rowcount = 10000.0` (maxResultWindow fallback, async collect just triggered)
- **After collect (~200 ms):** `.opensearch-statistics` doc contains
  `{status=COMPLETED, doc_count=137, fields={latency, status}}`
- **`GET /_plugins/_sql/_statistics/poc-v4`:** returns the full JSON including per-field stats
- **Second PPL explain cost:** `CalciteLogicalIndexScan rowcount = 137.0` ✅

**Two production bugs caught only at cluster verification** (added in commit `199c16c91`):

1. `OpenSearchDataSourceFactory` (the default-datasource wiring path used by all PPL queries)
   still used the 2-arg `OpenSearchStorageEngine` ctor → statistic services arrived as
   `null` → `getStatistic()` always returned `UNKNOWN`. Fix: `SQLPlugin` pre-creates
   singletons before `createDataSourceService()` and passes them in via a new 4-arg factory
   ctor, then injects the same instances into Guice.
2. `TableStatisticCollector.buildAggregationRequest` produced an empty-sub-agg sampler when
   called with `Map.of()` fieldTypes (happens via REST `POST /analyze`). OpenSearch rejects
   empty samplers with `all shards failed`. Fix: skip the sampler entirely when no
   sub-aggregations are eligible; `trackTotalHits(true)` still produces the doc_count.

**Observed cost-model gap (next logical follow-up):**
Aggregate-node `rowcount` for `source=poc-v4 | stats count() by status` came back as `13.7`
(i.e. `input_rows / 10` — Calcite's default guess when `getDistinctRowCount` returns null).
Ideal is `5.0` because the stored stat shows `status.unique_count = 5`. The per-field
cardinality data is already collected and persisted; nothing in the current POC code path
feeds it into Calcite's `RelMetadataQuery`. Addressed by Consumer-side Future Work #2 below.

---

## Consumer-side Future Work (post-POC; not in the 8-task scope above)

These items extend the collected statistics into more places in the Calcite cost model.
The collector and storage already supply the data; these are purely consumer-side plumbing.

1. **Per-field equality selectivity in FILTER pushdown cost**
   Replace `RelMdUtil.guessSelectivity(condition)` inside `AbstractCalciteIndexScan`'s
   FILTER case with `FieldStatistic.equalitySelectivity()` when the condition is a simple
   equality on a statisticked column. Requires extracting field names from `RexNode`
   conditions (a Calcite `RexVisitor`).

2. **Aggregation cardinality estimation (GROUP BY rowcount)**
   Register a custom `RelMetadataProvider` supplying `BuiltInMetadata.DistinctRowCount` for
   `CalciteLogicalIndexScan`. When asked "how many distinct (col₁, col₂) tuples?", look up
   each column's `FieldStatistic.cardinality()` from the scan's `TableStatistic` and combine
   (for single-column groups: return the stored cardinality; for multi-column: use
   `RelMdUtil.numDistinctVals` on the product with clamping). This fixes the 13.7 → 5
   regression observed in POC verification.

3. **Range selectivity using min/max**
   Extend #1 to handle `<`, `<=`, `>`, `>=`, `BETWEEN` via `FieldStatistic.rangeSelectivity`.
   Combined with #1 gives accurate selectivity for most WHERE predicates.

4. **Null-ratio awareness**
   `FieldStatistic` already holds `nullRatio`, but no aggregation currently sets it (would
   need a `FiltersAggregation` with an `exists` filter per field — see ml-commons's
   `not_null` aggregation pattern). Once collected, factor into `IS NULL` / `IS NOT NULL`
   selectivity.

5. **Async prefetch on schema resolution**
   Today the first `getStatistic()` call triggers `refreshAsync` and returns UNKNOWN — the
   first query after a cold start still pays a cost estimate penalty. Preloading during
   query analysis (before cost) would let even the first query benefit from fresh stats.

---

## Ground rules for the executor (alignment guardrails)

To avoid drift from prior session state:

1. **Do NOT import `org.opensearch.ml.*`**. Any reference to `MLIndexInsightType`, `MLIndexInsightGetAction`, `MLIndexInsightGetResponse`, `IndexInsight`, etc. is a bug.
2. **Do NOT query `.plugins-ml-index-insight-storage`**. Our storage is `.opensearch-statistics`.
3. **Do NOT set `extended.plugins` on `opensearch-ml`** in plugin/build.gradle. Leave it as `opensearch-job-scheduler` only.
4. **Class names**: `TableStatistic*` across the board. If you catch yourself typing `IndexInsight`, stop.
5. **`OpenSearchIndex.getStatistic()` must be non-blocking** except for the ≤500 ms storage read. No 5-second latches.
6. **Don't add `LinkageError` catches** — they were for ml-commons optionality, no longer relevant.
7. **If uncertain whether a thing is in scope, check the Future Work section** before implementing it.
