# Table Statistics — Cron Refresh (M2 Phase 2)

Spec for the scheduled-refresh subsystem that keeps stored table statistics within their TTL without any user intervention. Complements M2 Phase 1 (min/max extraction, range selectivity). Rationale for every design choice is in `docs/dev/table-statistics-design.md` §3.5 — this document is the implementation-facing contract.

## 1. Goal

Extend the POC so that table statistics re-collect themselves on a cluster-wide schedule. Today the only refresh triggers are (a) the per-query miss path in `OpenSearchIndex.getStatistic()` and (b) an explicit `POST /_plugins/_sql/_statistics/{index}/analyze`. That is enough for dev / demo, not for productionization — an index that drifts stale and is never re-queried will keep stale stats forever.

Success criterion: on a live cluster, after setting `plugins.calcite.table_statistics.ttl=5s` and `plugins.calcite.table_statistics.refresh_interval=2s`, a stored record's `last_updated_time` advances without any user-issued request.

## 2. Scope

**In scope:**
- Scheduled refresh driven by the elected cluster-manager.
- Three new dynamic settings: `refresh_interval`, `ttl`, `refresh_max_in_flight`.
- Concurrency throttle via `Semaphore`.
- Distinguish search-pool rejection from hard failures (do not write `FAILED` marker on rejection).
- Unit + integration test coverage.

**Out of scope** (explicit TODOs for later milestones):
- Orphan stat-doc cleanup (index deleted but `.opensearch-statistics` record remains).
- Pagination / scroll of `listStale` when stat-doc count > 1000.
- Stat-doc mapping version migration (Phase 3).
- `null_ratio` / `top_terms` collection (Phase 3).
- Sampler `shard_size` tuning and promotion to a setting (Phase 3).

## 3. Architecture

```
 cluster-manager node                                 all nodes
 ┌──────────────────────────────────┐         ┌───────────────────────┐
 │  TableStatisticRefreshScheduler  │         │  SQLPlugin            │
 │   └─ LocalNodeClusterManager…    │         │   createComponents()  │
 │       ├─ onClusterManager()─┐    │         │         │             │
 │       └─ offClusterManager()│    │          └─────────┼─────────────┘
 │                             ▼    │                    │
 │   ThreadPool.scheduleWithFixedDelay (refresh_interval)
 │                             │                         ▼
 │                             ▼         (all nodes register listener
 │        TableStatisticRefreshTask       but only CM's onClusterManager
 │          1) flag off? → noop           fires; tick runs there)
 │          2) storage.listStale(ttl)
 │          3) for each stale index:
 │               tryAcquire semaphore
 │                 ├─ fail → skip
 │                 └─ ok   → resolveMapping (GENERIC)
 │                             │
 │                             ▼
 │                    collector.refreshAsync(name, fieldTypes,
 │                         completion → semaphore.release())
 └──────────────────────────────────┘
```

### 3.1 Components (new)

All live under `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/`.

| Class | Responsibility |
|---|---|
| `TableStatisticRefreshScheduler` | Binds a `LocalNodeClusterManagerListener` against `ClusterService`. On `onClusterManager()` starts the tick via `ThreadPool.scheduleWithFixedDelay`; on `offClusterManager()` cancels it. Holds the `Cancellable` handle. Also subscribes to dynamic-setting updates. |
| `TableStatisticRefreshTask` | The tick body — a `Runnable`. Reads settings, calls `storage.listStale`, iterates results under the semaphore, kicks off `refreshAsync`. Stateless beyond the injected collaborators. |
| Mapping resolver helper | Extracts the "resolve index name → `Map<String, OpenSearchDataType>`" logic currently inlined in `RestTableStatisticsAction` into a reusable collaborator (shared by scheduler + REST handler). |

### 3.2 Components (modified)

- `TableStatisticStorage`: add `void listStale(Duration ttl, int maxResults, ActionListener<List<String>> listener)`. Issues a `SearchRequest` on `.opensearch-statistics` with `size=maxResults`, filter `status IN (COMPLETED, FAILED) AND last_updated_time < now() - ttl`, and extracts the doc `_source.index_name` (see §3.3 for storage-doc change). Returns empty list on `IndexNotFoundException` (cold start). Never calls `onFailure`.
- `TableStatisticStorage.INDEX_MAPPING`: add `index_name` as a `keyword` field so `listStale` can return index names directly. `put()` / `putStatus()` both write `index_name` into the source.
- `TableStatistic` / stored doc: include `"index_name": "<raw-name>"` in `toStoredDocSource()`. Parseback in `fromStoredDoc` is not needed (consumers today only read from the known name side), but adding a field does no harm.
- `TableStatisticCollector.refreshAsync`: add an overload `refreshAsync(String name, Map<String, OpenSearchDataType> fieldTypes, ActionListener<Void> completion)`. Completion fires on every terminal path (success, FAILED marker, `EsRejectedExecutionException` skip). The existing 2-arg form delegates to the new one with a noop listener.
- `TableStatisticCollector.proceedWithCollect` (`onFailure` branch): if `ExceptionsHelper.unwrap(e, EsRejectedExecutionException.class) != null`, invoke `completion.onResponse(null)` and log at `debug` — do **not** write a `FAILED` marker.

### 3.3 Storage doc layout change

Stored doc grows one keyword field:

```json
{
  "status": "COMPLETED",
  "last_updated_time": "2026-04-23T...",
  "index_name": "logs-2026-04-23",   // new
  "doc_count": 137,
  "fields": { ... }
}
```

Deterministic `docId = sha256(indexName)` stays — `index_name` is additive metadata for the sweep query. Existing docs missing this field are still readable (consumers never touch it); they simply won't be returned by `listStale` until the next refresh rewrites them with the field. That's acceptable — their presence in the index without `index_name` is functionally equivalent to "never been refreshed by the new code," and the query-path miss will eventually re-write them.

**Rollout implication:** after deploying M2 Phase 2, old docs remain effective for reads but opaque to the sweeper. No migration step is taken — the docs will self-heal on next refresh (either via query-path miss, explicit `/analyze`, or a manual TTL bump). This is recorded in Phase 3 (index lifecycle / mapping migration).

### 3.4 Wiring (`SQLPlugin.createComponents`)

```java
TableStatisticRefreshScheduler scheduler = new TableStatisticRefreshScheduler(
    clusterService, threadPool, settings,
    tableStatisticStorage, tableStatisticCollector, mappingResolver);
scheduler.register();  // clusterService.addLocalNodeClusterManagerListener(this)
components.add(scheduler);  // keep alive for plugin lifecycle
```

No Guice-side plumbing needed; the scheduler is self-contained and keeps the singleton reference via `createComponents`'s return list.

## 4. Data flow

### 4.1 Startup

1. Every node calls `createComponents` → registers `LocalNodeClusterManagerListener`.
2. On the cluster-manager node, OpenSearch fires `onClusterManager()` immediately.
3. Scheduler reads `TABLE_STATISTICS_ENABLED`:
   - `false` → don't schedule tick. Subscribe to the flag's update-consumer to start later if flipped.
   - `true` → `threadPool.scheduleWithFixedDelay(task, refresh_interval, ThreadPool.Names.GENERIC)`.

### 4.2 Tick

```
TableStatisticRefreshTask.run()
 ├─ if (!flag.enabled) return
 ├─ ttl = settings.get(TTL)        // re-read every tick; no caching
 ├─ semaphore = scheduler.currentSemaphore()   // see §4.4; volatile ref
 ├─ storage.listStale(ttl, LIST_STALE_LIMIT /*=1000*/, ActionListener {
 │    onResponse(staleNames):
 │      for name in staleNames:
 │        if (!semaphore.tryAcquire()) continue   // try next; no retry
 │        threadPool.executor(GENERIC).execute(() -> {
 │          try {
 │            fieldTypes = mappingResolver.resolve(name)  // blocking; GENERIC is OK
 │            collector.refreshAsync(name, fieldTypes,
 │                ActionListener.wrap(
 │                    v -> semaphore.release(),
 │                    e -> semaphore.release()));
 │          } catch (Exception ex) {
 │            semaphore.release();
 │            LOG.debug(...);
 │          }
 │        });
 │    onFailure: /* never called */
 │  });
```

`LIST_STALE_LIMIT=1000` is a static constant. If a sweep ever returns exactly 1000 results, the next tick picks up the remainder.

### 4.3 Shutdown / cluster-manager loss

```
offClusterManager():
  cancellable.cancel();      // stops future ticks
  // in-flight refreshes run to completion on their own; they
  // will release the semaphore in their own callbacks
  // (those callbacks are decoupled from tick lifecycle)
```

Do **not** wait for in-flight to drain. They will write their latest `last_updated_time` to `.opensearch-statistics`; whichever node becomes the next cluster-manager will not re-refresh those indices on its first tick.

### 4.4 Dynamic setting changes

Scheduler registers an update-consumer for each setting via `clusterService.getClusterSettings().addSettingsUpdateConsumer`:

- `refresh_interval` → `cancellable.cancel(); cancellable = threadPool.scheduleWithFixedDelay(task, newValue, GENERIC)`.
- `ttl` → no action. Each tick re-reads from `settings`.
- `refresh_max_in_flight` → replace `Semaphore` reference. In-flight refreshes hold permits on the old semaphore — they release cleanly against it (permits are simply lost, which is fine — old semaphore is GC'd). New refreshes use the new semaphore.
- `plugins.calcite.table_statistics.enabled` → false: cancel tick. → true: schedule tick if we're still cluster-manager.

All update-consumer work happens on the cluster-state applier thread; keep it strictly to "swap refs, schedule/cancel one task" — no I/O.

## 5. Error handling

| Source | Handling |
|---|---|
| `listStale` fails (index not found, etc.) | `onResponse([])` — tick returns cleanly. Cold-start normal. |
| `mappingResolver.resolve` throws (index deleted mid-tick) | `semaphore.release()` + `log.debug`; next index continues. Orphan `.opensearch-statistics` doc stays (cleanup is out of scope). |
| `refreshAsync` GENERATING-dedup fires | Collector's existing logic skips; completion listener called with `onResponse(null)`; permit released. |
| `refreshAsync` persist succeeds | `onResponse(null)` → release. |
| `refreshAsync` persist fails non-rejection | Existing logic writes FAILED marker → `onResponse(null)` (terminal) → release. |
| `refreshAsync` persist fails with rejection | `onResponse(null)` without writing FAILED marker → release. Stat record unchanged; will be picked up again next tick. |
| Listener callback throws | Every `refreshAsync` completion wrap has a try/finally guaranteeing `semaphore.release()`. Losing a permit is harder to diagnose than any other symptom — paranoia here pays. |

## 6. Settings

All three are dynamic + `NodeScope`. Added to:

1. `common/src/main/java/org/opensearch/sql/common/setting/Settings.java` — `Key` enum entries.
2. `opensearch/src/main/java/org/opensearch/sql/opensearch/setting/OpenSearchSettings.java` — `Setting<?>` definitions + `register(...)` calls + `pluginSettings()` list.

| Key | Type | Default | Min | Max |
|---|---|---|---|---|
| `plugins.calcite.table_statistics.refresh_interval` | `TimeValue` | `60s` | `5s` | — |
| `plugins.calcite.table_statistics.ttl` | `TimeValue` | `24h` | `1m` | — |
| `plugins.calcite.table_statistics.refresh_max_in_flight` | `Integer` | `4` | `1` | `100` |

Min / max bounds are defensive — unrelated users setting `refresh_interval=0` will not OOM the node. Max on `refresh_max_in_flight` is advisory; an operator overriding it past 100 has likely accepted the consequence.

## 7. Testing

### 7.1 Unit tests

New `TableStatisticRefreshSchedulerTest` (mocks `ClusterService`, `ThreadPool`, `Settings`):
- `onClusterManager` with flag on schedules a `Cancellable`.
- `onClusterManager` with flag off does not schedule.
- `offClusterManager` cancels a previously-scheduled `Cancellable`.
- Flag flipped true→false during execution cancels.
- Flag flipped false→true while cluster-manager schedules.
- `refresh_interval` update cancels + reschedules.
- `refresh_max_in_flight` update swaps the semaphore.
- Listener cleanup on plugin close.

New `TableStatisticRefreshTaskTest` (mocks collector, storage, mapping resolver):
- 0 stale indices → no refresh calls.
- N stale indices, semaphore = 2 → exactly 2 refresh calls; semaphore release on completion allows further ticks.
- Listener callback throws → semaphore released anyway.
- `listStale` returns empty → no side effects.
- Mapping-resolve throws → release + next index still attempted.
- `refreshAsync` completion fires after failure → release still happens.

Extended `TableStatisticCollectorTest`:
- `EsRejectedExecutionException` → completion listener called, no FAILED marker written.
- 3-arg `refreshAsync` invokes completion on success, hard failure, rejection, and GENERATING-skip paths.

Extended `TableStatisticStorageTest`:
- `listStale` on missing index → `[]`.
- `listStale` filters by `last_updated_time` and `status`.
- `listStale` respects `maxResults`.
- `put` writes `index_name` field.

### 7.2 Integration tests

Extended `TableStatisticsIT`:
- **Stale refresh.** Write a stat doc with `last_updated_time` = 1h ago. Set `refresh_interval=2s, ttl=5s`. Wait 6s. Assert the doc's `last_updated_time` has advanced.
- **Disabled flag.** With flag false, no refresh happens even after TTL passes.
- **Dynamic interval.** Start with `refresh_interval=10m`; update to `2s`; observe refresh within a few seconds.
- **Rejection does not persist FAILED.** Simulating search-pool rejection deterministically in an `IntegTestCase` is fragile; skip this in IT, cover it at unit level only.

The existing single-node IT harness is sufficient — OpenSearch's test cluster always elects a cluster-manager on node 0.

## 8. Risks & mitigations

| Risk | Mitigation |
|---|---|
| Cron's `listStale` search occupies a search slot every tick | size=1000 on one system index ≈ trivial; same-pool contention is capped at 1 request per `refresh_interval`. |
| Mapping resolution blocks `GENERIC` pool | Same pattern already used by REST `/analyze`; no new exposure. |
| cluster-manager overload at 1k+ indices | Current `listStale` returns up to 1k/tick; 1k refreshes at 4 in-flight over 60s tick completes in ~250 ticks ≈ 4 h — well within 24 h TTL. If this becomes a bottleneck, introduce pagination or bump `refresh_max_in_flight` — no architectural change. |
| Permit leak on unexpected exception in callback | Every release is wrapped in `ActionListener.wrap` with identical onResponse/onFailure bodies; additionally, the mapping-resolve block has explicit try/catch. Unit tests assert permit returns on exception paths. |
| Old-format stat docs invisible to sweeper | Accepted — see §3.3. They remain valid for reads; query-path miss or explicit `/analyze` re-writes them with the new field. |

## 9. Open questions

None. Every decision is recorded in `docs/dev/table-statistics-design.md` §3.5 or in §3 / §5 of this spec.

## 10. Done definition

- [ ] New scheduler class + listener wired via `SQLPlugin.createComponents`.
- [ ] Three dynamic settings registered, bounded, and documented.
- [ ] `refreshAsync` 3-arg overload; rejection path distinguished from hard failure.
- [ ] `listStale` + `index_name` stored field.
- [ ] Unit tests as per §7.1 pass.
- [ ] Integration test "stale refresh" scenario passes.
- [ ] Living design doc §3.5 + worklog entry already merged as part of this spec's prep.
- [ ] Live cluster verify: stale doc's `last_updated_time` advances without user action.
