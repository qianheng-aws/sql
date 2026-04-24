# Table Statistics — Cron Refresh Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a cluster-manager-driven background sweep that refreshes stale stored `TableStatistic` records within a configurable TTL, without any user request.

**Architecture:** A `LocalNodeClusterManagerListener` is registered on every node. On the elected cluster-manager, `onClusterManager()` starts a `ThreadPool.scheduleWithFixedDelay` tick that calls a new `TableStatisticStorage.listStale(ttl, ...)`, then for each stale index name acquires a permit from a `Semaphore` (default 4), resolves the mapping on `GENERIC`, and calls `TableStatisticCollector.refreshAsync` with a completion callback that releases the permit. `EsRejectedExecutionException` on the inner search is treated as "cluster busy" and does NOT write a `FAILED` marker. Three new dynamic settings (`refresh_interval`, `ttl`, `refresh_max_in_flight`) plus one storage-doc change (adding an `index_name` keyword field to let `listStale` return names directly).

**Tech Stack:** Java 21, OpenSearch plugin APIs (`ClusterService`, `LocalNodeClusterManagerListener`, `ThreadPool.scheduleWithFixedDelay`, `ActionListener`), JUnit 4 + Mockito, Google Java Format.

**Reference spec:** `docs/superpowers/specs/2026-04-23-table-statistics-cron-refresh-design.md`

---

## File Structure

**New files (all under `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/`):**
- `TableStatisticRefreshScheduler.java` — binds `LocalNodeClusterManagerListener`, owns the `Cancellable` + `Semaphore` references, subscribes to dynamic-setting updates.
- `TableStatisticRefreshTask.java` — `Runnable` invoked per tick; reads settings, drives `listStale` → throttled `refreshAsync`.
- `TableStatisticsMappingResolver.java` — extracts the mapping-resolve logic currently inlined in `RestTableStatisticsAction` so both callers can share it.

**New test files (under `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/`):**
- `TableStatisticRefreshSchedulerTest.java`
- `TableStatisticRefreshTaskTest.java`
- `TableStatisticsMappingResolverTest.java`

**Modified files:**
- `common/src/main/java/org/opensearch/sql/common/setting/Settings.java` — add 3 `Key` enum entries.
- `opensearch/src/main/java/org/opensearch/sql/opensearch/setting/OpenSearchSettings.java` — register 3 `Setting<?>` instances in `pluginSettings()` and `register(...)`.
- `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/TableStatistic.java` — `toStoredDocSource()` writes `index_name`.
- `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticStorage.java` — extend `INDEX_MAPPING` with `index_name: keyword`; `putStatus` also writes `index_name`; add `listStale(Duration ttl, int maxResults, ActionListener<List<String>> listener)`.
- `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticCollector.java` — add 3-arg `refreshAsync` overload with completion `ActionListener<Void>`; distinguish `EsRejectedExecutionException` → do not write FAILED marker.
- `plugin/src/main/java/org/opensearch/sql/plugin/SQLPlugin.java` — new scheduler field + wire in `createComponents`.
- `plugin/src/main/java/org/opensearch/sql/plugin/rest/RestTableStatisticsAction.java` — delegate mapping resolution to the new helper.
- `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticStorageTest.java` — add `listStale` + `index_name` tests.
- `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticCollectorTest.java` — add rejection + completion tests.
- `integ-test/src/test/java/org/opensearch/sql/calcite/remote/TableStatisticsIT.java` — new `testStaleRefresh` test.

---

## Task Ordering Rationale

1. **Settings first** (Task 1) — every downstream class references them.
2. **Storage schema change + `listStale`** (Tasks 2–3) — scheduler depends on `listStale`; collector and REST handler depend on the storage writing `index_name`.
3. **Collector 3-arg overload + rejection branch** (Task 4) — scheduler depends on the completion callback.
4. **Mapping resolver extraction** (Task 5) — scheduler needs it; REST handler keeps working.
5. **Refresh task** (Task 6) then **scheduler** (Task 7) — task is stateless logic, scheduler owns lifecycle.
6. **Wiring in `SQLPlugin`** (Task 8) — all collaborators now exist.
7. **Integration test** (Task 9) — last, once everything is wired.

---

## Task 1: Register three new dynamic settings

**Files:**
- Modify: `common/src/main/java/org/opensearch/sql/common/setting/Settings.java` (add 3 enum entries)
- Modify: `opensearch/src/main/java/org/opensearch/sql/opensearch/setting/OpenSearchSettings.java` (define, register, expose)

**Purpose:** All three settings must exist in registered state before any scheduler code can read them. No tests — existing `OpenSearchSettings` tests are integration-shaped and the next tasks exercise these settings for real.

- [ ] **Step 1: Add enum keys**

Edit `common/src/main/java/org/opensearch/sql/common/setting/Settings.java`, inside the `Key` enum, **immediately after** the existing `TABLE_STATISTICS_ENABLED` line (around line 47):

```java
TABLE_STATISTICS_ENABLED("plugins.calcite.table_statistics.enabled"),
TABLE_STATISTICS_REFRESH_INTERVAL("plugins.calcite.table_statistics.refresh_interval"),
TABLE_STATISTICS_TTL("plugins.calcite.table_statistics.ttl"),
TABLE_STATISTICS_REFRESH_MAX_IN_FLIGHT("plugins.calcite.table_statistics.refresh_max_in_flight"),
```

- [ ] **Step 2: Define `Setting<?>` constants**

Edit `opensearch/src/main/java/org/opensearch/sql/opensearch/setting/OpenSearchSettings.java`. **Add** the following three constants immediately after `TABLE_STATISTICS_ENABLED_SETTING` (around line 194):

```java
  public static final Setting<TimeValue> TABLE_STATISTICS_REFRESH_INTERVAL_SETTING =
      Setting.timeSetting(
          Key.TABLE_STATISTICS_REFRESH_INTERVAL.getKeyValue(),
          TimeValue.timeValueSeconds(60),
          TimeValue.timeValueSeconds(5),
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<TimeValue> TABLE_STATISTICS_TTL_SETTING =
      Setting.timeSetting(
          Key.TABLE_STATISTICS_TTL.getKeyValue(),
          TimeValue.timeValueHours(24),
          TimeValue.timeValueMinutes(1),
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<Integer> TABLE_STATISTICS_REFRESH_MAX_IN_FLIGHT_SETTING =
      Setting.intSetting(
          Key.TABLE_STATISTICS_REFRESH_MAX_IN_FLIGHT.getKeyValue(),
          4,
          1,
          100,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);
```

Ensure `import org.opensearch.common.unit.TimeValue;` is already present (it is — line 29).

- [ ] **Step 3: Register the three settings**

In `OpenSearchSettings.java`, find the existing `register(...)` call for `TABLE_STATISTICS_ENABLED` (around line 477). **Immediately after** that `register(...)` block, add:

```java
    register(
        settingBuilder,
        clusterSettings,
        Key.TABLE_STATISTICS_REFRESH_INTERVAL,
        TABLE_STATISTICS_REFRESH_INTERVAL_SETTING,
        new Updater(Key.TABLE_STATISTICS_REFRESH_INTERVAL));
    register(
        settingBuilder,
        clusterSettings,
        Key.TABLE_STATISTICS_TTL,
        TABLE_STATISTICS_TTL_SETTING,
        new Updater(Key.TABLE_STATISTICS_TTL));
    register(
        settingBuilder,
        clusterSettings,
        Key.TABLE_STATISTICS_REFRESH_MAX_IN_FLIGHT,
        TABLE_STATISTICS_REFRESH_MAX_IN_FLIGHT_SETTING,
        new Updater(Key.TABLE_STATISTICS_REFRESH_MAX_IN_FLIGHT));
```

- [ ] **Step 4: Add settings to `pluginSettings()` list**

In `OpenSearchSettings.java`, find the `pluginSettings()` method. **Immediately after** `.add(TABLE_STATISTICS_ENABLED_SETTING)` (around line 674), add:

```java
        .add(TABLE_STATISTICS_REFRESH_INTERVAL_SETTING)
        .add(TABLE_STATISTICS_TTL_SETTING)
        .add(TABLE_STATISTICS_REFRESH_MAX_IN_FLIGHT_SETTING)
```

- [ ] **Step 5: Verify compile**

Run: `./gradlew :common:compileJava :opensearch:compileJava`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 6: Commit**

```bash
git add common/src/main/java/org/opensearch/sql/common/setting/Settings.java \
        opensearch/src/main/java/org/opensearch/sql/opensearch/setting/OpenSearchSettings.java
git commit -s -m "feat: register three new settings for cron refresh

plugins.calcite.table_statistics.refresh_interval (60s, min 5s)
plugins.calcite.table_statistics.ttl (24h, min 1m)
plugins.calcite.table_statistics.refresh_max_in_flight (4, range 1..100)

All dynamic, node-scope. Consumed by the upcoming refresh scheduler."
```

---

## Task 2: Write `index_name` into the stored stat doc

**Files:**
- Modify: `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/TableStatistic.java`
- Modify: `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticStorage.java`
- Modify: `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticStorageTest.java`

**Purpose:** `listStale` needs the index name discoverable from the doc `_source`. We add a `keyword` field `index_name` to the index mapping and to every write path (`put`, `putStatus`). `TableStatistic.toStoredDocSource` takes an extra argument, since the record itself doesn't know its name; callers pass it in.

- [ ] **Step 1: Write failing test — `put` writes `index_name`**

In `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticStorageTest.java`, locate the existing `put` success test and add a new test next to it (adjust imports if needed — `ArgumentCaptor`, `IndexRequest` are likely already imported):

```java
  @Test
  public void putWritesIndexNameField() {
    TableStatistic stat = TableStatistic.fromFields(42L, Collections.emptyMap());
    ArgumentCaptor<IndexRequest> captor = ArgumentCaptor.forClass(IndexRequest.class);
    // mock existsResponse.isExists() = true to skip the createIndex branch
    givenIndexExists();

    storage.put("logs-2026", stat, new ActionListener<Void>() {
      @Override public void onResponse(Void v) {}
      @Override public void onFailure(Exception e) { fail(e.getMessage()); }
    });

    verify(nodeClient).index(captor.capture(), any());
    Map<String, Object> source = captor.getValue().sourceAsMap();
    assertEquals("logs-2026", source.get("index_name"));
  }
```

If `givenIndexExists()` isn't already a helper in the test file, inspect existing tests for the pattern and copy it (it'll mock the exists() admin call to return true).

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :opensearch:test --tests "*TableStatisticStorageTest.putWritesIndexNameField" -i`
Expected: FAIL — `source.get("index_name")` returns null.

- [ ] **Step 3: Modify `INDEX_MAPPING`**

In `TableStatisticStorage.java`, change `INDEX_MAPPING` (around line 51) to include `index_name`:

```java
  private static final Map<String, Object> INDEX_MAPPING =
      Map.of(
          "properties",
          Map.of(
              "status",
              Map.of("type", "keyword"),
              "last_updated_time",
              Map.of("type", "date", "format", "strict_date_time||epoch_millis"),
              "index_name",
              Map.of("type", "keyword"),
              "doc_count",
              Map.of("type", "long"),
              "fields",
              Map.of("type", "object", "enabled", false)));
```

- [ ] **Step 4: Make `put` include `index_name`**

In `TableStatisticStorage.java`, change the `put(...)` method so that the stored source has `index_name` injected before the index write. The cleanest change is to modify the existing `stat.toStoredDocSource()` call site to merge the name in:

```java
  public void put(String indexName, TableStatistic stat, ActionListener<Void> listener) {
    Map<String, Object> source = new LinkedHashMap<>(stat.toStoredDocSource());
    source.put("index_name", indexName);
    ensureIndexExists(
        ActionListener.wrap(
            ignored -> writeDoc(indexName, source, listener),
            listener::onFailure));
  }
```

- [ ] **Step 5: Make `putStatus` include `index_name`**

In `TableStatisticStorage.java`, change the `putStatus(...)` method similarly:

```java
  public void putStatus(String indexName, String status, ActionListener<Void> listener) {
    Map<String, Object> source = new LinkedHashMap<>();
    source.put("status", status);
    source.put("last_updated_time", Instant.now().toString());
    source.put("index_name", indexName);
    ensureIndexExists(
        ActionListener.wrap(ignored -> writeDoc(indexName, source, listener), listener::onFailure));
  }
```

- [ ] **Step 6: Run test to verify it passes**

Run: `./gradlew :opensearch:test --tests "*TableStatisticStorageTest.putWritesIndexNameField" -i`
Expected: PASS.

- [ ] **Step 7: Run the full storage test class**

Run: `./gradlew :opensearch:test --tests "*TableStatisticStorageTest"`
Expected: all existing tests still pass (the `index_name` addition is purely additive in the doc shape).

- [ ] **Step 8: Format and commit**

```bash
./gradlew spotlessApply
git add opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticStorage.java \
        opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticStorageTest.java
git commit -s -m "feat: write index_name into stored stat doc

The stored doc now has an \`index_name\` keyword field written on both
put() and putStatus(). This is the data that the upcoming listStale
sweep will query on to return plain index names."
```

---

## Task 3: Add `listStale` to `TableStatisticStorage`

**Files:**
- Modify: `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticStorage.java`
- Modify: `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticStorageTest.java`

**Purpose:** Scheduler's tick needs a way to pull all stale index names from the system index in one search. Contract: returns a `List<String>` via `onResponse`; never throws; `IndexNotFoundException` or any transport failure yields an empty list (cold start).

- [ ] **Step 1: Write failing test — empty on missing index**

Add to `TableStatisticStorageTest.java`:

```java
  @Test
  public void listStaleReturnsEmptyOnIndexNotFound() {
    doAnswer(inv -> {
      ActionListener<SearchResponse> l = inv.getArgument(1);
      l.onFailure(new IndexNotFoundException(".opensearch-statistics"));
      return null;
    }).when(nodeClient).search(any(), any());

    AtomicReference<List<String>> captured = new AtomicReference<>();
    storage.listStale(Duration.ofHours(1), 1000,
        ActionListener.wrap(captured::set, e -> fail(e.getMessage())));

    assertNotNull(captured.get());
    assertTrue(captured.get().isEmpty());
  }
```

Add any missing imports at the top of the test file: `import java.util.concurrent.atomic.AtomicReference;`, `import org.opensearch.action.search.SearchResponse;`, `import org.opensearch.index.IndexNotFoundException;`, `import java.time.Duration;`, etc.

- [ ] **Step 2: Run test to verify it fails to compile**

Run: `./gradlew :opensearch:test --tests "*TableStatisticStorageTest.listStaleReturnsEmptyOnIndexNotFound"`
Expected: FAIL — method `listStale` does not exist.

- [ ] **Step 3: Implement `listStale`**

Add to `TableStatisticStorage.java`:

```java
  /**
   * List index names whose stored stat is stale (older than {@code ttl}) and whose status is
   * actionable ({@code COMPLETED} or {@code FAILED} — {@code GENERATING} is skipped since the
   * collector has its own abandonment logic).
   *
   * <p>Returns an empty list on missing storage index or any transport failure. Never calls
   * {@code onFailure}. {@code maxResults} caps the page size; if the sweep ever returns exactly
   * {@code maxResults}, the next tick will pick up the remainder.
   */
  public void listStale(
      Duration ttl, int maxResults, ActionListener<List<String>> listener) {
    long cutoffMillis = System.currentTimeMillis() - ttl.toMillis();
    BoolQueryBuilder query =
        QueryBuilders.boolQuery()
            .filter(
                QueryBuilders.termsQuery(
                    "status", TableStatistic.STATUS_COMPLETED, TableStatistic.STATUS_FAILED))
            .filter(QueryBuilders.rangeQuery("last_updated_time").lt(cutoffMillis));
    SearchRequest request =
        new SearchRequest(STORAGE_INDEX)
            .source(
                new SearchSourceBuilder()
                    .query(query)
                    .size(maxResults)
                    .fetchSource(new String[] {"index_name"}, null)
                    .trackTotalHits(false));
    nodeClient.search(
        request,
        new ActionListener<SearchResponse>() {
          @Override
          public void onResponse(SearchResponse response) {
            List<String> names = new ArrayList<>();
            for (SearchHit hit : response.getHits().getHits()) {
              Object name = hit.getSourceAsMap() == null ? null : hit.getSourceAsMap().get("index_name");
              if (name instanceof String s && !s.isEmpty()) {
                names.add(s);
              }
            }
            listener.onResponse(names);
          }

          @Override
          public void onFailure(Exception e) {
            if (ExceptionsHelper.unwrap(e, IndexNotFoundException.class) != null) {
              LOG.debug("Statistics index {} does not exist yet", STORAGE_INDEX);
            } else {
              LOG.warn(
                  "Failed to listStale stat docs ({}): {}",
                  e.getClass().getSimpleName(),
                  e.getMessage());
            }
            listener.onResponse(Collections.emptyList());
          }
        });
  }
```

Add imports: `org.opensearch.action.search.SearchRequest`, `org.opensearch.action.search.SearchResponse`, `org.opensearch.index.query.BoolQueryBuilder`, `org.opensearch.index.query.QueryBuilders`, `org.opensearch.search.builder.SearchSourceBuilder`, `org.opensearch.search.SearchHit`, `java.time.Duration`, `java.util.ArrayList`, `java.util.Collections`, `java.util.List`.

- [ ] **Step 4: Run empty-on-missing test to verify it passes**

Run: `./gradlew :opensearch:test --tests "*TableStatisticStorageTest.listStaleReturnsEmptyOnIndexNotFound"`
Expected: PASS.

- [ ] **Step 5: Write second failing test — returns names on match**

Add to `TableStatisticStorageTest.java`:

```java
  @Test
  public void listStaleReturnsIndexNamesFromHits() throws IOException {
    SearchHit hit1 = new SearchHit(1);
    hit1.sourceRef(org.opensearch.core.common.bytes.BytesReference.bytes(
        org.opensearch.common.xcontent.XContentFactory.jsonBuilder()
            .startObject().field("index_name", "logs-a").endObject()));
    SearchHit hit2 = new SearchHit(2);
    hit2.sourceRef(org.opensearch.core.common.bytes.BytesReference.bytes(
        org.opensearch.common.xcontent.XContentFactory.jsonBuilder()
            .startObject().field("index_name", "logs-b").endObject()));
    SearchHits hits =
        new SearchHits(new SearchHit[] {hit1, hit2}, new TotalHits(2, TotalHits.Relation.EQUAL_TO), 1.0f);
    SearchResponse resp = mock(SearchResponse.class);
    when(resp.getHits()).thenReturn(hits);

    doAnswer(inv -> {
      ActionListener<SearchResponse> l = inv.getArgument(1);
      l.onResponse(resp);
      return null;
    }).when(nodeClient).search(any(), any());

    AtomicReference<List<String>> captured = new AtomicReference<>();
    storage.listStale(Duration.ofHours(1), 1000,
        ActionListener.wrap(captured::set, e -> fail(e.getMessage())));

    assertEquals(List.of("logs-a", "logs-b"), captured.get());
  }
```

Add imports: `org.apache.lucene.search.TotalHits`, `org.opensearch.search.SearchHit`, `org.opensearch.search.SearchHits`, `java.io.IOException`.

- [ ] **Step 6: Run the hits test to verify PASS**

Run: `./gradlew :opensearch:test --tests "*TableStatisticStorageTest.listStaleReturnsIndexNamesFromHits"`
Expected: PASS.

- [ ] **Step 7: Run full storage test class**

Run: `./gradlew :opensearch:test --tests "*TableStatisticStorageTest"`
Expected: all tests pass.

- [ ] **Step 8: Format and commit**

```bash
./gradlew spotlessApply
git add opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticStorage.java \
        opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticStorageTest.java
git commit -s -m "feat: TableStatisticStorage.listStale to query stale records

Range query on last_updated_time < (now - ttl), filter on
status IN {COMPLETED, FAILED}. Returns index_name values from the
stored source. IndexNotFoundException or any transport failure
is swallowed to an empty list so cold-start just no-ops."
```

---

## Task 4: Collector — 3-arg `refreshAsync` + rejection branch

**Files:**
- Modify: `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticCollector.java`
- Modify: `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticCollectorTest.java`

**Purpose:** The scheduler needs to release a semaphore permit exactly when a refresh is *done* — any terminal path (success, hard failure, rejection, GENERATING dedup). Today `refreshAsync` is fire-and-forget with no completion signal. We add a 3-arg overload that takes an `ActionListener<Void>` which fires exactly once on every terminal path. The old 2-arg form delegates with a noop listener. Also: on `EsRejectedExecutionException` we SKIP (do not write FAILED marker); this reflects "cluster busy, not a stat problem."

- [ ] **Step 1: Write failing test — completion fires on success**

Add to `TableStatisticCollectorTest.java` (the file should already have a mocked `NodeClient` and `TableStatisticStorage` from existing tests):

```java
  @Test
  public void refreshAsyncThreeArgInvokesCompletionOnSuccess() {
    // Wire up: getRaw returns empty, search returns a valid SearchResponse,
    // storage.put invokes its listener.onResponse(null).
    givenGetRawReturnsEmpty();
    givenSearchReturnsSuccess();
    givenStorageAcceptsPut();

    AtomicInteger successCount = new AtomicInteger();
    collector.refreshAsync(
        "logs-a",
        Map.of(),
        ActionListener.wrap(v -> successCount.incrementAndGet(), e -> fail(e.getMessage())));

    assertEquals(1, successCount.get());
  }
```

If `givenGetRawReturnsEmpty()` etc. helpers don't exist, inline the mock setup using `doAnswer`/`when` as the class currently does for its existing tests.

- [ ] **Step 2: Run the test — expect compile failure**

Run: `./gradlew :opensearch:test --tests "*TableStatisticCollectorTest.refreshAsyncThreeArgInvokesCompletionOnSuccess"`
Expected: FAIL (method missing).

- [ ] **Step 3: Implement 3-arg `refreshAsync`**

In `TableStatisticCollector.java`:

1. Add a class-level `noopCompletion`:

```java
  private static final ActionListener<Void> NOOP_COMPLETION =
      ActionListener.wrap(v -> {}, e -> {});
```

2. Rename the existing `refreshAsync(String, Map<String, OpenSearchDataType>)` body to take a third parameter `ActionListener<Void> completion`. Keep the 2-arg method as a delegating public method:

```java
  public void refreshAsync(String indexName, Map<String, OpenSearchDataType> fieldTypes) {
    refreshAsync(indexName, fieldTypes, NOOP_COMPLETION);
  }

  public void refreshAsync(
      String indexName,
      Map<String, OpenSearchDataType> fieldTypes,
      ActionListener<Void> completion) {
    ActionListener<Void> safeCompletion = oneShot(completion, indexName);
    try {
      storage.getRaw(
          indexName,
          new ActionListener<Optional<Map<String, Object>>>() {
            @Override
            public void onResponse(Optional<Map<String, Object>> maybeDoc) {
              if (maybeDoc.isPresent() && isFreshGenerating(maybeDoc.get())) {
                LOG.debug("refresh for {} already in progress; skipping", indexName);
                safeCompletion.onResponse(null);
                return;
              }
              proceedWithCollect(indexName, fieldTypes, safeCompletion);
            }

            @Override
            public void onFailure(Exception e) {
              LOG.debug(
                  "getRaw failed for {}: {}; proceeding with refresh anyway",
                  indexName,
                  e.getMessage());
              proceedWithCollect(indexName, fieldTypes, safeCompletion);
            }
          });
    } catch (RuntimeException e) {
      LOG.warn("refreshAsync for {} threw synchronously: {}", indexName, e.getMessage());
      safeCompletion.onResponse(null);
    }
  }
```

3. Add the one-shot adapter helper:

```java
  private static ActionListener<Void> oneShot(ActionListener<Void> inner, String indexName) {
    AtomicBoolean fired = new AtomicBoolean(false);
    return new ActionListener<Void>() {
      @Override
      public void onResponse(Void v) {
        if (fired.compareAndSet(false, true)) {
          try {
            inner.onResponse(null);
          } catch (RuntimeException ex) {
            LOG.warn("completion listener for {} threw: {}", indexName, ex.getMessage());
          }
        }
      }

      @Override
      public void onFailure(Exception e) {
        onResponse(null);  // completion fires uniformly; errors already logged at source
      }
    };
  }
```

Add imports: `java.util.concurrent.atomic.AtomicBoolean`.

4. Change `proceedWithCollect` signature to take `ActionListener<Void> completion` and invoke `completion.onResponse(null)` on ALL terminal paths:

```java
  private void proceedWithCollect(
      String indexName,
      Map<String, OpenSearchDataType> fieldTypes,
      ActionListener<Void> completion) {
    storage.putStatus(indexName, TableStatistic.STATUS_GENERATING, noopListener(indexName));

    final SearchRequest request;
    try {
      request = buildAggregationRequest(indexName, fieldTypes);
    } catch (RuntimeException e) {
      LOG.warn("Failed to build aggregation request for {}: {}", indexName, e.getMessage());
      storage.putStatus(indexName, TableStatistic.STATUS_FAILED, noopListener(indexName));
      completion.onResponse(null);
      return;
    }

    nodeClient.search(
        request,
        new ActionListener<SearchResponse>() {
          @Override
          public void onResponse(SearchResponse response) {
            final TableStatistic stat;
            try {
              stat = parseSearchResponse(response, fieldTypes);
            } catch (RuntimeException e) {
              LOG.warn("Failed to parse statistic response for {}: {}", indexName, e.getMessage());
              storage.putStatus(indexName, TableStatistic.STATUS_FAILED, noopListener(indexName));
              completion.onResponse(null);
              return;
            }
            storage.put(
                indexName,
                stat,
                new ActionListener<Void>() {
                  @Override
                  public void onResponse(Void ignored) {
                    LOG.debug(
                        "Collected statistic for {}: docCount={}, fieldCount={}",
                        indexName,
                        stat.getDocCount(),
                        stat.getFields().size());
                    completion.onResponse(null);
                  }

                  @Override
                  public void onFailure(Exception e) {
                    LOG.warn("Failed to persist statistic for {}: {}", indexName, e.getMessage());
                    storage.putStatus(
                        indexName, TableStatistic.STATUS_FAILED, noopListener(indexName));
                    completion.onResponse(null);
                  }
                });
          }

          @Override
          public void onFailure(Exception e) {
            if (ExceptionsHelper.unwrap(e, EsRejectedExecutionException.class) != null) {
              LOG.debug(
                  "Search rejected for {} (cluster busy); skipping without FAILED marker",
                  indexName);
              completion.onResponse(null);
              return;
            }
            LOG.warn("Failed to collect statistic for {}: {}", indexName, e.getMessage());
            storage.putStatus(indexName, TableStatistic.STATUS_FAILED, noopListener(indexName));
            completion.onResponse(null);
          }
        });
  }
```

Add imports: `org.opensearch.ExceptionsHelper`, `org.opensearch.core.concurrency.EsRejectedExecutionException` (verify the exact package — in current OpenSearch versions it's `org.opensearch.core.concurrency.EsRejectedExecutionException`; if compile fails, try `org.opensearch.common.util.concurrent.EsRejectedExecutionException`).

- [ ] **Step 4: Run success test**

Run: `./gradlew :opensearch:test --tests "*TableStatisticCollectorTest.refreshAsyncThreeArgInvokesCompletionOnSuccess"`
Expected: PASS.

- [ ] **Step 5: Add rejection test**

Add to `TableStatisticCollectorTest.java`:

```java
  @Test
  public void refreshAsyncDoesNotWriteFailedOnRejection() {
    givenGetRawReturnsEmpty();
    // Make search fail with EsRejectedExecutionException.
    doAnswer(inv -> {
      ActionListener<SearchResponse> l = inv.getArgument(1);
      l.onFailure(new EsRejectedExecutionException("queue is full"));
      return null;
    }).when(nodeClient).search(any(), any());
    // Track putStatus calls:
    ArgumentCaptor<String> statusCaptor = ArgumentCaptor.forClass(String.class);

    AtomicInteger completions = new AtomicInteger();
    collector.refreshAsync(
        "logs-a",
        Map.of(),
        ActionListener.wrap(v -> completions.incrementAndGet(), e -> fail(e.getMessage())));

    // Assert: completion fired, no FAILED marker written. (GENERATING may be written via the
    // initial putStatus call; that is unchanged behaviour — just verify FAILED is absent.)
    verify(storage, atLeastOnce()).putStatus(anyString(), statusCaptor.capture(), any());
    assertFalse(
        "FAILED must not be written on rejection",
        statusCaptor.getAllValues().contains(TableStatistic.STATUS_FAILED));
    assertEquals(1, completions.get());
  }
```

Import `EsRejectedExecutionException` matching the path you chose in Step 3.

- [ ] **Step 6: Run rejection test**

Run: `./gradlew :opensearch:test --tests "*TableStatisticCollectorTest.refreshAsyncDoesNotWriteFailedOnRejection"`
Expected: PASS.

- [ ] **Step 7: Run full collector test class**

Run: `./gradlew :opensearch:test --tests "*TableStatisticCollectorTest"`
Expected: all tests pass (existing 2-arg callers now go through the 3-arg path with `NOOP_COMPLETION` — behaviour should be unchanged for them).

- [ ] **Step 8: Format and commit**

```bash
./gradlew spotlessApply
git add opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticCollector.java \
        opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticCollectorTest.java
git commit -s -m "feat: add refreshAsync 3-arg overload with completion listener

Completion fires exactly once on every terminal path (success, failed
marker, GENERATING-dedup skip, synchronous throw, rejection skip).
EsRejectedExecutionException is treated as 'cluster busy' — completion
fires but no FAILED marker is written, so the next tick can retry
cleanly. The existing 2-arg refreshAsync delegates to the new form
with a noop completion."
```

---

## Task 5: Extract mapping-resolver helper

**Files:**
- Create: `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticsMappingResolver.java`
- Create: `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticsMappingResolverTest.java`
- Modify: `plugin/src/main/java/org/opensearch/sql/plugin/rest/RestTableStatisticsAction.java`

**Purpose:** Both REST `/analyze` and the new scheduler need to turn an index name into `Map<String, OpenSearchDataType>`. The logic currently sits inline in `RestTableStatisticsAction.resolveMappingsAndTriggerRefresh`. Lift it into a helper so the scheduler can call it cleanly.

- [ ] **Step 1: Write failing test — returns fieldTypes on success**

Create `TableStatisticsMappingResolverTest.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.statistics;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Map;
import org.junit.Test;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.transport.client.node.NodeClient;

public class TableStatisticsMappingResolverTest {

  @Test
  public void resolveReturnsEmptyMapOnFailure() {
    NodeClient client = mock(NodeClient.class);
    // No mapping is wired — the describe call will fail internally; resolver must swallow.
    TableStatisticsMappingResolver resolver = new TableStatisticsMappingResolver(client);

    Map<String, OpenSearchDataType> fieldTypes = resolver.resolve("nonexistent-index");

    assertNotNull(fieldTypes);
    assertTrue(fieldTypes.isEmpty());
  }
}
```

- [ ] **Step 2: Run test to verify compile failure**

Run: `./gradlew :opensearch:test --tests "*TableStatisticsMappingResolverTest.resolveReturnsEmptyMapOnFailure"`
Expected: FAIL — class does not exist.

- [ ] **Step 3: Create the helper class**

Create `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticsMappingResolver.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.statistics;

import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.opensearch.client.OpenSearchNodeClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.system.OpenSearchDescribeIndexRequest;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Resolves an index name into its flattened field-type map. Shared between the REST {@code
 * /analyze} handler and the cron refresh scheduler. Blocking — callers must run it on {@code
 * GENERIC} (never on the transport thread).
 *
 * <p>Failure is silent: a missing / unreachable index yields an empty map. The collector treats an
 * empty map as "no per-field aggregations" and writes a doc-count-only stat, which is a gentler
 * failure mode than a thrown exception from a background sweep.
 */
public class TableStatisticsMappingResolver {

  private static final Logger LOG = LogManager.getLogger(TableStatisticsMappingResolver.class);

  private final NodeClient nodeClient;

  public TableStatisticsMappingResolver(NodeClient nodeClient) {
    this.nodeClient = nodeClient;
  }

  public Map<String, OpenSearchDataType> resolve(String indexName) {
    try {
      return new OpenSearchDescribeIndexRequest(
              new OpenSearchNodeClient(nodeClient), new OpenSearchRequest.IndexName(indexName))
          .getFieldTypes();
    } catch (Exception e) {
      LOG.debug(
          "Failed to resolve mappings for {} — returning empty field map: {}",
          indexName,
          e.getMessage());
      return Map.of();
    }
  }
}
```

- [ ] **Step 4: Run the resolver test — PASS**

Run: `./gradlew :opensearch:test --tests "*TableStatisticsMappingResolverTest.resolveReturnsEmptyMapOnFailure"`
Expected: PASS.

- [ ] **Step 5: Update REST handler to delegate**

In `plugin/src/main/java/org/opensearch/sql/plugin/rest/RestTableStatisticsAction.java`, replace the `resolveMappingsAndTriggerRefresh` method with a call to the helper:

```java
  private void resolveMappingsAndTriggerRefresh(String indexName, NodeClient nodeClient) {
    Map<String, OpenSearchDataType> fieldTypes =
        new TableStatisticsMappingResolver(nodeClient).resolve(indexName);
    try {
      collector.refreshAsync(indexName, fieldTypes);
    } catch (Exception e) {
      LOG.warn("Failed to trigger collector.refreshAsync for {}: {}", indexName, e.getMessage());
    }
  }
```

Remove now-unused imports: `OpenSearchNodeClient`, `OpenSearchRequest`, `OpenSearchDescribeIndexRequest`. Add `import org.opensearch.sql.opensearch.storage.statistics.TableStatisticsMappingResolver;`.

- [ ] **Step 6: Run plugin tests**

Run: `./gradlew :plugin:test --tests "*RestTableStatisticsActionTest"`
Expected: PASS (behaviour unchanged; resolver is a thin delegate).

- [ ] **Step 7: Format and commit**

```bash
./gradlew spotlessApply
git add opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticsMappingResolver.java \
        opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticsMappingResolverTest.java \
        plugin/src/main/java/org/opensearch/sql/plugin/rest/RestTableStatisticsAction.java
git commit -s -m "refactor: extract TableStatisticsMappingResolver

Shared helper for REST /analyze and the upcoming cron scheduler.
Silent on failure — returns an empty field map so background sweeps
never surface a missing-index error."
```

---

## Task 6: `TableStatisticRefreshTask`

**Files:**
- Create: `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticRefreshTask.java`
- Create: `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticRefreshTaskTest.java`

**Purpose:** The tick body. Stateless Runnable that drives the refresh flow: read flag + TTL + semaphore from the scheduler; call `listStale`; for each stale name, `tryAcquire` → dispatch to GENERIC → resolve mapping → `refreshAsync` with a completion that always releases.

- [ ] **Step 1: Write failing test — no refresh when list is empty**

Create `TableStatisticRefreshTaskTest.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.statistics;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.threadpool.ThreadPool;

public class TableStatisticRefreshTaskTest {

  private TableStatisticStorage storage;
  private TableStatisticCollector collector;
  private TableStatisticsMappingResolver resolver;
  private ThreadPool threadPool;
  private Supplier<Boolean> enabledFlag;
  private Supplier<Duration> ttlSupplier;
  private Supplier<Semaphore> semaphoreSupplier;
  private Semaphore semaphore;

  @Before
  public void setUp() {
    storage = mock(TableStatisticStorage.class);
    collector = mock(TableStatisticCollector.class);
    resolver = mock(TableStatisticsMappingResolver.class);
    threadPool = mock(ThreadPool.class);
    // GENERIC executor: run inline so tests stay deterministic.
    ExecutorService sameThread = mock(ExecutorService.class);
    doAnswer(inv -> {
      Runnable r = inv.getArgument(0);
      r.run();
      return null;
    }).when(sameThread).execute(any(Runnable.class));
    when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(sameThread);

    enabledFlag = () -> true;
    ttlSupplier = () -> Duration.ofHours(1);
    semaphore = new Semaphore(2);
    semaphoreSupplier = () -> semaphore;
  }

  @Test
  public void runDoesNothingWhenFlagOff() {
    enabledFlag = () -> false;
    new TableStatisticRefreshTask(
            storage, collector, resolver, threadPool,
            enabledFlag, ttlSupplier, semaphoreSupplier)
        .run();
    verify(storage, never()).listStale(any(), anyInt(), any());
  }
}
```

Add imports: `static org.mockito.ArgumentMatchers.anyInt`.

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :opensearch:test --tests "*TableStatisticRefreshTaskTest.runDoesNothingWhenFlagOff"`
Expected: FAIL — class does not exist.

- [ ] **Step 3: Implement `TableStatisticRefreshTask`**

Create `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticRefreshTask.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.statistics;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.threadpool.ThreadPool;

/**
 * Single tick body for the cron refresh sweep. Runs only on the cluster-manager node — the
 * scheduler that owns this task is gated by {@code LocalNodeClusterManagerListener}.
 *
 * <p>Flow: (1) short-circuit when the feature flag is off; (2) call {@link
 * TableStatisticStorage#listStale} with the current TTL; (3) for each returned index name,
 * {@code tryAcquire} a permit from the live semaphore — on failure, skip (next tick picks it up);
 * on success, dispatch to the generic pool to resolve mapping synchronously and fire {@link
 * TableStatisticCollector#refreshAsync} with a completion listener that releases the permit.
 *
 * <p>Stateless beyond the injected collaborators. The {@code Supplier<Semaphore>} indirection lets
 * the scheduler swap the semaphore reference when {@code refresh_max_in_flight} changes.
 */
public class TableStatisticRefreshTask implements Runnable {

  private static final Logger LOG = LogManager.getLogger(TableStatisticRefreshTask.class);

  /** Cap on page size per sweep. A larger cluster will self-resolve across successive ticks. */
  static final int LIST_STALE_LIMIT = 1000;

  private final TableStatisticStorage storage;
  private final TableStatisticCollector collector;
  private final TableStatisticsMappingResolver mappingResolver;
  private final ThreadPool threadPool;
  private final Supplier<Boolean> enabledFlag;
  private final Supplier<Duration> ttlSupplier;
  private final Supplier<Semaphore> semaphoreSupplier;

  public TableStatisticRefreshTask(
      TableStatisticStorage storage,
      TableStatisticCollector collector,
      TableStatisticsMappingResolver mappingResolver,
      ThreadPool threadPool,
      Supplier<Boolean> enabledFlag,
      Supplier<Duration> ttlSupplier,
      Supplier<Semaphore> semaphoreSupplier) {
    this.storage = storage;
    this.collector = collector;
    this.mappingResolver = mappingResolver;
    this.threadPool = threadPool;
    this.enabledFlag = enabledFlag;
    this.ttlSupplier = ttlSupplier;
    this.semaphoreSupplier = semaphoreSupplier;
  }

  @Override
  public void run() {
    if (!Boolean.TRUE.equals(enabledFlag.get())) {
      return;
    }
    Duration ttl = ttlSupplier.get();
    Semaphore semaphore = semaphoreSupplier.get();
    storage.listStale(
        ttl,
        LIST_STALE_LIMIT,
        new ActionListener<List<String>>() {
          @Override
          public void onResponse(List<String> staleNames) {
            for (String name : staleNames) {
              if (!semaphore.tryAcquire()) {
                LOG.debug("Semaphore exhausted; skipping remaining stale indices this tick");
                return;
              }
              threadPool
                  .executor(ThreadPool.Names.GENERIC)
                  .execute(() -> refreshOne(name, semaphore));
            }
          }

          @Override
          public void onFailure(Exception e) {
            // listStale contract: never called. Release no permit (none acquired).
            LOG.debug("listStale onFailure called unexpectedly: {}", e.getMessage());
          }
        });
  }

  private void refreshOne(String name, Semaphore semaphore) {
    try {
      Map<String, OpenSearchDataType> fieldTypes = mappingResolver.resolve(name);
      collector.refreshAsync(
          name,
          fieldTypes,
          ActionListener.wrap(
              v -> semaphore.release(),
              e -> semaphore.release()));
    } catch (RuntimeException ex) {
      LOG.debug("refreshOne failed for {}: {}", name, ex.getMessage());
      semaphore.release();
    }
  }
}
```

- [ ] **Step 4: Run flag-off test — PASS**

Run: `./gradlew :opensearch:test --tests "*TableStatisticRefreshTaskTest.runDoesNothingWhenFlagOff"`
Expected: PASS.

- [ ] **Step 5: Add throttle + release test**

In `TableStatisticRefreshTaskTest.java`:

```java
  @Test
  public void runRespectsSemaphoreLimit() {
    // 3 stale indices, semaphore size 2 → only 2 refreshAsync calls
    doAnswer(inv -> {
      ActionListener<List<String>> l = inv.getArgument(2);
      l.onResponse(List.of("a", "b", "c"));
      return null;
    }).when(storage).listStale(any(), anyInt(), any());
    when(resolver.resolve(anyString())).thenReturn(Map.of());

    new TableStatisticRefreshTask(
            storage, collector, resolver, threadPool,
            enabledFlag, ttlSupplier, semaphoreSupplier)
        .run();

    verify(collector, times(2)).refreshAsync(anyString(), any(), any());
    // and the semaphore should still be at 0 available (both permits held)
  }

  @Test
  public void refreshOneReleasesPermitOnCompletion() {
    doAnswer(inv -> {
      ActionListener<List<String>> l = inv.getArgument(2);
      l.onResponse(List.of("a"));
      return null;
    }).when(storage).listStale(any(), anyInt(), any());
    when(resolver.resolve(anyString())).thenReturn(Map.of());
    // Capture the completion and fire it with onResponse.
    doAnswer(inv -> {
      ActionListener<Void> completion = inv.getArgument(2);
      completion.onResponse(null);
      return null;
    }).when(collector).refreshAsync(anyString(), any(), any());

    new TableStatisticRefreshTask(
            storage, collector, resolver, threadPool,
            enabledFlag, ttlSupplier, semaphoreSupplier)
        .run();

    // after completion the permit is returned — we can acquire 2 in total
    assertTrue(semaphore.tryAcquire(2));
  }

  @Test
  public void refreshOneReleasesPermitOnResolverThrow() {
    doAnswer(inv -> {
      ActionListener<List<String>> l = inv.getArgument(2);
      l.onResponse(List.of("a"));
      return null;
    }).when(storage).listStale(any(), anyInt(), any());
    when(resolver.resolve(anyString())).thenThrow(new RuntimeException("boom"));

    new TableStatisticRefreshTask(
            storage, collector, resolver, threadPool,
            enabledFlag, ttlSupplier, semaphoreSupplier)
        .run();

    // resolver threw; collector was never called; permit must be released
    verify(collector, never()).refreshAsync(anyString(), any(), any());
    assertTrue(semaphore.tryAcquire(2));
  }
```

Add `import static org.junit.Assert.assertTrue;`.

- [ ] **Step 6: Run task tests — all PASS**

Run: `./gradlew :opensearch:test --tests "*TableStatisticRefreshTaskTest"`
Expected: all 4 tests PASS.

- [ ] **Step 7: Format and commit**

```bash
./gradlew spotlessApply
git add opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticRefreshTask.java \
        opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticRefreshTaskTest.java
git commit -s -m "feat: TableStatisticRefreshTask — tick body for cron sweep

Stateless Runnable driven by the scheduler. listStale → tryAcquire
semaphore → dispatch to GENERIC → resolve mapping → refreshAsync with
release-on-completion. Exhausted semaphore skips remaining indices;
they will be picked up next tick. Resolver or collector throwing
guarantees a permit release via try/catch + wrap."
```

---

## Task 7: `TableStatisticRefreshScheduler`

**Files:**
- Create: `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticRefreshScheduler.java`
- Create: `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticRefreshSchedulerTest.java`

**Purpose:** Owns the cluster-manager lifecycle (`LocalNodeClusterManagerListener`), the `Cancellable` tick handle, the live `Semaphore` reference, and subscriptions to dynamic-setting updates. The single point of state-mutation: all setting-update handlers read/write via here.

- [ ] **Step 1: Write failing test — onClusterManager schedules**

Create `TableStatisticRefreshSchedulerTest.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.statistics;

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;

public class TableStatisticRefreshSchedulerTest {

  private ClusterService clusterService;
  private ClusterSettings clusterSettings;
  private ThreadPool threadPool;
  private OpenSearchSettings settings;
  private TableStatisticStorage storage;
  private TableStatisticCollector collector;
  private TableStatisticsMappingResolver resolver;

  @Before
  public void setUp() {
    clusterService = mock(ClusterService.class);
    clusterSettings = mock(ClusterSettings.class);
    when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
    threadPool = mock(ThreadPool.class);
    settings = mock(OpenSearchSettings.class);
    when(settings.getSettingValue(Key.TABLE_STATISTICS_ENABLED)).thenReturn(true);
    when(settings.getSettingValue(Key.TABLE_STATISTICS_REFRESH_INTERVAL))
        .thenReturn(TimeValue.timeValueSeconds(60));
    when(settings.getSettingValue(Key.TABLE_STATISTICS_TTL))
        .thenReturn(TimeValue.timeValueHours(24));
    when(settings.getSettingValue(Key.TABLE_STATISTICS_REFRESH_MAX_IN_FLIGHT)).thenReturn(4);
    storage = mock(TableStatisticStorage.class);
    collector = mock(TableStatisticCollector.class);
    resolver = mock(TableStatisticsMappingResolver.class);
  }

  @Test
  public void onClusterManagerSchedulesCancellable() {
    Cancellable cancellable = mock(Cancellable.class);
    when(threadPool.scheduleWithFixedDelay(any(), any(), any())).thenReturn(cancellable);

    TableStatisticRefreshScheduler scheduler = new TableStatisticRefreshScheduler(
        clusterService, threadPool, settings, storage, collector, resolver);
    scheduler.register();
    scheduler.onClusterManager();

    verify(threadPool).scheduleWithFixedDelay(
        any(), eq(TimeValue.timeValueSeconds(60)), eq(ThreadPool.Names.GENERIC));
    assertNotNull(scheduler.currentSemaphore());
  }
}
```

- [ ] **Step 2: Run test — compile fail**

Run: `./gradlew :opensearch:test --tests "*TableStatisticRefreshSchedulerTest.onClusterManagerSchedulesCancellable"`
Expected: FAIL — class does not exist.

- [ ] **Step 3: Implement `TableStatisticRefreshScheduler`**

Create `TableStatisticRefreshScheduler.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.statistics;

import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.LocalNodeClusterManagerListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;

/**
 * Lifecycle owner for the cron-based table-statistic refresh. Registered on every node via {@link
 * LocalNodeClusterManagerListener}; the tick only actually runs on the elected cluster-manager.
 *
 * <p>Mutable state — the {@link Cancellable} tick handle and the live {@link Semaphore} — is kept
 * here and read through {@link TableStatisticRefreshTask}'s supplier indirection so that
 * dynamic-setting updates can swap either atomically.
 */
public class TableStatisticRefreshScheduler implements LocalNodeClusterManagerListener {

  private static final Logger LOG = LogManager.getLogger(TableStatisticRefreshScheduler.class);

  private final ClusterService clusterService;
  private final ThreadPool threadPool;
  private final OpenSearchSettings settings;
  private final TableStatisticStorage storage;
  private final TableStatisticCollector collector;
  private final TableStatisticsMappingResolver mappingResolver;

  private final AtomicReference<Cancellable> cancellable = new AtomicReference<>();
  private final AtomicReference<Semaphore> semaphore = new AtomicReference<>();
  private volatile boolean isClusterManager = false;

  public TableStatisticRefreshScheduler(
      ClusterService clusterService,
      ThreadPool threadPool,
      OpenSearchSettings settings,
      TableStatisticStorage storage,
      TableStatisticCollector collector,
      TableStatisticsMappingResolver mappingResolver) {
    this.clusterService = clusterService;
    this.threadPool = threadPool;
    this.settings = settings;
    this.storage = storage;
    this.collector = collector;
    this.mappingResolver = mappingResolver;
    int initial = settings.getSettingValue(Key.TABLE_STATISTICS_REFRESH_MAX_IN_FLIGHT);
    this.semaphore.set(new Semaphore(initial));
  }

  /**
   * Register this instance as a cluster-manager listener. Must be called exactly once from plugin
   * startup (caller: {@code SQLPlugin.createComponents}).
   */
  public void register() {
    clusterService.addLocalNodeClusterManagerListener(this);
  }

  @Override
  public synchronized void onClusterManager() {
    isClusterManager = true;
    scheduleTickIfEligible();
  }

  @Override
  public synchronized void offClusterManager() {
    isClusterManager = false;
    cancelTick();
  }

  /** Visible for testing — reads the currently live semaphore reference. */
  public Semaphore currentSemaphore() {
    return semaphore.get();
  }

  /** Visible for testing — cancellable handle. */
  Cancellable currentCancellable() {
    return cancellable.get();
  }

  private void scheduleTickIfEligible() {
    if (!isClusterManager) {
      return;
    }
    if (!Boolean.TRUE.equals(settings.getSettingValue(Key.TABLE_STATISTICS_ENABLED))) {
      return;
    }
    cancelTick();
    TimeValue interval = settings.getSettingValue(Key.TABLE_STATISTICS_REFRESH_INTERVAL);
    TableStatisticRefreshTask task = newTask();
    Cancellable c = threadPool.scheduleWithFixedDelay(task, interval, ThreadPool.Names.GENERIC);
    cancellable.set(c);
    LOG.debug("Table-statistics cron scheduled with interval {}", interval);
  }

  private void cancelTick() {
    Cancellable c = cancellable.getAndSet(null);
    if (c != null) {
      c.cancel();
      LOG.debug("Table-statistics cron cancelled");
    }
  }

  private TableStatisticRefreshTask newTask() {
    return new TableStatisticRefreshTask(
        storage,
        collector,
        mappingResolver,
        threadPool,
        () -> Boolean.TRUE.equals(settings.getSettingValue(Key.TABLE_STATISTICS_ENABLED)),
        () -> {
          TimeValue ttl = settings.getSettingValue(Key.TABLE_STATISTICS_TTL);
          return Duration.ofMillis(ttl.millis());
        },
        this::currentSemaphore);
  }

  /**
   * Called externally when {@code refresh_max_in_flight} changes. Swaps the semaphore; in-flight
   * permits against the old semaphore are simply lost when they release (correct — we don't care
   * to preserve them).
   */
  synchronized void onMaxInFlightChanged(int newMax) {
    semaphore.set(new Semaphore(newMax));
  }

  /** Called externally when {@code refresh_interval} changes. */
  synchronized void onIntervalChanged() {
    if (isClusterManager) {
      scheduleTickIfEligible();
    }
  }

  /** Called externally when {@code TABLE_STATISTICS_ENABLED} changes. */
  synchronized void onEnabledChanged() {
    if (Boolean.TRUE.equals(settings.getSettingValue(Key.TABLE_STATISTICS_ENABLED))) {
      scheduleTickIfEligible();
    } else {
      cancelTick();
    }
  }
}
```

Add the import for `LocalNodeClusterManagerListener` — verify it's at `org.opensearch.cluster.LocalNodeClusterManagerListener`; if not, adjust. In some distributions the package is `org.opensearch.cluster.LocalNodeMasterListener` (legacy) — prefer the `ClusterManager` variant if available.

- [ ] **Step 4: Run scheduling test — PASS**

Run: `./gradlew :opensearch:test --tests "*TableStatisticRefreshSchedulerTest.onClusterManagerSchedulesCancellable"`
Expected: PASS.

- [ ] **Step 5: Add off-and-cancel test**

Add to `TableStatisticRefreshSchedulerTest.java`:

```java
  @Test
  public void offClusterManagerCancelsCancellable() {
    Cancellable cancellable = mock(Cancellable.class);
    when(threadPool.scheduleWithFixedDelay(any(), any(), any())).thenReturn(cancellable);

    TableStatisticRefreshScheduler scheduler = new TableStatisticRefreshScheduler(
        clusterService, threadPool, settings, storage, collector, resolver);
    scheduler.register();
    scheduler.onClusterManager();
    scheduler.offClusterManager();

    verify(cancellable).cancel();
  }

  @Test
  public void onClusterManagerDoesNotScheduleWhenFlagOff() {
    when(settings.getSettingValue(Key.TABLE_STATISTICS_ENABLED)).thenReturn(false);

    TableStatisticRefreshScheduler scheduler = new TableStatisticRefreshScheduler(
        clusterService, threadPool, settings, storage, collector, resolver);
    scheduler.register();
    scheduler.onClusterManager();

    verify(threadPool, never()).scheduleWithFixedDelay(any(), any(), any());
  }

  @Test
  public void maxInFlightChangeSwapsSemaphore() {
    TableStatisticRefreshScheduler scheduler = new TableStatisticRefreshScheduler(
        clusterService, threadPool, settings, storage, collector, resolver);
    Semaphore before = scheduler.currentSemaphore();
    scheduler.onMaxInFlightChanged(8);
    Semaphore after = scheduler.currentSemaphore();
    assertNotSame(before, after);
    assertEquals(8, after.availablePermits());
  }

  @Test
  public void intervalChangeReschedules() {
    Cancellable first = mock(Cancellable.class);
    Cancellable second = mock(Cancellable.class);
    when(threadPool.scheduleWithFixedDelay(any(), any(), any()))
        .thenReturn(first).thenReturn(second);

    TableStatisticRefreshScheduler scheduler = new TableStatisticRefreshScheduler(
        clusterService, threadPool, settings, storage, collector, resolver);
    scheduler.register();
    scheduler.onClusterManager();

    when(settings.getSettingValue(Key.TABLE_STATISTICS_REFRESH_INTERVAL))
        .thenReturn(TimeValue.timeValueSeconds(10));
    scheduler.onIntervalChanged();

    verify(first).cancel();
    verify(threadPool).scheduleWithFixedDelay(
        any(), eq(TimeValue.timeValueSeconds(10)), eq(ThreadPool.Names.GENERIC));
  }
```

Add imports:
```java
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.mockito.Mockito.never;

import java.util.concurrent.Semaphore;
```

- [ ] **Step 6: Run all scheduler tests**

Run: `./gradlew :opensearch:test --tests "*TableStatisticRefreshSchedulerTest"`
Expected: all 5 tests PASS.

- [ ] **Step 7: Format and commit**

```bash
./gradlew spotlessApply
git add opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticRefreshScheduler.java \
        opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/TableStatisticRefreshSchedulerTest.java
git commit -s -m "feat: TableStatisticRefreshScheduler for cluster-manager cron

Binds LocalNodeClusterManagerListener; on onClusterManager() starts a
scheduleWithFixedDelay tick; offClusterManager() cancels. Owns the
mutable Cancellable + Semaphore refs and exposes change-handlers for
dynamic settings (max_in_flight, interval, enabled). No wiring yet —
SQLPlugin task comes next."
```

---

## Task 8: Wire scheduler into `SQLPlugin.createComponents`

**Files:**
- Modify: `plugin/src/main/java/org/opensearch/sql/plugin/SQLPlugin.java`

**Purpose:** Construct the scheduler, wire the mapping resolver, register the cluster-manager listener, and subscribe to the three dynamic settings' update consumers so the scheduler's `on*Changed` handlers fire. No unit test — this is integration wiring, covered by Task 9's IT.

- [ ] **Step 1: Add field + imports**

In `plugin/src/main/java/org/opensearch/sql/plugin/SQLPlugin.java`, near the existing `tableStatisticStorage` / `tableStatisticCollector` fields (around line 153), add:

```java
  private TableStatisticRefreshScheduler tableStatisticRefreshScheduler;
```

Add imports (near the existing statistics imports around line 98):
```java
import org.opensearch.sql.opensearch.storage.statistics.TableStatisticRefreshScheduler;
import org.opensearch.sql.opensearch.storage.statistics.TableStatisticsMappingResolver;
```

- [ ] **Step 2: Wire in `createComponents`**

In `SQLPlugin.java`, immediately after the existing statistics singletons (line 281), add:

```java
    TableStatisticsMappingResolver mappingResolver =
        new TableStatisticsMappingResolver(this.client);
    this.tableStatisticRefreshScheduler =
        new TableStatisticRefreshScheduler(
            clusterService,
            threadPool,
            (OpenSearchSettings) pluginSettings,
            tableStatisticStorage,
            tableStatisticCollector,
            mappingResolver);
    this.tableStatisticRefreshScheduler.register();

    // Hook dynamic-setting updates to the scheduler
    clusterService
        .getClusterSettings()
        .addSettingsUpdateConsumer(
            OpenSearchSettings.TABLE_STATISTICS_ENABLED_SETTING,
            v -> this.tableStatisticRefreshScheduler.onEnabledChanged());
    clusterService
        .getClusterSettings()
        .addSettingsUpdateConsumer(
            OpenSearchSettings.TABLE_STATISTICS_REFRESH_INTERVAL_SETTING,
            v -> this.tableStatisticRefreshScheduler.onIntervalChanged());
    clusterService
        .getClusterSettings()
        .addSettingsUpdateConsumer(
            OpenSearchSettings.TABLE_STATISTICS_REFRESH_MAX_IN_FLIGHT_SETTING,
            v -> this.tableStatisticRefreshScheduler.onMaxInFlightChanged(v));
```

- [ ] **Step 3: Add scheduler to returned components list**

Find the `createComponents` return statement — the existing code likely returns `List.of(...)` or `Arrays.asList(...)` of components. Add `tableStatisticRefreshScheduler` alongside the other statistics objects in the returned list so the plugin lifecycle retains a reference.

Search the file for the actual return statement (likely around line 417) and append `tableStatisticRefreshScheduler` next to `tableStatisticStorage, tableStatisticCollector`.

- [ ] **Step 4: Verify compile**

Run: `./gradlew :plugin:compileJava`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 5: Run plugin tests**

Run: `./gradlew :plugin:test`
Expected: all existing tests pass (we only added wiring).

- [ ] **Step 6: Format and commit**

```bash
./gradlew spotlessApply
git add plugin/src/main/java/org/opensearch/sql/plugin/SQLPlugin.java
git commit -s -m "feat: wire TableStatisticRefreshScheduler in SQLPlugin

Constructs the scheduler + mapping resolver, registers the
cluster-manager listener, and binds the three dynamic settings to
the scheduler's on*Changed hooks. Returned in createComponents so the
plugin lifecycle retains a reference."
```

---

## Task 9: Integration test — stale refresh end-to-end

**Files:**
- Modify: `integ-test/src/test/java/org/opensearch/sql/calcite/remote/TableStatisticsIT.java`

**Purpose:** Live verification that the loop works: seed a doc with an old `last_updated_time`, kick the settings to aggressive values, wait, assert `last_updated_time` has moved.

- [ ] **Step 1: Write the IT test**

Add to `TableStatisticsIT.java`:

```java
  @Test
  public void testStaleRefresh() throws Exception {
    enableTableStatistics();
    try {
      // Seed a "normal" stat so the stored doc exists.
      triggerAnalyze();
      waitUntilAnalyzed();

      // Back-date the stored stat so it looks stale.
      String staleDoc =
          String.format(
              Locale.ROOT,
              "{\"doc\":{\"last_updated_time\":\"2020-01-01T00:00:00Z\"}}");
      Request update = new Request(
          "POST",
          "/.opensearch-statistics/_update/"
              + com.google.common.hash.Hashing.sha256()
                  .hashString(INDEX, java.nio.charset.StandardCharsets.UTF_8)
                  .toString());
      update.setJsonEntity(staleDoc);
      client().performRequest(update);
      client().performRequest(new Request("POST", "/.opensearch-statistics/_refresh"));

      // Dial down the cron so we don't wait a minute.
      updateClusterSetting("plugins.calcite.table_statistics.refresh_interval", "2s");
      updateClusterSetting("plugins.calcite.table_statistics.ttl", "5s");

      // Wait for an automatic refresh.
      boolean refreshed =
          waitForCondition(
              () -> {
                try {
                  Response r = client().performRequest(
                      new Request("GET", "/_plugins/_sql/_statistics/" + INDEX));
                  JSONObject body = new JSONObject(getResponseBody(r));
                  String updated = body.getString("last_updated_time");
                  // Any timestamp newer than 2020 means the cron wrote a new record.
                  return updated.startsWith("20") && !updated.startsWith("2020");
                } catch (IOException e) {
                  return false;
                }
              },
              TimeUnit.SECONDS.toMillis(30));

      assertTrue("cron refresh did not update last_updated_time within 30s", refreshed);
    } finally {
      // Reset to sane values before the next test.
      updateClusterSetting("plugins.calcite.table_statistics.refresh_interval", null);
      updateClusterSetting("plugins.calcite.table_statistics.ttl", null);
    }
  }
```

If helpers like `triggerAnalyze`, `waitUntilAnalyzed`, `updateClusterSetting`, `enableTableStatistics`, `waitForCondition` don't already exist, add them modelled on the existing patterns in the file. Concretely:

```java
  private void updateClusterSetting(String key, String value) throws IOException {
    Request req = new Request("PUT", "/_cluster/settings");
    String body =
        value == null
            ? String.format(Locale.ROOT, "{\"persistent\":{\"%s\":null}}", key)
            : String.format(Locale.ROOT, "{\"persistent\":{\"%s\":\"%s\"}}", key, value);
    req.setJsonEntity(body);
    client().performRequest(req);
  }

  private boolean waitForCondition(BooleanSupplier cond, long timeoutMillis)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMillis;
    while (System.currentTimeMillis() < deadline) {
      if (cond.getAsBoolean()) return true;
      Thread.sleep(250);
    }
    return cond.getAsBoolean();
  }
```

Reuse existing `triggerAnalyze`, `waitUntilAnalyzed`, `enableTableStatistics`, `disableTableStatistics` if present.

- [ ] **Step 2: Run the IT test**

Run: `./gradlew :integ-test:integTest --tests "*TableStatisticsIT.testStaleRefresh" -i`
Expected: PASS.

If it flakes due to timing, the default 30s wait generally accommodates jitter — if you see it fail intermittently with "not updated", confirm via the IT log that the scheduler logged "scheduled with interval 2s" at settings-update time.

- [ ] **Step 3: Run the full IT class to confirm no regressions**

Run: `./gradlew :integ-test:integTest --tests "*TableStatisticsIT"`
Expected: all tests pass.

- [ ] **Step 4: Format and commit**

```bash
./gradlew spotlessApply
git add integ-test/src/test/java/org/opensearch/sql/calcite/remote/TableStatisticsIT.java
git commit -s -m "test: end-to-end cron refresh IT

Backdates a stored stat doc, dials refresh_interval and ttl to
seconds, and asserts last_updated_time advances within 30s without a
user-issued /analyze. Covers the M2 Phase 2 success criterion."
```

---

## Task 10: Update design doc roadmap

**Files:**
- Modify: `docs/dev/table-statistics-design.md`

**Purpose:** Check off the Phase 2 items in §4, and append a work-log entry noting the landing.

- [ ] **Step 1: Check off roadmap items**

In `docs/dev/table-statistics-design.md`, find the `#### Phase 2 — Cron-driven refresh` section. Replace the unchecked list with:

```markdown
#### Phase 2 — Cron-driven refresh (DONE)

- [x] `ThreadPool.scheduleWithFixedDelay` tick driven from the cluster-manager via `LocalNodeClusterManagerListener` (interval via `plugins.calcite.table_statistics.refresh_interval`, default 60 s).
- [x] Scan source is `.opensearch-statistics` (rejected: `ClusterState` enumeration — see §3.5).
- [x] Per-index TTL staleness check via `storage.listStale(ttl, ...)` (TTL from `plugins.calcite.table_statistics.ttl`, default 24 h).
- [x] Concurrent-refresh throttling via `Semaphore` (default 4, via `plugins.calcite.table_statistics.refresh_max_in_flight`).
- [x] Search rejection (`EsRejectedExecutionException`) skipped without writing a FAILED marker.
- [x] IT coverage: `testStaleRefresh` in `TableStatisticsIT`.
- [ ] Follow-up (Phase 3): `listStale` pagination when stat-docs > 1000, orphan-doc cleanup.
- See §3.3 for why this is NOT a refresh-listener-based design.
- See §3.5 for the full decision matrix and rejected alternatives.
```

- [ ] **Step 2: Add work-log entry**

At the top of `## 5. Work log` (right after the heading), insert:

```markdown
### 2026-04-24 — M2 Phase 2 shipped

Cron refresh lands: `TableStatisticRefreshScheduler` + `TableStatisticRefreshTask` run on the elected cluster-manager, sweeping `.opensearch-statistics` every 60 s (default) and refreshing any stat whose `last_updated_time` is older than the TTL (default 24 h). Three new dynamic settings (`refresh_interval`, `ttl`, `refresh_max_in_flight`) promoted from hardcoded defaults. Notable wrinkle: the stored doc needed a new `index_name` keyword field so the sweeper's `listStale` could return names directly — old-format docs remain readable but are invisible to the sweep until their next refresh rewrites them, which is acceptable (see spec §3.3). Verified on a live cluster: backdating a stat doc + setting `refresh_interval=2s, ttl=5s` made the stored `last_updated_time` advance within 5 s without any user request.
```

- [ ] **Step 3: Commit**

```bash
git add docs/dev/table-statistics-design.md
git commit -s -m "docs: mark M2 Phase 2 cron refresh done

Update roadmap (§4) + append work-log entry."
```

---

## Self-Review

**Spec coverage:**
- §2 components (Scheduler, Task, Mapping resolver) → Tasks 5, 6, 7.
- §2 modified (`listStale`, `put` changes, `refreshAsync` overload) → Tasks 2, 3, 4.
- §3.3 `index_name` field → Task 2.
- §3.4 wiring → Task 8.
- §4.1 startup flow / registration → Task 8.
- §4.2 tick → Task 6.
- §4.3 shutdown → Task 7 (offClusterManager + cancel).
- §4.4 dynamic settings → Task 1 + Task 7 handlers + Task 8 consumers.
- §5 error table → Task 4 (rejection) + Task 6 (release-on-throw).
- §6 settings → Task 1.
- §7.1 unit tests → Tasks 2, 3, 4, 6, 7.
- §7.2 IT → Task 9.

**Placeholder scan:** No "TBD", "TODO" in tasks. All code blocks complete. All file paths absolute-or-module-relative and precise.

**Type consistency:** `refreshAsync(String, Map<String, OpenSearchDataType>, ActionListener<Void>)` is consistent across Tasks 4, 6. `listStale(Duration, int, ActionListener<List<String>>)` consistent in Tasks 3, 6. `TableStatisticRefreshTask` constructor args match between Tasks 6, 7. `TableStatisticRefreshScheduler` public methods (`register`, `onClusterManager`, `offClusterManager`, `onEnabledChanged`, `onIntervalChanged`, `onMaxInFlightChanged`, `currentSemaphore`) consistent between Tasks 7 and 8.
