# Supplementary Context for Index Insight PPL Integration

This document supplements `2026-04-20-index-insight-ppl-integration.md` with pre-explored context about ml-commons (the external dependency) and the current SQL codebase. The executing session runs in this worktree (`/Volumes/workplace/OpenSearch/index-insight/`, a worktree of the SQL project) and will NOT have the ml-commons repo available locally.

## Working Directory

- **Worktree (where all code changes happen):** `/Volumes/workplace/OpenSearch/index-insight/`
- **Branch:** `index-insight`
- **Upstream parent:** OpenSearch SQL project
- **ml-commons repo (reference only, not in this worktree):** `/Volumes/workplace/OpenSearch/ml-commons/`

All `./gradlew` commands run from `/Volumes/workplace/OpenSearch/index-insight/`.

## External dependency (no code changes needed)

The `opensearch/build.gradle` already declares:

```gradle
implementation group: 'org.opensearch', name:'opensearch-ml-client', version: "${opensearch_build}"
```

This gives us access to the `org.opensearch.ml.common.*` classes below. No new dependency needed.

## ml-commons API reference

### Transport action (cross-plugin call)

**Action class:** `org.opensearch.ml.common.transport.indexInsight.MLIndexInsightGetAction`
- `INSTANCE` singleton
- Action name: `"cluster:admin/opensearch/ml/index_insight/get"`

**Request class:** `org.opensearch.ml.common.transport.indexInsight.MLIndexInsightGetRequest`

```java
public MLIndexInsightGetRequest(String indexName, MLIndexInsightType targetIndexInsight, String tenantId)
```

Required fields:
- `indexName` — OpenSearch index name
- `targetIndexInsight` — enum value, use `MLIndexInsightType.STATISTICAL_DATA`
- `tenantId` — pass `null` for default tenant

Getters: `getIndexName()`, `getTargetIndexInsight()`, `getTenantId()`

**Response class:** `org.opensearch.ml.common.transport.indexInsight.MLIndexInsightGetResponse`

```java
MLIndexInsightGetResponse.builder().indexInsight(IndexInsight).build()
// Accessor:
response.getIndexInsight()
```

### Model classes

**`org.opensearch.ml.common.indexInsight.IndexInsight`**

Builder-based; fields:
- `String index`
- `String content` — JSON string (for STATISTICAL_DATA task)
- `IndexInsightTaskStatus status`
- `MLIndexInsightType taskType`
- `Instant lastUpdatedTime`
- `String tenantId`

Construction in tests:
```java
IndexInsight.builder()
    .index("test")
    .content(jsonContent)
    .status(IndexInsightTaskStatus.COMPLETED)
    .taskType(MLIndexInsightType.STATISTICAL_DATA)
    .lastUpdatedTime(Instant.now())
    .build();
```

**`org.opensearch.ml.common.indexInsight.MLIndexInsightType`** (enum)
- `STATISTICAL_DATA` — this is what we consume
- `FIELD_DESCRIPTION`
- `LOG_RELATED_INDEX_CHECK`
- `ALL`

**`org.opensearch.ml.common.indexInsight.IndexInsightTaskStatus`** (enum)
- `GENERATING`
- `COMPLETED`
- `FAILED`

### STATISTICAL_DATA content JSON format

The `content` field of `IndexInsight` is a JSON string produced by `StatisticalDataTask`. Exact structure:

```json
{
  "example_docs": [
    {"field_a": "value", "field_b": 42}
  ],
  "important_column_and_distribution": {
    "<field_name>": {
      "type": "<keyword|text|long|integer|double|float|short|date>",
      "unique_terms": [...],
      "unique_count": <number>,
      "min_value": <number or date string>,
      "max_value": <number or date string>
    }
  }
}
```

**Key constants (from `StatisticalDataTask.java`):**
- `IMPORTANT_COLUMN_KEYWORD = "important_column_and_distribution"`
- `EXAMPLE_DOC_KEYWORD = "example_docs"`

**Note on `null_ratio`:** The plan's `FieldStatistic.fromInsightMap()` parses a `null_ratio` key, but **StatisticalDataTask does NOT currently emit this field**. The code defensively defaults to `0.0` when the key is absent. This is intentional forward-compatibility — don't remove the parse logic.

### Error conditions to handle

The transport action can fail with:
- `"Index insight feature is not enabled yet..."` — if `plugins.ml_commons.index_insight_feature_enabled=false` on the cluster
- `"You are not enabled to use index insight yet..."` — if per-tenant config is disabled
- `"Index insight is being generated, please wait..."` — if status=GENERATING and not timed out (returns `TOO_MANY_REQUESTS`)
- Generic `RuntimeException` / other exceptions

In all failure cases, `IndexInsightStatisticProvider.getStatistic()` returns `null` and the cost model falls back to existing heuristic (using `maxResultWindow`).

## SQL project reference — current state (pre-plan)

These files/line numbers are valid as of commit `a375f98f5` on the `index-insight` branch.

### `AbstractCalciteIndexScan.java`

Path: `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/scan/AbstractCalciteIndexScan.java`

Current baseline is hardcoded in two places:

**Line 130** (in `estimateRowCount`):
```java
osIndex.getMaxResultWindow().doubleValue(),
```

**Line 173** (in `computeSelfCost`):
```java
double dRows = osIndex.getMaxResultWindow().doubleValue(), dCpu = 0.0d;
```

Task 6 replaces both with `getBaselineRowCount()`.

### `OpenSearchIndex.java`

Path: `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/OpenSearchIndex.java`

- Class extends `AbstractOpenSearchTable` (no explicit `getStatistic()` override)
- Constructor (line 98): `OpenSearchIndex(OpenSearchClient client, Settings settings, String indexName)`
- `client.getNodeClient()` returns `Optional<NodeClient>` — used to execute transport actions
- Precedent for NodeClient usage: `visitMLCommons()` (~line 255) — look there for the pattern if uncertain

### `AbstractOpenSearchTable.java`

Path: `core/src/main/java/org/opensearch/sql/calcite/plan/AbstractOpenSearchTable.java`

Extends `org.apache.calcite.schema.impl.AbstractTable` which provides default `getStatistic()` returning `Statistics.UNKNOWN`. Do NOT add `getStatistic()` here — add it directly on `OpenSearchIndex` (per Task 5) to keep `AbstractOpenSearchTable` general.

### `OpenSearchClient.java`

Path: `opensearch/src/main/java/org/opensearch/sql/opensearch/client/OpenSearchClient.java`

Key method already exists (line ~101):
```java
Optional<NodeClient> getNodeClient();
```

No changes needed.

### Settings registration pattern

Path: `opensearch/src/main/java/org/opensearch/sql/opensearch/setting/OpenSearchSettings.java`

**Existing precedent** — lines ~154-181 show Calcite settings registration:

```java
public static final Setting<Boolean> CALCITE_ENGINE_ENABLED_SETTING =
    Setting.boolSetting(
        Key.CALCITE_ENGINE_ENABLED.getKeyValue(),
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic);
```

And registration in `pluginSettings()` map (lines ~648-691). Task 4 follows this pattern exactly.

### Test infrastructure

**Existing cost tests** — `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/scan/CalciteIndexScanCostTest.java`

Setup pattern (lines 69-88):
```java
@BeforeEach
void setUp() {
  RelTraitSet traitSet = mock(RelTraitSet.class);
  when(cluster.traitSetOf(any(Convention.class))).thenReturn(traitSet);
  when(osIndex.getMaxResultWindow()).thenReturn(10000);
  Settings settings = mock(Settings.class);
  when(settings.getSettingValue(Key.CALCITE_PUSHDOWN_ROWCOUNT_ESTIMATION_FACTOR)).thenReturn(0.9);
  when(osIndex.getSettings()).thenReturn(settings);
  // ...
}
```

**Important:** Existing tests in `CalciteIndexScanCostTest` do NOT currently stub `osIndex.getStatistic()`. After Task 6's change, `AbstractCalciteIndexScan.getBaselineRowCount()` calls `osIndex.getStatistic()`. Mockito returns `null` for unstubbed methods on mocks — the `instanceof` check on `null` is `false`, so it falls through to `maxResultWindow` correctly. Existing tests continue to pass without modification.

### Build/test commands

Run specific test class:
```bash
cd /Volumes/workplace/OpenSearch/index-insight
./gradlew opensearch:test --tests "org.opensearch.sql.opensearch.storage.statistics.FieldStatisticTest" --no-build-cache
```

Run all opensearch module tests:
```bash
./gradlew opensearch:test --no-build-cache
```

Compile only:
```bash
./gradlew opensearch:compileJava --no-build-cache
```

## Why this design (rationale)

**Q: Why not add the statistic API to ml-commons' public `MachineLearningClient` SDK?**
A: `MachineLearningClient` doesn't expose Index Insight methods yet. Using raw transport action is the existing pattern — see `MLCommonsOperator` in `opensearch/src/main/java/org/opensearch/sql/opensearch/planner/physical/MLCommonsOperator.java` which also calls ml-commons transport actions directly via `NodeClient`.

**Q: Why blocking fetch with timeout in `IndexInsightStatisticProvider`?**
A: Calcite's `Statistic.getRowCount()` is synchronous. Making it async would require restructuring the entire planner. A 5s timeout with graceful null fallback is the pragmatic choice. Subsequent calls hit the in-memory cache.

**Q: Why default the feature flag to `false`?**
A: Index Insight requires:
1. `plugins.ml_commons.index_insight_feature_enabled=true` cluster setting
2. Per-tenant enablement via `PUT /_plugins/_ml/index_insight_config`
3. An LLM agent configured under `os_index_insight_agent`

None of these are guaranteed in a typical SQL-plugin deployment. Opt-in avoids startup errors for users who don't use Index Insight.

**Q: Why cache in `IndexInsightStatisticProvider` instead of relying on `cachedStatistic` in `OpenSearchIndex`?**
A: Both. The `OpenSearchIndex.cachedStatistic` field handles the per-instance case; `IndexInsightStatisticProvider` cache would help if multiple `OpenSearchIndex` instances share a provider. The plan creates a new provider per `OpenSearchIndex`, so the provider cache is redundant today — but it's trivially small and enables future reuse (e.g., plugin-scoped provider singleton).

## Conversation context (what was decided and why)

The following design choices came out of the planning conversation and are worth preserving:

1. **Scope is intentionally narrow.** Only `maxResultWindow` → real `rowCount` is changed in the cost model. Per-field selectivity (using `FieldStatistic.equalitySelectivity()` in `FILTER` case) is listed as future work — it requires extracting field names from `RexNode` conditions, which is significantly more involved. Establishing the foundation first, then iterating, is preferred.

2. **The row count baseline has the highest leverage.** Selectivity heuristics compound on top of the baseline. Fixing the baseline from 10k → actual doc count (e.g., 100M for a real log index) immediately makes cost comparisons across operators (join reorder, pushdown vs non-pushdown) meaningful, without needing field-level stats yet.

3. **24-hour TTL is acceptable for this use case.** Query optimizer statistics tolerate staleness — cardinality shifting from 1000 to 1200 doesn't change plan selection. Only order-of-magnitude changes matter. Index Insight's TTL-based refresh is a fine fit.

4. **No changes to ml-commons.** This plan is entirely consumer-side. If later iterations need a "force refresh" API or a `null_ratio` in the output, those become separate plans against the ml-commons repo.
