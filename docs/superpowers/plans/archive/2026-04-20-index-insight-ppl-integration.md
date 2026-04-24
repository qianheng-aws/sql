# Index Insight Integration with PPL Calcite Optimizer

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable the PPL query optimizer (Calcite-based) to use Index Insight statistical data for cost-based decisions, replacing hardcoded heuristics with real cardinality, min/max, and null-ratio data.

**Architecture:** The SQL plugin already depends on `opensearch-ml-client`. We add a `IndexInsightStatisticProvider` in the SQL opensearch module that fetches STATISTICAL_DATA from ml-commons via transport action, parses the JSON content into a `IndexInsightStatistic` implementing Calcite's `Statistic` interface, and wires it into `OpenSearchIndex.getStatistic()`. The cost model in `AbstractCalciteIndexScan` is then updated to use real row counts and selectivity estimates when available, falling back to existing heuristics when Index Insight is disabled or unavailable.

**Tech Stack:** Java 21, Apache Calcite, OpenSearch transport client, ml-commons transport actions (`MLIndexInsightGetAction`), Gson, JUnit 5 + Mockito

**Worktree:** `/Volumes/workplace/OpenSearch/index-insight/` (branch `index-insight`, a worktree of the SQL project). All implementation changes and `./gradlew` commands happen here.

**Supplementary context:** See `2026-04-20-index-insight-ppl-integration-context.md` in this same directory for ml-commons API details, current-state file references with line numbers, design rationale, and conversation decisions. Read it before starting — the executor will not have access to the ml-commons repo locally.

---

## File Structure

| Action | File (relative to worktree root) | Responsibility |
|--------|------------------------|----------------|
| Create | `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/IndexInsightStatistic.java` | Calcite `Statistic` impl backed by parsed Index Insight data |
| Create | `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/IndexInsightStatisticProvider.java` | Fetches & caches Index Insight STATISTICAL_DATA via transport action |
| Create | `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/FieldStatistic.java` | Per-field stats record (cardinality, min, max, topTerms, nullRatio) |
| Modify | `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/OpenSearchIndex.java:55-108` | Override `getStatistic()` to return `IndexInsightStatistic` |
| Modify | `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/scan/AbstractCalciteIndexScan.java:126-234` | Use real row count and selectivity from statistic when available |
| Modify | `opensearch/src/main/java/org/opensearch/sql/opensearch/client/OpenSearchClient.java` | Add `getNodeClient()` usage note (already exists, no code change needed) |
| Modify | `common/src/main/java/org/opensearch/sql/common/setting/Settings.java` | Add `INDEX_INSIGHT_STATISTICS_ENABLED` setting key |
| Modify | `opensearch/src/main/java/org/opensearch/sql/opensearch/setting/OpenSearchSettings.java` | Register the new setting |
| Create | `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/IndexInsightStatisticTest.java` | Unit tests for Statistic impl |
| Create | `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/IndexInsightStatisticProviderTest.java` | Unit tests for provider/fetcher |
| Create | `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/FieldStatisticTest.java` | Unit tests for field stats parsing |
| Modify | `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/scan/CalciteIndexScanCostTest.java` | Add tests for stats-aware cost computation |
| Modify | `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/OpenSearchIndexTest.java` | Test getStatistic() returns IndexInsightStatistic |

---

## Task 1: FieldStatistic — Per-field statistics record

**Files:**
- Create: `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/FieldStatistic.java`
- Test: `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/FieldStatisticTest.java`

This record holds parsed per-field statistics from the Index Insight `important_column_and_distribution` JSON.

- [ ] **Step 1: Write the failing test for FieldStatistic parsing**

```java
package org.opensearch.sql.opensearch.storage.statistics;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FieldStatisticTest {

  @Test
  void parseFromInsightMap_keywordField() {
    Map<String, Object> fieldData = Map.of(
        "type", "keyword",
        "unique_terms", List.of("GET", "POST", "PUT", "DELETE", "PATCH"),
        "unique_count", 5.0
    );
    FieldStatistic stat = FieldStatistic.fromInsightMap(fieldData);
    assertEquals("keyword", stat.type());
    assertEquals(5L, stat.cardinality());
    assertEquals(List.of("GET", "POST", "PUT", "DELETE", "PATCH"), stat.topTerms());
    assertNull(stat.minValue());
    assertNull(stat.maxValue());
  }

  @Test
  void parseFromInsightMap_longField() {
    Map<String, Object> fieldData = Map.of(
        "type", "long",
        "unique_count", 10000.0,
        "unique_terms", List.of(200.0, 301.0, 404.0, 500.0, 502.0),
        "min_value", 100.0,
        "max_value", 99999.0
    );
    FieldStatistic stat = FieldStatistic.fromInsightMap(fieldData);
    assertEquals(10000L, stat.cardinality());
    assertEquals(100.0, stat.minValue());
    assertEquals(99999.0, stat.maxValue());
  }

  @Test
  void parseFromInsightMap_dateField() {
    Map<String, Object> fieldData = Map.of(
        "type", "date",
        "min_value", "2024-01-01T00:00:00Z",
        "max_value", "2024-12-31T23:59:59Z"
    );
    FieldStatistic stat = FieldStatistic.fromInsightMap(fieldData);
    assertEquals(0L, stat.cardinality());
    assertEquals("2024-01-01T00:00:00Z", stat.minValue());
    assertEquals("2024-12-31T23:59:59Z", stat.maxValue());
  }

  @Test
  void parseFromInsightMap_emptyMap() {
    Map<String, Object> fieldData = Map.of("type", "text");
    FieldStatistic stat = FieldStatistic.fromInsightMap(fieldData);
    assertEquals("text", stat.type());
    assertEquals(0L, stat.cardinality());
    assertNull(stat.minValue());
    assertNull(stat.maxValue());
    assertTrue(stat.topTerms().isEmpty());
  }

  @Test
  void equalitySelectivity_usesCardinality() {
    FieldStatistic stat = new FieldStatistic("keyword", 100L, null, null, List.of(), 0.0);
    assertEquals(0.01, stat.equalitySelectivity(), 1e-9);
  }

  @Test
  void equalitySelectivity_zeroCardinality_returnsDefault() {
    FieldStatistic stat = new FieldStatistic("keyword", 0L, null, null, List.of(), 0.0);
    // Falls back to Calcite default guess: 0.15
    assertEquals(0.15, stat.equalitySelectivity(), 1e-9);
  }

  @Test
  void rangeSelectivity_usesMinMax() {
    FieldStatistic stat = new FieldStatistic("long", 1000L, 0.0, 100.0, List.of(), 0.0);
    // Range [20, 80] → (80-20)/(100-0) = 0.6
    assertEquals(0.6, stat.rangeSelectivity(20.0, 80.0), 1e-9);
  }

  @Test
  void rangeSelectivity_noMinMax_returnsDefault() {
    FieldStatistic stat = new FieldStatistic("long", 1000L, null, null, List.of(), 0.0);
    assertEquals(0.5, stat.rangeSelectivity(20.0, 80.0), 1e-9);
  }

  @Test
  void nullRatio_returnsStoredValue() {
    FieldStatistic stat = new FieldStatistic("keyword", 100L, null, null, List.of(), 0.3);
    assertEquals(0.3, stat.nullRatio(), 1e-9);
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Volumes/workplace/OpenSearch/index-insight && ./gradlew opensearch:test --tests "org.opensearch.sql.opensearch.storage.statistics.FieldStatisticTest" --no-build-cache 2>&1 | tail -5`
Expected: compilation failure — `FieldStatistic` class does not exist

- [ ] **Step 3: Implement FieldStatistic**

```java
package org.opensearch.sql.opensearch.storage.statistics;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Per-field statistics parsed from Index Insight STATISTICAL_DATA content.
 *
 * @param type        OpenSearch field type (keyword, long, date, etc.)
 * @param cardinality approximate number of distinct values
 * @param minValue    minimum value (numeric or date string), null if unavailable
 * @param maxValue    maximum value (numeric or date string), null if unavailable
 * @param topTerms    top-K frequent terms
 * @param nullRatio   fraction of documents where this field is null (0.0–1.0)
 */
public record FieldStatistic(
    String type,
    long cardinality,
    Object minValue,
    Object maxValue,
    List<Object> topTerms,
    double nullRatio
) {

  private static final double DEFAULT_EQUALITY_SELECTIVITY = 0.15;
  private static final double DEFAULT_RANGE_SELECTIVITY = 0.5;

  @SuppressWarnings("unchecked")
  public static FieldStatistic fromInsightMap(Map<String, Object> fieldData) {
    String type = (String) fieldData.getOrDefault("type", "unknown");

    double rawCardinality = fieldData.containsKey("unique_count")
        ? ((Number) fieldData.get("unique_count")).doubleValue()
        : 0.0;
    long cardinality = (long) rawCardinality;

    Object minValue = fieldData.get("min_value");
    Object maxValue = fieldData.get("max_value");

    List<Object> topTerms = fieldData.containsKey("unique_terms")
        ? (List<Object>) fieldData.get("unique_terms")
        : Collections.emptyList();

    double nullRatio = fieldData.containsKey("null_ratio")
        ? ((Number) fieldData.get("null_ratio")).doubleValue()
        : 0.0;

    return new FieldStatistic(type, cardinality, minValue, maxValue, topTerms, nullRatio);
  }

  public double equalitySelectivity() {
    return cardinality > 0 ? 1.0 / cardinality : DEFAULT_EQUALITY_SELECTIVITY;
  }

  public double rangeSelectivity(double low, double high) {
    if (minValue instanceof Number minNum && maxValue instanceof Number maxNum) {
      double range = maxNum.doubleValue() - minNum.doubleValue();
      if (range > 0) {
        return Math.max(0.0, Math.min(1.0, (high - low) / range));
      }
    }
    return DEFAULT_RANGE_SELECTIVITY;
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Volumes/workplace/OpenSearch/index-insight && ./gradlew opensearch:test --tests "org.opensearch.sql.opensearch.storage.statistics.FieldStatisticTest" --no-build-cache 2>&1 | tail -5`
Expected: all tests PASS

- [ ] **Step 5: Commit**

```bash
cd /Volumes/workplace/OpenSearch/index-insight
git add opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/FieldStatistic.java \
        opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/FieldStatisticTest.java
git commit -s -m "feat: add FieldStatistic record for per-field Index Insight stats"
```

---

## Task 2: IndexInsightStatistic — Calcite Statistic implementation

**Files:**
- Create: `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/IndexInsightStatistic.java`
- Test: `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/IndexInsightStatisticTest.java`

Implements Calcite's `Statistic` interface. Provides `getRowCount()` and field-level statistics lookup. Parses the `content` JSON from IndexInsight STATISTICAL_DATA response.

- [ ] **Step 1: Write the failing test**

```java
package org.opensearch.sql.opensearch.storage.statistics;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.junit.jupiter.api.Test;

class IndexInsightStatisticTest {

  private static final String SAMPLE_CONTENT = """
      {
        "example_docs": [
          {"status": "200", "method": "GET", "latency": 42}
        ],
        "important_column_and_distribution": {
          "status": {
            "type": "keyword",
            "unique_terms": ["200", "301", "404", "500", "502"],
            "unique_count": 5
          },
          "method": {
            "type": "keyword",
            "unique_terms": ["GET", "POST", "PUT"],
            "unique_count": 3
          },
          "latency": {
            "type": "long",
            "unique_count": 8500,
            "min_value": 1,
            "max_value": 30000
          }
        }
      }
      """;

  @Test
  void parseContent_extractsFieldStats() {
    IndexInsightStatistic stat = IndexInsightStatistic.fromContentJson(SAMPLE_CONTENT, 100_000L);
    assertNotNull(stat.getFieldStatistic("status"));
    assertEquals(5L, stat.getFieldStatistic("status").cardinality());
    assertEquals(8500L, stat.getFieldStatistic("latency").cardinality());
  }

  @Test
  void getRowCount_returnsProvidedDocCount() {
    IndexInsightStatistic stat = IndexInsightStatistic.fromContentJson(SAMPLE_CONTENT, 100_000L);
    assertEquals(100_000.0, stat.getRowCount());
  }

  @Test
  void getFieldStatistic_unknownField_returnsNull() {
    IndexInsightStatistic stat = IndexInsightStatistic.fromContentJson(SAMPLE_CONTENT, 100_000L);
    assertNull(stat.getFieldStatistic("nonexistent"));
  }

  @Test
  void fromContentJson_nullContent_returnsEmpty() {
    IndexInsightStatistic stat = IndexInsightStatistic.fromContentJson(null, 10_000L);
    assertEquals(10_000.0, stat.getRowCount());
    assertTrue(stat.getFieldStatistics().isEmpty());
  }

  @Test
  void fromContentJson_malformedJson_returnsEmpty() {
    IndexInsightStatistic stat = IndexInsightStatistic.fromContentJson("not json", 10_000L);
    assertEquals(10_000.0, stat.getRowCount());
    assertTrue(stat.getFieldStatistics().isEmpty());
  }

  @Test
  void implementsCalciteStatistic() {
    IndexInsightStatistic stat = IndexInsightStatistic.fromContentJson(SAMPLE_CONTENT, 100_000L);
    // Must implement Calcite Statistic
    assertTrue(stat instanceof Statistic);
    assertEquals(100_000.0, stat.getRowCount());
    // Default Statistic methods should still work
    assertNotNull(stat.getKeys());
    assertNotNull(stat.getCollations());
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Volumes/workplace/OpenSearch/index-insight && ./gradlew opensearch:test --tests "org.opensearch.sql.opensearch.storage.statistics.IndexInsightStatisticTest" --no-build-cache 2>&1 | tail -5`
Expected: compilation failure — `IndexInsightStatistic` does not exist

- [ ] **Step 3: Implement IndexInsightStatistic**

```java
package org.opensearch.sql.opensearch.storage.statistics;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IndexInsightStatistic implements Statistic {

  private static final Logger LOG = LogManager.getLogger(IndexInsightStatistic.class);
  private static final String IMPORTANT_COLUMN_KEY = "important_column_and_distribution";
  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {}.getType();

  @Getter
  private final double rowCount;

  @Getter
  private final Map<String, FieldStatistic> fieldStatistics;

  private IndexInsightStatistic(double rowCount, Map<String, FieldStatistic> fieldStatistics) {
    this.rowCount = rowCount;
    this.fieldStatistics = Collections.unmodifiableMap(fieldStatistics);
  }

  @SuppressWarnings("unchecked")
  public static IndexInsightStatistic fromContentJson(String contentJson, long docCount) {
    Map<String, FieldStatistic> fieldStats = new HashMap<>();
    if (contentJson == null || contentJson.isBlank()) {
      return new IndexInsightStatistic(docCount, fieldStats);
    }
    try {
      Map<String, Object> content = GSON.fromJson(contentJson, MAP_TYPE);
      if (content == null || !content.containsKey(IMPORTANT_COLUMN_KEY)) {
        return new IndexInsightStatistic(docCount, fieldStats);
      }
      Map<String, Object> columns = (Map<String, Object>) content.get(IMPORTANT_COLUMN_KEY);
      for (Map.Entry<String, Object> entry : columns.entrySet()) {
        Map<String, Object> fieldData = (Map<String, Object>) entry.getValue();
        fieldStats.put(entry.getKey(), FieldStatistic.fromInsightMap(fieldData));
      }
    } catch (Exception e) {
      LOG.warn("Failed to parse Index Insight content, falling back to empty stats", e);
    }
    return new IndexInsightStatistic(docCount, fieldStats);
  }

  public FieldStatistic getFieldStatistic(String fieldName) {
    return fieldStatistics.get(fieldName);
  }

  @Override
  public Double getRowCount() {
    return rowCount;
  }

  @Override
  public List<ImmutableBitSet> getKeys() {
    return ImmutableList.of();
  }

  @Override
  public List<RelCollation> getCollations() {
    return ImmutableList.of();
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Volumes/workplace/OpenSearch/index-insight && ./gradlew opensearch:test --tests "org.opensearch.sql.opensearch.storage.statistics.IndexInsightStatisticTest" --no-build-cache 2>&1 | tail -5`
Expected: all tests PASS

- [ ] **Step 5: Commit**

```bash
cd /Volumes/workplace/OpenSearch/index-insight
git add opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/IndexInsightStatistic.java \
        opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/IndexInsightStatisticTest.java
git commit -s -m "feat: add IndexInsightStatistic implementing Calcite Statistic interface"
```

---

## Task 3: IndexInsightStatisticProvider — Fetch & cache stats from ml-commons

**Files:**
- Create: `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/IndexInsightStatisticProvider.java`
- Test: `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/IndexInsightStatisticProviderTest.java`

Fetches Index Insight STATISTICAL_DATA via `MLIndexInsightGetAction` transport action, caches the result, and returns `IndexInsightStatistic`. Uses `NodeClient` from `OpenSearchClient.getNodeClient()`.

- [ ] **Step 1: Write the failing test**

```java
package org.opensearch.sql.opensearch.storage.statistics;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.common.indexInsight.IndexInsight;
import org.opensearch.ml.common.indexInsight.IndexInsightTaskStatus;
import org.opensearch.ml.common.indexInsight.MLIndexInsightType;
import org.opensearch.ml.common.transport.indexInsight.MLIndexInsightGetAction;
import org.opensearch.ml.common.transport.indexInsight.MLIndexInsightGetRequest;
import org.opensearch.ml.common.transport.indexInsight.MLIndexInsightGetResponse;
import org.opensearch.transport.client.node.NodeClient;

@ExtendWith(MockitoExtension.class)
class IndexInsightStatisticProviderTest {

  @Mock private NodeClient nodeClient;

  private IndexInsightStatisticProvider provider;

  private static final String SAMPLE_CONTENT = """
      {
        "important_column_and_distribution": {
          "status": {"type": "keyword", "unique_count": 5}
        }
      }
      """;

  @BeforeEach
  void setUp() {
    provider = new IndexInsightStatisticProvider(nodeClient);
  }

  @Test
  void getStatistic_success_returnsStatWithFieldStats() {
    IndexInsight insight = IndexInsight.builder()
        .index("test-index")
        .content(SAMPLE_CONTENT)
        .status(IndexInsightTaskStatus.COMPLETED)
        .taskType(MLIndexInsightType.STATISTICAL_DATA)
        .lastUpdatedTime(Instant.now())
        .build();
    MLIndexInsightGetResponse response = MLIndexInsightGetResponse.builder()
        .indexInsight(insight).build();

    mockTransportSuccess(response);

    IndexInsightStatistic result = provider.getStatistic("test-index", 50_000L);
    assertNotNull(result);
    assertEquals(50_000.0, result.getRowCount());
    assertNotNull(result.getFieldStatistic("status"));
    assertEquals(5L, result.getFieldStatistic("status").cardinality());
  }

  @Test
  void getStatistic_transportFailure_returnsNull() {
    mockTransportFailure(new RuntimeException("Index insight not available"));

    IndexInsightStatistic result = provider.getStatistic("test-index", 10_000L);
    assertNull(result);
  }

  @Test
  void getStatistic_cachedResult_doesNotCallTransportAgain() {
    IndexInsight insight = IndexInsight.builder()
        .index("test-index")
        .content(SAMPLE_CONTENT)
        .status(IndexInsightTaskStatus.COMPLETED)
        .taskType(MLIndexInsightType.STATISTICAL_DATA)
        .lastUpdatedTime(Instant.now())
        .build();
    MLIndexInsightGetResponse response = MLIndexInsightGetResponse.builder()
        .indexInsight(insight).build();

    mockTransportSuccess(response);

    provider.getStatistic("test-index", 50_000L);
    provider.getStatistic("test-index", 50_000L);

    // Transport action called only once
    verify(nodeClient, times(1)).execute(eq(MLIndexInsightGetAction.INSTANCE), any(), any());
  }

  @Test
  void getStatistic_sendsCorrectRequest() {
    mockTransportFailure(new RuntimeException("test"));

    provider.getStatistic("my-logs-2024", 100L);

    ArgumentCaptor<MLIndexInsightGetRequest> captor =
        ArgumentCaptor.forClass(MLIndexInsightGetRequest.class);
    verify(nodeClient).execute(eq(MLIndexInsightGetAction.INSTANCE), captor.capture(), any());

    MLIndexInsightGetRequest request = captor.getValue();
    assertEquals("my-logs-2024", request.getIndexName());
    assertEquals(MLIndexInsightType.STATISTICAL_DATA, request.getTargetIndexInsight());
  }

  @SuppressWarnings("unchecked")
  private void mockTransportSuccess(MLIndexInsightGetResponse response) {
    doAnswer(invocation -> {
      ActionListener<MLIndexInsightGetResponse> listener = invocation.getArgument(2);
      listener.onResponse(response);
      return null;
    }).when(nodeClient).execute(eq(MLIndexInsightGetAction.INSTANCE), any(), any());
  }

  @SuppressWarnings("unchecked")
  private void mockTransportFailure(Exception e) {
    doAnswer(invocation -> {
      ActionListener<MLIndexInsightGetResponse> listener = invocation.getArgument(2);
      listener.onFailure(e);
      return null;
    }).when(nodeClient).execute(eq(MLIndexInsightGetAction.INSTANCE), any(), any());
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Volumes/workplace/OpenSearch/index-insight && ./gradlew opensearch:test --tests "org.opensearch.sql.opensearch.storage.statistics.IndexInsightStatisticProviderTest" --no-build-cache 2>&1 | tail -5`
Expected: compilation failure — `IndexInsightStatisticProvider` does not exist

- [ ] **Step 3: Implement IndexInsightStatisticProvider**

```java
package org.opensearch.sql.opensearch.storage.statistics;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.common.indexInsight.MLIndexInsightType;
import org.opensearch.ml.common.transport.indexInsight.MLIndexInsightGetAction;
import org.opensearch.ml.common.transport.indexInsight.MLIndexInsightGetRequest;
import org.opensearch.ml.common.transport.indexInsight.MLIndexInsightGetResponse;
import org.opensearch.transport.client.node.NodeClient;

public class IndexInsightStatisticProvider {

  private static final Logger LOG = LogManager.getLogger(IndexInsightStatisticProvider.class);
  private static final long FETCH_TIMEOUT_SECONDS = 5;

  private final NodeClient nodeClient;
  private final ConcurrentHashMap<String, IndexInsightStatistic> cache = new ConcurrentHashMap<>();

  public IndexInsightStatisticProvider(NodeClient nodeClient) {
    this.nodeClient = nodeClient;
  }

  /**
   * Get statistic for the given index. Returns cached result if available.
   * Returns null if Index Insight is unavailable or fetch fails.
   *
   * @param indexName the OpenSearch index name
   * @param docCount estimated document count (from _stats or maxResultWindow)
   * @return IndexInsightStatistic or null if unavailable
   */
  public IndexInsightStatistic getStatistic(String indexName, long docCount) {
    return cache.computeIfAbsent(indexName, key -> fetchStatistic(key, docCount));
  }

  private IndexInsightStatistic fetchStatistic(String indexName, long docCount) {
    MLIndexInsightGetRequest request = new MLIndexInsightGetRequest(
        indexName, MLIndexInsightType.STATISTICAL_DATA, null);

    AtomicReference<IndexInsightStatistic> resultRef = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);

    nodeClient.execute(MLIndexInsightGetAction.INSTANCE, request,
        new ActionListener<MLIndexInsightGetResponse>() {
          @Override
          public void onResponse(MLIndexInsightGetResponse response) {
            try {
              String content = response.getIndexInsight().getContent();
              resultRef.set(IndexInsightStatistic.fromContentJson(content, docCount));
            } catch (Exception e) {
              LOG.warn("Failed to parse Index Insight response for {}", indexName, e);
            } finally {
              latch.countDown();
            }
          }

          @Override
          public void onFailure(Exception e) {
            LOG.debug("Index Insight unavailable for {}: {}", indexName, e.getMessage());
            latch.countDown();
          }
        });

    try {
      if (!latch.await(FETCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        LOG.warn("Index Insight fetch timed out for {}", indexName);
        return null;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    }
    return resultRef.get();
  }

  public void invalidate(String indexName) {
    cache.remove(indexName);
  }

  public void invalidateAll() {
    cache.clear();
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Volumes/workplace/OpenSearch/index-insight && ./gradlew opensearch:test --tests "org.opensearch.sql.opensearch.storage.statistics.IndexInsightStatisticProviderTest" --no-build-cache 2>&1 | tail -5`
Expected: all tests PASS

- [ ] **Step 5: Commit**

```bash
cd /Volumes/workplace/OpenSearch/index-insight
git add opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/IndexInsightStatisticProvider.java \
        opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/IndexInsightStatisticProviderTest.java
git commit -s -m "feat: add IndexInsightStatisticProvider to fetch and cache stats from ml-commons"
```

---

## Task 4: Feature flag — Add INDEX_INSIGHT_STATISTICS_ENABLED setting

**Files:**
- Modify: `common/src/main/java/org/opensearch/sql/common/setting/Settings.java`
- Modify: `opensearch/src/main/java/org/opensearch/sql/opensearch/setting/OpenSearchSettings.java`

Add a cluster setting `plugins.calcite.index_insight_statistics.enabled` (default `false`) so the feature is opt-in initially.

- [ ] **Step 1: Add setting key to Settings.java**

In `common/src/main/java/org/opensearch/sql/common/setting/Settings.java`, add to the `Key` enum after `CALCITE_SUPPORT_ALL_JOIN_TYPES`:

```java
INDEX_INSIGHT_STATISTICS_ENABLED("plugins.calcite.index_insight_statistics.enabled"),
```

- [ ] **Step 2: Register the setting in OpenSearchSettings.java**

In `opensearch/src/main/java/org/opensearch/sql/opensearch/setting/OpenSearchSettings.java`:

Add the setting declaration alongside other Calcite settings:

```java
public static final Setting<Boolean> INDEX_INSIGHT_STATISTICS_ENABLED_SETTING =
    Setting.boolSetting(
        Key.INDEX_INSIGHT_STATISTICS_ENABLED.getKeyValue(),
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic);
```

Register it in `pluginSettings()` map:

```java
register(
    settingBuilder,
    pluginSettings,
    Key.INDEX_INSIGHT_STATISTICS_ENABLED,
    INDEX_INSIGHT_STATISTICS_ENABLED_SETTING,
    new Updater(Key.INDEX_INSIGHT_STATISTICS_ENABLED));
```

Add it to `pluginOpenSearchSettings()` list:

```java
INDEX_INSIGHT_STATISTICS_ENABLED_SETTING,
```

- [ ] **Step 3: Verify compilation**

Run: `cd /Volumes/workplace/OpenSearch/index-insight && ./gradlew opensearch:compileJava --no-build-cache 2>&1 | tail -5`
Expected: BUILD SUCCESSFUL

- [ ] **Step 4: Commit**

```bash
cd /Volumes/workplace/OpenSearch/index-insight
git add common/src/main/java/org/opensearch/sql/common/setting/Settings.java \
        opensearch/src/main/java/org/opensearch/sql/opensearch/setting/OpenSearchSettings.java
git commit -s -m "feat: add plugins.calcite.index_insight_statistics.enabled setting (default false)"
```

---

## Task 5: Wire IndexInsightStatistic into OpenSearchIndex

**Files:**
- Modify: `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/OpenSearchIndex.java`
- Modify: `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/OpenSearchIndexTest.java`

Override `getStatistic()` in `OpenSearchIndex` to return `IndexInsightStatistic` when the feature flag is enabled and Index Insight data is available. Falls back to `Statistics.UNKNOWN` otherwise.

- [ ] **Step 1: Write the failing test**

Add to `OpenSearchIndexTest.java`:

```java
@Test
void getStatistic_whenInsightEnabled_returnsIndexInsightStatistic() {
  when(settings.getSettingValue(Key.INDEX_INSIGHT_STATISTICS_ENABLED)).thenReturn(true);
  when(client.getNodeClient()).thenReturn(Optional.of(nodeClient));

  // Mock the transport action call to return insight data
  String content = """
      {"important_column_and_distribution": {"status": {"type": "keyword", "unique_count": 5}}}
      """;
  IndexInsight insight = IndexInsight.builder()
      .index("test")
      .content(content)
      .status(IndexInsightTaskStatus.COMPLETED)
      .taskType(MLIndexInsightType.STATISTICAL_DATA)
      .lastUpdatedTime(Instant.now())
      .build();
  MLIndexInsightGetResponse response = MLIndexInsightGetResponse.builder()
      .indexInsight(insight).build();
  mockMLTransportSuccess(response);

  OpenSearchIndex index = new OpenSearchIndex(client, settings, "test");
  Statistic stat = index.getStatistic();
  assertTrue(stat instanceof IndexInsightStatistic);
}

@Test
void getStatistic_whenInsightDisabled_returnsUnknown() {
  when(settings.getSettingValue(Key.INDEX_INSIGHT_STATISTICS_ENABLED)).thenReturn(false);

  OpenSearchIndex index = new OpenSearchIndex(client, settings, "test");
  Statistic stat = index.getStatistic();
  assertEquals(Statistics.UNKNOWN, stat);
}

@Test
void getStatistic_whenNodeClientAbsent_returnsUnknown() {
  when(settings.getSettingValue(Key.INDEX_INSIGHT_STATISTICS_ENABLED)).thenReturn(true);
  when(client.getNodeClient()).thenReturn(Optional.empty());

  OpenSearchIndex index = new OpenSearchIndex(client, settings, "test");
  Statistic stat = index.getStatistic();
  assertEquals(Statistics.UNKNOWN, stat);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Volumes/workplace/OpenSearch/index-insight && ./gradlew opensearch:test --tests "org.opensearch.sql.opensearch.storage.OpenSearchIndexTest.getStatistic*" --no-build-cache 2>&1 | tail -5`
Expected: FAIL — `getStatistic()` not overridden yet

- [ ] **Step 3: Implement getStatistic() in OpenSearchIndex**

Add a new field and override to `OpenSearchIndex.java`:

```java
// Add field after cachedMaxResultWindow
private Statistic cachedStatistic = null;

@Override
public Statistic getStatistic() {
  if (cachedStatistic != null) {
    return cachedStatistic;
  }
  if (!Boolean.TRUE.equals(settings.getSettingValue(Key.INDEX_INSIGHT_STATISTICS_ENABLED))) {
    return Statistics.UNKNOWN;
  }
  Optional<NodeClient> nc = client.getNodeClient();
  if (nc.isEmpty()) {
    return Statistics.UNKNOWN;
  }
  IndexInsightStatisticProvider provider = new IndexInsightStatisticProvider(nc.get());
  long docCount = getMaxResultWindow().longValue();
  IndexInsightStatistic stat = provider.getStatistic(
      indexName.getIndexNames()[0], docCount);
  if (stat != null) {
    cachedStatistic = stat;
    return stat;
  }
  return Statistics.UNKNOWN;
}
```

Add imports:

```java
import java.util.Optional;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.opensearch.storage.statistics.IndexInsightStatistic;
import org.opensearch.sql.opensearch.storage.statistics.IndexInsightStatisticProvider;
import org.opensearch.transport.client.node.NodeClient;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Volumes/workplace/OpenSearch/index-insight && ./gradlew opensearch:test --tests "org.opensearch.sql.opensearch.storage.OpenSearchIndexTest.getStatistic*" --no-build-cache 2>&1 | tail -5`
Expected: all tests PASS

- [ ] **Step 5: Commit**

```bash
cd /Volumes/workplace/OpenSearch/index-insight
git add opensearch/src/main/java/org/opensearch/sql/opensearch/storage/OpenSearchIndex.java \
        opensearch/src/test/java/org/opensearch/sql/opensearch/storage/OpenSearchIndexTest.java
git commit -s -m "feat: wire IndexInsightStatistic into OpenSearchIndex.getStatistic()"
```

---

## Task 6: Enhance cost model — Use real statistics in AbstractCalciteIndexScan

**Files:**
- Modify: `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/scan/AbstractCalciteIndexScan.java`
- Modify: `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/scan/CalciteIndexScanCostTest.java`

Update `estimateRowCount()` and `computeSelfCost()` to use `IndexInsightStatistic.getRowCount()` as baseline when available, instead of `maxResultWindow`. This is the minimal high-value change — the row count baseline affects all downstream cost calculations.

- [ ] **Step 1: Write failing tests for stats-aware cost computation**

Add to `CalciteIndexScanCostTest.java`:

```java
@Test
void test_cost_with_insight_statistic_baseline() {
  // Set up IndexInsightStatistic with 500,000 row count
  IndexInsightStatistic insightStat = IndexInsightStatistic.fromContentJson(
      "{\"important_column_and_distribution\": {}}", 500_000L);
  when(osIndex.getStatistic()).thenReturn(insightStat);

  RelDataType relDataType = mock(RelDataType.class);
  lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
  lenient().when(table.getRowType()).thenReturn(relDataType);
  CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);

  // Cost should use 500,000 as baseline instead of 10,000 (maxResultWindow)
  // non-pushdown cost = rows * fields * factor = 500,000 * 10 * 0.9 = 4,500,000
  assertEquals(4_500_000, scan.computeSelfCost(planner, mq).getRows());
}

@Test
void test_estimateRowCount_with_insight_statistic_baseline() {
  IndexInsightStatistic insightStat = IndexInsightStatistic.fromContentJson(
      "{\"important_column_and_distribution\": {}}", 500_000L);
  when(osIndex.getStatistic()).thenReturn(insightStat);

  RelDataType relDataType = mock(RelDataType.class);
  lenient().when(table.getRowType()).thenReturn(relDataType);
  CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);

  assertEquals(500_000.0, scan.estimateRowCount(mq));
}

@Test
void test_cost_fallback_when_no_insight_statistic() {
  // getStatistic returns default Statistics.UNKNOWN (not IndexInsightStatistic)
  when(osIndex.getStatistic()).thenReturn(Statistics.UNKNOWN);

  RelDataType relDataType = mock(RelDataType.class);
  lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
  lenient().when(table.getRowType()).thenReturn(relDataType);
  CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);

  // Falls back to maxResultWindow = 10,000
  assertEquals(90_000, scan.computeSelfCost(planner, mq).getRows());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Volumes/workplace/OpenSearch/index-insight && ./gradlew opensearch:test --tests "org.opensearch.sql.opensearch.storage.scan.CalciteIndexScanCostTest.test_cost_with_insight*" --tests "org.opensearch.sql.opensearch.storage.scan.CalciteIndexScanCostTest.test_estimateRowCount_with*" --tests "org.opensearch.sql.opensearch.storage.scan.CalciteIndexScanCostTest.test_cost_fallback*" --no-build-cache 2>&1 | tail -10`
Expected: FAIL

- [ ] **Step 3: Add helper method to get baseline row count**

In `AbstractCalciteIndexScan.java`, add a helper method:

```java
/**
 * Get the baseline row count for cost estimation.
 * Uses IndexInsightStatistic.getRowCount() when available (from Index Insight),
 * otherwise falls back to maxResultWindow.
 */
private double getBaselineRowCount() {
  Statistic stat = osIndex.getStatistic();
  if (stat instanceof IndexInsightStatistic insightStat) {
    return insightStat.getRowCount();
  }
  return osIndex.getMaxResultWindow().doubleValue();
}
```

Add imports:

```java
import org.apache.calcite.schema.Statistic;
import org.opensearch.sql.opensearch.storage.statistics.IndexInsightStatistic;
```

- [ ] **Step 4: Replace hardcoded baseline in estimateRowCount()**

Change line 130 from:

```java
osIndex.getMaxResultWindow().doubleValue(),
```

to:

```java
getBaselineRowCount(),
```

- [ ] **Step 5: Replace hardcoded baseline in computeSelfCost()**

Change line 173 from:

```java
double dRows = osIndex.getMaxResultWindow().doubleValue(), dCpu = 0.0d;
```

to:

```java
double dRows = getBaselineRowCount(), dCpu = 0.0d;
```

- [ ] **Step 6: Run all cost tests to verify correctness**

Run: `cd /Volumes/workplace/OpenSearch/index-insight && ./gradlew opensearch:test --tests "org.opensearch.sql.opensearch.storage.scan.CalciteIndexScanCostTest" --no-build-cache 2>&1 | tail -10`
Expected: all tests PASS (both new and existing — existing tests mock `getStatistic()` to return `Statistics.UNKNOWN` by default via Mockito, so they hit the fallback path)

- [ ] **Step 7: Commit**

```bash
cd /Volumes/workplace/OpenSearch/index-insight
git add opensearch/src/main/java/org/opensearch/sql/opensearch/storage/scan/AbstractCalciteIndexScan.java \
        opensearch/src/test/java/org/opensearch/sql/opensearch/storage/scan/CalciteIndexScanCostTest.java
git commit -s -m "feat: use Index Insight row count as baseline for cost estimation when available"
```

---

## Task 7: Integration test — End-to-end PPL with Index Insight statistics

**Files:**
- Create: `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/IndexInsightStatisticIntegrationTest.java`

Verifies that when IndexInsightStatistic is present, the cost model produces different (more accurate) costs than the default. This is a unit-level integration test using mocks — not a full cluster IT.

- [ ] **Step 1: Write the integration test**

```java
package org.opensearch.sql.opensearch.storage.statistics;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;
import org.opensearch.sql.opensearch.storage.scan.context.FilterDigest;
import org.opensearch.sql.opensearch.storage.scan.context.OSRequestBuilderAction;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownOperation;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownType;
import java.util.AbstractList;
import java.util.List;

@ExtendWith(MockitoExtension.class)
class IndexInsightStatisticIntegrationTest {

  private static final SqlTypeFactoryImpl TYPE_FACTORY =
      new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  private final RexBuilder rexBuilder = new RexBuilder(TYPE_FACTORY);

  @Mock private RelOptCluster cluster;
  @Mock private RelOptTable table;
  @Mock private OpenSearchIndex osIndex;
  @Mock private RelOptPlanner planner;
  @Mock private RelMetadataQuery mq;

  @BeforeEach
  void setUp() {
    RelTraitSet traitSet = mock(RelTraitSet.class);
    when(cluster.traitSetOf(any(Convention.class))).thenReturn(traitSet);
    when(osIndex.getMaxResultWindow()).thenReturn(10_000);
    Settings settings = mock(Settings.class);
    when(settings.getSettingValue(Key.CALCITE_PUSHDOWN_ROWCOUNT_ESTIMATION_FACTOR)).thenReturn(0.9);
    when(osIndex.getSettings()).thenReturn(settings);

    RelOptCostFactory costFactory = mock(RelOptCostFactory.class);
    when(planner.getCostFactory()).thenReturn(costFactory);
    when(costFactory.makeCost(anyDouble(), anyDouble(), anyDouble()))
        .thenAnswer(inv -> {
          RelOptCost cost = mock(RelOptCost.class);
          when(cost.getRows()).thenReturn((Double) inv.getArguments()[0]);
          return cost;
        });
  }

  @Test
  void costDiffers_withAndWithoutInsightStats() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(5));
    lenient().when(table.getRowType()).thenReturn(relDataType);

    // Without insight stats: baseline = maxResultWindow (10,000)
    when(osIndex.getStatistic()).thenReturn(Statistics.UNKNOWN);
    CalciteLogicalIndexScan scanWithout = new CalciteLogicalIndexScan(cluster, table, osIndex);
    double costWithout = scanWithout.computeSelfCost(planner, mq).getRows();

    // With insight stats: baseline = 1,000,000
    IndexInsightStatistic insightStat = IndexInsightStatistic.fromContentJson(
        "{\"important_column_and_distribution\":{}}", 1_000_000L);
    when(osIndex.getStatistic()).thenReturn(insightStat);
    CalciteLogicalIndexScan scanWith = new CalciteLogicalIndexScan(cluster, table, osIndex);
    double costWith = scanWith.computeSelfCost(planner, mq).getRows();

    // Cost with real stats should be 100x larger (1M vs 10K baseline)
    assertTrue(costWith > costWithout * 50,
        "Cost with insight stats (%s) should be much larger than without (%s)"
            .formatted(costWith, costWithout));
  }

  @Test
  void rowCountEstimate_moreAccurate_withInsightStats() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(table.getRowType()).thenReturn(relDataType);

    // With 2M real doc count
    IndexInsightStatistic insightStat = IndexInsightStatistic.fromContentJson(
        "{\"important_column_and_distribution\":{}}", 2_000_000L);
    when(osIndex.getStatistic()).thenReturn(insightStat);
    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);

    assertEquals(2_000_000.0, scan.estimateRowCount(mq));
  }

  static class MockFieldList extends AbstractList<RelDataTypeField> {
    private final int size;
    MockFieldList(int size) { this.size = size; }
    @Override public RelDataTypeField get(int index) { return mock(RelDataTypeField.class); }
    @Override public int size() { return size; }
  }
}
```

- [ ] **Step 2: Run the integration test**

Run: `cd /Volumes/workplace/OpenSearch/index-insight && ./gradlew opensearch:test --tests "org.opensearch.sql.opensearch.storage.statistics.IndexInsightStatisticIntegrationTest" --no-build-cache 2>&1 | tail -10`
Expected: all tests PASS

- [ ] **Step 3: Run full opensearch module test suite to check for regressions**

Run: `cd /Volumes/workplace/OpenSearch/index-insight && ./gradlew opensearch:test --no-build-cache 2>&1 | tail -20`
Expected: all tests PASS

- [ ] **Step 4: Commit**

```bash
cd /Volumes/workplace/OpenSearch/index-insight
git add opensearch/src/test/java/org/opensearch/sql/opensearch/storage/statistics/IndexInsightStatisticIntegrationTest.java
git commit -s -m "test: add integration test for Index Insight statistics in cost model"
```

---

## Summary

| Task | What it does | Key decision |
|------|-------------|--------------|
| 1 | `FieldStatistic` record | Parses per-field data from Index Insight JSON; provides `equalitySelectivity()` and `rangeSelectivity()` helpers |
| 2 | `IndexInsightStatistic` | Implements Calcite `Statistic`; bridges Index Insight JSON → Calcite metadata |
| 3 | `IndexInsightStatisticProvider` | Fetches STATISTICAL_DATA via transport action; caches per-index; 5s timeout with null fallback |
| 4 | Feature flag | `plugins.calcite.index_insight_statistics.enabled` (default `false`); opt-in |
| 5 | `OpenSearchIndex.getStatistic()` | Returns `IndexInsightStatistic` when enabled + available, else `Statistics.UNKNOWN` |
| 6 | Cost model enhancement | Replaces `maxResultWindow` baseline with real row count; minimal change, maximal impact |
| 7 | Integration test | Verifies cost differs with real stats vs heuristic |

## Future Work (not in scope)

These are natural follow-ups after this foundation is validated:

1. **Per-field selectivity in FilterDigest** — Use `FieldStatistic.equalitySelectivity()` instead of `RelMdUtil.guessSelectivity()` in the FILTER cost case. Requires extracting field names from `RexNode` conditions.
2. **Aggregation cardinality estimation** — Use `FieldStatistic.cardinality()` for group-by fields to estimate agg output rows.
3. **Null-ratio awareness** — Factor null ratio into NOT NULL filter selectivity.
4. **Async prefetch** — Preload Index Insight stats during schema resolution rather than blocking on first cost computation.
5. **doc count from _stats API** — Replace `maxResultWindow` as the `docCount` fallback with actual `_cat/indices` doc count.
