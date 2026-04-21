/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.expression.DSL.named;
import static org.opensearch.sql.expression.DSL.ref;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.eval;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.project;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.remove;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.rename;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.sort;

import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.mapping.IndexMapping;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.storage.scan.OpenSearchIndexScan;
import org.opensearch.sql.opensearch.storage.statistics.TableStatistic;
import org.opensearch.sql.opensearch.storage.statistics.TableStatisticCollector;
import org.opensearch.sql.opensearch.storage.statistics.TableStatisticStorage;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanDSL;
import org.opensearch.sql.planner.physical.PhysicalPlanDSL;
import org.opensearch.transport.client.node.NodeClient;

@ExtendWith(MockitoExtension.class)
class OpenSearchIndexTest {

  public static final TimeValue SCROLL_TIMEOUT = new TimeValue(1);
  public static final OpenSearchRequest.IndexName INDEX_NAME =
      new OpenSearchRequest.IndexName("test");

  @Mock private OpenSearchClient client;

  @Mock private OpenSearchExprValueFactory exprValueFactory;

  @Mock private Settings settings;

  @Mock private IndexMapping mapping;

  @Mock private TableStatisticStorage statisticStorage;

  @Mock private TableStatisticCollector statisticCollector;

  private OpenSearchIndex index;

  @BeforeEach
  void setUp() {
    this.index = new OpenSearchIndex(client, settings, "test");
    lenient().when(settings.getSettingValue(Settings.Key.FIELD_TYPE_TOLERANCE)).thenReturn(true);
    lenient()
        .when(settings.getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE))
        .thenReturn(TimeValue.timeValueMinutes(1));
  }

  private OpenSearchIndex newIndexWithStats() {
    return new OpenSearchIndex(client, settings, "test", statisticStorage, statisticCollector);
  }

  @Test
  void isExist() {
    when(client.exists("test")).thenReturn(true);

    assertTrue(index.exists());
  }

  @Test
  void createIndex() {
    Map<String, Object> mappings =
        Map.of(
            "properties",
            Map.of(
                "name", "text",
                "age", "integer"));
    doNothing().when(client).createIndex("test", mappings);

    Map<String, ExprType> schema = new HashMap<>();
    schema.put(
        "name",
        OpenSearchTextType.of(Map.of("keyword", OpenSearchDataType.of(MappingType.Keyword))));
    schema.put("age", INTEGER);
    index.create(schema);
    verify(client).createIndex(any(), any());
  }

  @Test
  void getFieldTypes() {
    when(mapping.getFieldMappings())
        .thenReturn(
            ImmutableMap.<String, MappingType>builder()
                .put("name", MappingType.Keyword)
                .put("address", MappingType.Text)
                .put("age", MappingType.Integer)
                .put("account_number", MappingType.Long)
                .put("balance1", MappingType.Float)
                .put("balance2", MappingType.Double)
                .put("gender", MappingType.Boolean)
                .put("family", MappingType.Nested)
                .put("employer", MappingType.Object)
                .put("birthday", MappingType.Date)
                .put("id1", MappingType.Byte)
                .put("id2", MappingType.Short)
                .put("blob", MappingType.Binary)
                .build()
                .entrySet()
                .stream()
                .collect(
                    Collectors.toMap(Map.Entry::getKey, e -> OpenSearchDataType.of(e.getValue()))));
    when(client.getIndexMappings("test")).thenReturn(ImmutableMap.of("test", mapping));

    // Run more than once to confirm caching logic is covered and can work
    for (int i = 0; i < 2; i++) {
      Map<String, ExprType> fieldTypes = index.getFieldTypes();
      assertThat(
          fieldTypes,
          allOf(
              aMapWithSize(13),
              hasEntry("name", ExprCoreType.STRING),
              hasEntry("address", (ExprType) OpenSearchDataType.of(MappingType.Text)),
              hasEntry("age", ExprCoreType.INTEGER),
              hasEntry("account_number", ExprCoreType.LONG),
              hasEntry("balance1", ExprCoreType.FLOAT),
              hasEntry("balance2", ExprCoreType.DOUBLE),
              hasEntry("gender", ExprCoreType.BOOLEAN),
              hasEntry("family", ExprCoreType.ARRAY),
              hasEntry("employer", ExprCoreType.STRUCT),
              hasEntry("birthday", (ExprType) OpenSearchDataType.of(MappingType.Date)),
              hasEntry("id1", ExprCoreType.BYTE),
              hasEntry("id2", ExprCoreType.SHORT),
              hasEntry("blob", (ExprType) OpenSearchDataType.of(MappingType.Binary))));
    }
  }

  @Test
  void checkCacheUsedForFieldMappings() {
    when(mapping.getFieldMappings())
        .thenReturn(Map.of("name", OpenSearchDataType.of(MappingType.Keyword)));
    when(client.getIndexMappings("test")).thenReturn(ImmutableMap.of("test", mapping));

    OpenSearchIndex index = new OpenSearchIndex(client, settings, "test");
    assertThat(index.getFieldTypes(), allOf(aMapWithSize(1), hasEntry("name", STRING)));
    assertThat(
        index.getFieldOpenSearchTypes(),
        allOf(aMapWithSize(1), hasEntry("name", OpenSearchDataType.of(STRING))));

    lenient()
        .when(mapping.getFieldMappings())
        .thenReturn(Map.of("name", OpenSearchDataType.of(MappingType.Integer)));

    assertThat(index.getFieldTypes(), allOf(aMapWithSize(1), hasEntry("name", STRING)));
    assertThat(
        index.getFieldOpenSearchTypes(),
        allOf(aMapWithSize(1), hasEntry("name", OpenSearchDataType.of(STRING))));
  }

  @Test
  void getReservedFieldTypes() {
    Map<String, ExprType> fieldTypes = index.getReservedFieldTypes();
    assertThat(
        fieldTypes,
        allOf(
            aMapWithSize(6),
            hasEntry("_id", ExprCoreType.STRING),
            hasEntry("_index", ExprCoreType.STRING),
            hasEntry("_routing", ExprCoreType.STRING),
            hasEntry("_sort", ExprCoreType.LONG),
            hasEntry("_score", ExprCoreType.FLOAT),
            hasEntry("_maxscore", ExprCoreType.FLOAT)));
  }

  @Test
  void implementRelationOperatorOnly() {
    when(client.getIndexMaxResultWindows("test")).thenReturn(Map.of("test", 10000));
    LogicalPlan plan = index.createScanBuilder();
    Integer maxResultWindow = index.getMaxResultWindow();
    final var requestBuilder =
        new OpenSearchRequestBuilder(exprValueFactory, maxResultWindow, settings);
    assertEquals(
        new OpenSearchIndexScan(
            client, requestBuilder.build(INDEX_NAME, SCROLL_TIMEOUT, client, true)),
        index.implement(index.optimize(plan)));
  }

  @Test
  void implementRelationOperatorWithOptimization() {
    when(client.getIndexMaxResultWindows("test")).thenReturn(Map.of("test", 10000));
    LogicalPlan plan = index.createScanBuilder();
    Integer maxResultWindow = index.getMaxResultWindow();
    final var requestBuilder =
        new OpenSearchRequestBuilder(exprValueFactory, maxResultWindow, settings);
    assertEquals(
        new OpenSearchIndexScan(
            client, requestBuilder.build(INDEX_NAME, SCROLL_TIMEOUT, client, true)),
        index.implement(plan));
  }

  @Test
  void implementOtherLogicalOperators() {
    when(client.getIndexMaxResultWindows("test")).thenReturn(Map.of("test", 10000));
    when(client.getNodeClient()).thenReturn(Optional.of(Mockito.mock(NodeClient.class)));
    NamedExpression include = named("age", ref("age", INTEGER));
    ReferenceExpression exclude = ref("name", STRING);
    ReferenceExpression dedupeField = ref("name", STRING);
    Map<ReferenceExpression, ReferenceExpression> mappings =
        ImmutableMap.of(ref("name", STRING), ref("lastname", STRING));
    Pair<ReferenceExpression, Expression> newEvalField =
        ImmutablePair.of(ref("name1", STRING), ref("name", STRING));
    Pair<Sort.SortOption, Expression> sortField =
        ImmutablePair.of(Sort.SortOption.DEFAULT_ASC, ref("name1", STRING));

    LogicalPlan plan =
        project(
            LogicalPlanDSL.dedupe(
                sort(
                    eval(
                        remove(rename(index.createScanBuilder(), mappings), exclude), newEvalField),
                    sortField),
                dedupeField),
            include);

    Integer maxResultWindow = index.getMaxResultWindow();
    final var requestBuilder =
        new OpenSearchRequestBuilder(exprValueFactory, maxResultWindow, settings);
    assertEquals(
        PhysicalPlanDSL.project(
            PhysicalPlanDSL.dedupe(
                PhysicalPlanDSL.sort(
                    PhysicalPlanDSL.eval(
                        PhysicalPlanDSL.remove(
                            PhysicalPlanDSL.rename(
                                new OpenSearchIndexScan(
                                    client,
                                    requestBuilder.build(INDEX_NAME, SCROLL_TIMEOUT, client, true)),
                                mappings),
                            exclude),
                        newEvalField),
                    sortField),
                dedupeField),
            include),
        index.implement(plan));
  }

  @Test
  void isFieldTypeTolerance() {
    when(settings.getSettingValue(Settings.Key.FIELD_TYPE_TOLERANCE))
        .thenReturn(true)
        .thenReturn(false);
    assertTrue(index.isFieldTypeTolerance());
    assertFalse(index.isFieldTypeTolerance());
  }

  @Test
  void getStatistic_whenFlagOff_returnsUnknown() {
    when(settings.getSettingValue(Settings.Key.TABLE_STATISTICS_ENABLED)).thenReturn(false);
    OpenSearchIndex idx = newIndexWithStats();
    assertEquals(Statistics.UNKNOWN, idx.getStatistic());
    verify(statisticStorage, never()).get(any(), any());
  }

  @Test
  void getStatistic_whenNodeClientAbsent_returnsUnknown() {
    when(settings.getSettingValue(Settings.Key.TABLE_STATISTICS_ENABLED)).thenReturn(true);
    when(client.getNodeClient()).thenReturn(Optional.empty());
    OpenSearchIndex idx = newIndexWithStats();
    assertEquals(Statistics.UNKNOWN, idx.getStatistic());
    verify(statisticStorage, never()).get(any(), any());
  }

  @Test
  void getStatistic_whenStorageNull_returnsUnknown() {
    when(settings.getSettingValue(Settings.Key.TABLE_STATISTICS_ENABLED)).thenReturn(true);
    lenient().when(client.getNodeClient()).thenReturn(Optional.of(Mockito.mock(NodeClient.class)));
    // index constructed without storage / collector
    OpenSearchIndex idx = new OpenSearchIndex(client, settings, "test");
    assertEquals(Statistics.UNKNOWN, idx.getStatistic());
  }

  @Test
  void getStatistic_whenStorageHitFresh_returnsTableStatistic() {
    when(settings.getSettingValue(Settings.Key.TABLE_STATISTICS_ENABLED)).thenReturn(true);
    when(client.getNodeClient()).thenReturn(Optional.of(Mockito.mock(NodeClient.class)));

    TableStatistic fresh = TableStatistic.fromFields(1234L, Map.of());
    doAnswer(
            invocation -> {
              ActionListener<Optional<TableStatistic>> listener = invocation.getArgument(1);
              listener.onResponse(Optional.of(fresh));
              return null;
            })
        .when(statisticStorage)
        .get(eq("test"), any());

    OpenSearchIndex idx = newIndexWithStats();
    Statistic stat = idx.getStatistic();
    assertSame(fresh, stat);
    verify(statisticCollector, never()).refreshAsync(any(), any());
  }

  @Test
  void getStatistic_whenStorageHitStale_returnsStatAndTriggersRefresh() {
    when(settings.getSettingValue(Settings.Key.TABLE_STATISTICS_ENABLED)).thenReturn(true);
    when(client.getNodeClient()).thenReturn(Optional.of(Mockito.mock(NodeClient.class)));
    when(client.getIndexMappings("test")).thenReturn(ImmutableMap.of("test", mapping));
    when(mapping.getFieldMappings())
        .thenReturn(Map.of("age", OpenSearchDataType.of(MappingType.Integer)));

    Instant oneWeekAgo = Instant.now().minus(7, ChronoUnit.DAYS);
    Map<String, Object> staleDoc =
        Map.of(
            "status",
            "COMPLETED",
            "last_updated_time",
            oneWeekAgo.toString(),
            "doc_count",
            4200L,
            "fields",
            Map.of());
    TableStatistic stale = TableStatistic.fromStoredDoc(staleDoc);
    doAnswer(
            invocation -> {
              ActionListener<Optional<TableStatistic>> listener = invocation.getArgument(1);
              listener.onResponse(Optional.of(stale));
              return null;
            })
        .when(statisticStorage)
        .get(eq("test"), any());

    OpenSearchIndex idx = newIndexWithStats();
    // Prime the field-type cache so triggerRefreshIfNeeded() can fire refreshAsync.
    idx.getFieldOpenSearchTypes();
    Statistic stat = idx.getStatistic();
    assertSame(stale, stat);
    verify(statisticCollector, times(1)).refreshAsync(eq("test"), any());
  }

  @Test
  void getStatistic_whenStorageMiss_returnsUnknownAndTriggersRefresh() {
    when(settings.getSettingValue(Settings.Key.TABLE_STATISTICS_ENABLED)).thenReturn(true);
    when(client.getNodeClient()).thenReturn(Optional.of(Mockito.mock(NodeClient.class)));
    when(client.getIndexMappings("test")).thenReturn(ImmutableMap.of("test", mapping));
    when(mapping.getFieldMappings())
        .thenReturn(Map.of("age", OpenSearchDataType.of(MappingType.Integer)));

    doAnswer(
            invocation -> {
              ActionListener<Optional<TableStatistic>> listener = invocation.getArgument(1);
              listener.onResponse(Optional.empty());
              return null;
            })
        .when(statisticStorage)
        .get(eq("test"), any());

    OpenSearchIndex idx = newIndexWithStats();
    // Prime the field-type cache so triggerRefreshIfNeeded() can fire refreshAsync.
    idx.getFieldOpenSearchTypes();
    assertEquals(Statistics.UNKNOWN, idx.getStatistic());
    verify(statisticCollector, times(1)).refreshAsync(eq("test"), any());
  }

  @Test
  void getStatistic_whenStorageTimesOut_returnsUnknownAndTriggersRefresh() {
    // NOTE: This test intentionally waits the full STORAGE_READ_TIMEOUT_MS (500 ms wall time).
    // If that constant is ever raised, update this test to inject a shorter timeout rather
    // than silently inflating CI duration.
    when(settings.getSettingValue(Settings.Key.TABLE_STATISTICS_ENABLED)).thenReturn(true);
    when(client.getNodeClient()).thenReturn(Optional.of(Mockito.mock(NodeClient.class)));
    when(client.getIndexMappings("test")).thenReturn(ImmutableMap.of("test", mapping));
    when(mapping.getFieldMappings())
        .thenReturn(Map.of("age", OpenSearchDataType.of(MappingType.Integer)));

    // Simulate timeout — listener never invoked.
    doAnswer(invocation -> null).when(statisticStorage).get(eq("test"), any());

    OpenSearchIndex idx = newIndexWithStats();
    // Prime the field-type cache so triggerRefreshIfNeeded() can fire refreshAsync.
    idx.getFieldOpenSearchTypes();

    assertEquals(Statistics.UNKNOWN, idx.getStatistic());
    verify(statisticCollector, times(1)).refreshAsync(eq("test"), any());
  }

  @Test
  void getStatistic_cachedAcrossCalls() {
    when(settings.getSettingValue(Settings.Key.TABLE_STATISTICS_ENABLED)).thenReturn(true);
    when(client.getNodeClient()).thenReturn(Optional.of(Mockito.mock(NodeClient.class)));

    TableStatistic fresh = TableStatistic.fromFields(99L, Map.of());
    doAnswer(
            invocation -> {
              ActionListener<Optional<TableStatistic>> listener = invocation.getArgument(1);
              listener.onResponse(Optional.of(fresh));
              return null;
            })
        .when(statisticStorage)
        .get(eq("test"), any());

    OpenSearchIndex idx = newIndexWithStats();
    Statistic first = idx.getStatistic();
    Statistic second = idx.getStatistic();
    assertSame(fresh, first);
    assertSame(first, second);
    // Storage should only be hit once.
    verify(statisticStorage, times(1)).get(eq("test"), any());
  }

  @Test
  void getStatistic_missCachesUnknown_preventsRepeatedStorageHits() {
    // Regression: same-query repeated getStatistic() calls on a cold index should NOT
    // re-issue the 500ms storage read or repeatedly fire refreshAsync. Calcite can call
    // getStatistic() multiple times within a single optimization pass.
    when(settings.getSettingValue(Settings.Key.TABLE_STATISTICS_ENABLED)).thenReturn(true);
    when(client.getNodeClient()).thenReturn(Optional.of(Mockito.mock(NodeClient.class)));
    when(client.getIndexMappings("test")).thenReturn(ImmutableMap.of("test", mapping));
    when(mapping.getFieldMappings())
        .thenReturn(Map.of("age", OpenSearchDataType.of(MappingType.Integer)));

    doAnswer(
            invocation -> {
              ActionListener<Optional<TableStatistic>> listener = invocation.getArgument(1);
              listener.onResponse(Optional.empty());
              return null;
            })
        .when(statisticStorage)
        .get(eq("test"), any());

    OpenSearchIndex idx = newIndexWithStats();
    idx.getFieldOpenSearchTypes(); // prime mapping cache so refresh can fire

    assertEquals(Statistics.UNKNOWN, idx.getStatistic());
    assertEquals(Statistics.UNKNOWN, idx.getStatistic());
    assertEquals(Statistics.UNKNOWN, idx.getStatistic());

    // Despite 3 calls, storage hit and refresh trigger should only happen once.
    verify(statisticStorage, times(1)).get(eq("test"), any());
    verify(statisticCollector, times(1)).refreshAsync(eq("test"), any());
  }

  @Test
  void getStatistic_skipsRefreshWhenMappingNotLoaded() {
    // Defensive: if getStatistic() is ever invoked before the field-type cache is populated,
    // triggerRefreshIfNeeded() must skip rather than synchronously fetch the mapping (which
    // would double the blocking cost on the optimizer thread).
    when(settings.getSettingValue(Settings.Key.TABLE_STATISTICS_ENABLED)).thenReturn(true);
    when(client.getNodeClient()).thenReturn(Optional.of(Mockito.mock(NodeClient.class)));

    doAnswer(
            invocation -> {
              ActionListener<Optional<TableStatistic>> listener = invocation.getArgument(1);
              listener.onResponse(Optional.empty());
              return null;
            })
        .when(statisticStorage)
        .get(eq("test"), any());

    OpenSearchIndex idx = newIndexWithStats();
    // Intentionally do NOT call getFieldOpenSearchTypes().

    assertEquals(Statistics.UNKNOWN, idx.getStatistic());
    // Storage read still happened, but refresh should be skipped to avoid the mapping fetch.
    verify(statisticStorage, times(1)).get(eq("test"), any());
    verify(statisticCollector, never()).refreshAsync(any(), any());
  }
}
