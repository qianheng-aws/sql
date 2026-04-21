/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import static org.opensearch.search.aggregations.MultiBucketConsumerService.DEFAULT_MAX_BUCKETS;

import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.util.CompositeMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.calcite.plan.AbstractOpenSearchTable;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.monitor.OpenSearchMemoryHealthy;
import org.opensearch.sql.opensearch.monitor.OpenSearchResourceMonitor;
import org.opensearch.sql.opensearch.planner.physical.ADOperator;
import org.opensearch.sql.opensearch.planner.physical.MLCommonsOperator;
import org.opensearch.sql.opensearch.planner.physical.MLOperator;
import org.opensearch.sql.opensearch.planner.physical.OpenSearchEvalOperator;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.request.system.OpenSearchDescribeIndexRequest;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;
import org.opensearch.sql.opensearch.storage.scan.OpenSearchIndexScan;
import org.opensearch.sql.opensearch.storage.scan.OpenSearchIndexScanBuilder;
import org.opensearch.sql.opensearch.storage.statistics.TableStatistic;
import org.opensearch.sql.opensearch.storage.statistics.TableStatisticCollector;
import org.opensearch.sql.opensearch.storage.statistics.TableStatisticDistinctRowCountHandler;
import org.opensearch.sql.opensearch.storage.statistics.TableStatisticStorage;
import org.opensearch.sql.planner.DefaultImplementor;
import org.opensearch.sql.planner.logical.LogicalAD;
import org.opensearch.sql.planner.logical.LogicalEval;
import org.opensearch.sql.planner.logical.LogicalML;
import org.opensearch.sql.planner.logical.LogicalMLCommons;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.read.TableScanBuilder;
import org.opensearch.transport.client.node.NodeClient;

/** OpenSearch table (index) implementation. */
public class OpenSearchIndex extends AbstractOpenSearchTable {

  private static final Logger LOG = LogManager.getLogger(OpenSearchIndex.class);

  /** Synchronous budget for reading a stored statistic from the statistics index. */
  private static final long STORAGE_READ_TIMEOUT_MS = 500L;

  /** Time-to-live for stored statistics; older than this triggers an async refresh. */
  private static final Duration STATISTIC_TTL = Duration.ofHours(24);

  public static final String METADATA_FIELD_ID = "_id";
  public static final String METADATA_FIELD_INDEX = "_index";
  public static final String METADATA_FIELD_SCORE = "_score";
  public static final String METADATA_FIELD_MAXSCORE = "_maxscore";
  public static final String METADATA_FIELD_SORT = "_sort";

  public static final String METADATA_FIELD_ROUTING = "_routing";

  public static final java.util.Map<String, ExprType> METADATAFIELD_TYPE_MAP =
      new LinkedHashMap<>() {
        {
          put(METADATA_FIELD_ID, ExprCoreType.STRING);
          put(METADATA_FIELD_INDEX, ExprCoreType.STRING);
          put(METADATA_FIELD_SCORE, ExprCoreType.FLOAT);
          put(METADATA_FIELD_MAXSCORE, ExprCoreType.FLOAT);
          put(METADATA_FIELD_SORT, ExprCoreType.LONG);
          put(METADATA_FIELD_ROUTING, ExprCoreType.STRING);
        }
      };

  /** OpenSearch client connection. */
  @Getter private final OpenSearchClient client;

  @Getter private final Settings settings;

  /** {@link OpenSearchRequest.IndexName}. */
  @Getter private final OpenSearchRequest.IndexName indexName;

  /** The cached mapping of field and type in index. */
  private Map<String, OpenSearchDataType> cachedFieldOpenSearchTypes = null;

  /** The cached ExprType of fields. */
  private Map<String, ExprType> cachedFieldTypes = null;

  /** The cached mapping of alias type field to its original path. */
  private Map<String, String> aliasMapping = null;

  /** The cached max result window setting of index. */
  private Integer cachedMaxResultWindow = null;

  /** The cached Calcite Statistic for this index. */
  private Statistic cachedStatistic = null;

  @Nullable private final TableStatisticStorage statisticStorage;
  @Nullable private final TableStatisticCollector statisticCollector;

  /**
   * Primary constructor: accepts optional statistics services. Pass nulls to disable collection.
   */
  public OpenSearchIndex(
      OpenSearchClient client,
      Settings settings,
      String indexName,
      @Nullable TableStatisticStorage statisticStorage,
      @Nullable TableStatisticCollector statisticCollector) {
    this.client = client;
    this.settings = settings;
    this.indexName = new OpenSearchRequest.IndexName(indexName);
    this.statisticStorage = statisticStorage;
    this.statisticCollector = statisticCollector;
  }

  /** Backwards-compat: no statistics services wired. Used by callers that don't consume stats. */
  public OpenSearchIndex(OpenSearchClient client, Settings settings, String indexName) {
    this(client, settings, indexName, null, null);
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new CalciteLogicalIndexScan(cluster, relOptTable, this);
  }

  @Override
  public boolean exists() {
    return client.exists(indexName.toString());
  }

  @Override
  public void create(Map<String, ExprType> schema) {
    Map<String, Object> mappings = new HashMap<>();
    Map<String, Object> properties = new HashMap<>();
    mappings.put("properties", properties);

    for (Map.Entry<String, ExprType> colType : schema.entrySet()) {
      properties.put(colType.getKey(), colType.getValue().legacyTypeName().toLowerCase());
    }
    client.createIndex(indexName.toString(), mappings);
  }

  /*
   * TODO: Assume indexName doesn't have wildcard.
   *  Need to either handle field name conflicts
   *   or lazy evaluate when query engine pulls field type.
   */
  /**
   * Get simplified parsed mapping info. Unlike {@link #getFieldOpenSearchTypes()} it returns a
   * flattened map.
   *
   * @return A map between field names and matching `ExprCoreType`s.
   */
  @Override
  public Map<String, ExprType> getFieldTypes() {
    if (cachedFieldOpenSearchTypes == null) {
      cachedFieldOpenSearchTypes =
          new OpenSearchDescribeIndexRequest(client, indexName).getFieldTypes();
    }
    if (cachedFieldTypes == null) {
      cachedFieldTypes =
          OpenSearchDataType.traverseAndFlatten(cachedFieldOpenSearchTypes).entrySet().stream()
              .collect(
                  LinkedHashMap::new,
                  (map, item) -> map.put(item.getKey(), item.getValue().getExprType()),
                  Map::putAll);
    }
    return cachedFieldTypes;
  }

  @Override
  public Map<String, ExprType> getReservedFieldTypes() {
    return METADATAFIELD_TYPE_MAP;
  }

  // Return all field types including reserved fields
  public Map<String, ExprType> getAllFieldTypes() {
    return CompositeMap.of(getFieldTypes(), getReservedFieldTypes());
  }

  public Map<String, String> getAliasMapping() {
    if (cachedFieldOpenSearchTypes == null) {
      cachedFieldOpenSearchTypes =
          new OpenSearchDescribeIndexRequest(client, indexName).getFieldTypes();
    }
    if (aliasMapping == null) {
      aliasMapping =
          OpenSearchDataType.traverseAndFlatten(cachedFieldOpenSearchTypes).entrySet().stream()
              .filter(entry -> entry.getValue().getOriginalPath().isPresent())
              .collect(
                  Collectors.toUnmodifiableMap(
                      Entry::getKey, entry -> entry.getValue().getOriginalPath().get()));
    }
    return aliasMapping;
  }

  /**
   * Get parsed mapping info.
   *
   * @return A complete map between field names and their types.
   */
  public Map<String, OpenSearchDataType> getFieldOpenSearchTypes() {
    if (cachedFieldOpenSearchTypes == null) {
      cachedFieldOpenSearchTypes =
          new OpenSearchDescribeIndexRequest(client, indexName).getFieldTypes();
    }
    return cachedFieldOpenSearchTypes;
  }

  /** Get the max result window setting of the table. */
  public Integer getMaxResultWindow() {
    if (cachedMaxResultWindow == null) {
      cachedMaxResultWindow =
          new OpenSearchDescribeIndexRequest(client, indexName).getMaxResultWindow();
    }
    return cachedMaxResultWindow;
  }

  @Override
  public Statistic getStatistic() {
    // cachedStatistic memoizes the outcome (including Statistics.UNKNOWN) for the life of this
    // OpenSearchIndex instance. OpenSearchStorageEngine constructs a fresh instance per query,
    // so this cache is intentionally short-lived — it exists to keep Calcite's optimizer from
    // re-issuing the 500 ms storage read and fire-and-forget refresh on every rule application
    // within a single query.
    if (cachedStatistic != null) {
      return cachedStatistic;
    }
    if (!Boolean.TRUE.equals(settings.getSettingValue(Settings.Key.TABLE_STATISTICS_ENABLED))) {
      return cacheAndReturn(Statistics.UNKNOWN);
    }
    if (client.getNodeClient().isEmpty() || statisticStorage == null) {
      return cacheAndReturn(Statistics.UNKNOWN);
    }

    String rawIndexName = indexName.getIndexNames()[0];
    AtomicReference<Optional<TableStatistic>> ref = new AtomicReference<>(Optional.empty());
    CountDownLatch latch = new CountDownLatch(1);
    statisticStorage.get(
        rawIndexName,
        new ActionListener<Optional<TableStatistic>>() {
          @Override
          public void onResponse(Optional<TableStatistic> result) {
            ref.set(result);
            latch.countDown();
          }

          @Override
          public void onFailure(Exception e) {
            // TableStatisticStorage#get never calls onFailure, but defend anyway.
            latch.countDown();
          }
        });

    try {
      if (!latch.await(STORAGE_READ_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        LOG.debug("Timed out reading statistic for {}", rawIndexName);
        triggerRefreshIfNeeded();
        return cacheAndReturn(Statistics.UNKNOWN);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return cacheAndReturn(Statistics.UNKNOWN);
    }

    Optional<TableStatistic> opt = ref.get();
    if (opt.isEmpty()) {
      triggerRefreshIfNeeded();
      return cacheAndReturn(Statistics.UNKNOWN);
    }

    TableStatistic stat = opt.get();
    // Even stale stats are better than UNKNOWN — return them but schedule a refresh.
    if (stat.isStale(STATISTIC_TTL)) {
      triggerRefreshIfNeeded();
    }
    return cacheAndReturn(stat);
  }

  private Statistic cacheAndReturn(Statistic stat) {
    cachedStatistic = stat;
    return stat;
  }

  /**
   * Expose a Calcite {@link BuiltInMetadata.DistinctRowCount.Handler} when a {@link TableStatistic}
   * is available for this index. Calcite's {@code RelMdDistinctRowCount.getDistinctRowCount(
   * TableScan, ...)} unwraps this handler type from the scan's table and, when present, delegates
   * to it — which in turn lets {@code RelMdRowCount.getRowCount(Aggregate)} use real per-column
   * cardinalities instead of falling back to {@code inputRowCount / 10}.
   */
  @Override
  public <C> @Nullable C unwrap(Class<C> aClass) {
    if (aClass == BuiltInMetadata.DistinctRowCount.Handler.class) {
      Statistic stat = getStatistic();
      if (stat instanceof TableStatistic tableStat) {
        return aClass.cast(new TableStatisticDistinctRowCountHandler(tableStat));
      }
      return null;
    }
    return super.unwrap(aClass);
  }

  private void triggerRefreshIfNeeded() {
    if (statisticCollector == null) {
      return;
    }
    // NOTE: getFieldOpenSearchTypes() synchronously fetches mappings if not already cached.
    // In practice the mapping is populated during query analysis (before cost), so this is a
    // cheap field read. If some future path calls getStatistic() on an OpenSearchIndex whose
    // mapping hasn't been fetched yet, skip the refresh rather than pay a second blocking call.
    if (cachedFieldOpenSearchTypes == null) {
      LOG.debug("Skipping stat refresh for {}: field mapping not yet loaded", indexName);
      return;
    }
    try {
      statisticCollector.refreshAsync(indexName.getIndexNames()[0], cachedFieldOpenSearchTypes);
    } catch (RuntimeException e) {
      // refreshAsync is documented fire-and-forget; defend anyway.
      LOG.debug("Failed to trigger stat refresh for {}: {}", indexName, e.getMessage());
    }
  }

  public Integer getQueryBucketSize() {
    return Math.min(settings.getSettingValue(Settings.Key.QUERY_BUCKET_SIZE), getMaxBuckets());
  }

  public Integer getMaxBuckets() {
    try {
      return settings.getSettingValue(Settings.Key.SEARCH_MAX_BUCKETS);
    } catch (Exception e) {
      return DEFAULT_MAX_BUCKETS;
    }
  }

  /** TODO: Push down operations to index scan operator as much as possible in future. */
  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    // TODO: Leave it here to avoid impact Prometheus and AD operators. Need to move to Planner.
    return plan.accept(new OpenSearchDefaultImplementor(client), null);
  }

  @Override
  public TableScanBuilder createScanBuilder() {
    final TimeValue cursorKeepAlive = settings.getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE);
    var builder = createRequestBuilder();
    Function<OpenSearchRequestBuilder, OpenSearchIndexScan> createScanOperator =
        requestBuilder ->
            new OpenSearchIndexScan(
                client,
                requestBuilder.getMaxResponseSize(),
                requestBuilder.build(
                    indexName, cursorKeepAlive, client, cachedFieldOpenSearchTypes.isEmpty()));
    return new OpenSearchIndexScanBuilder(builder, createScanOperator);
  }

  private OpenSearchExprValueFactory createExprValueFactory() {
    Map<String, OpenSearchDataType> allFields = new HashMap<>();
    getReservedFieldTypes().forEach((k, v) -> allFields.put(k, OpenSearchDataType.of(v)));
    allFields.putAll(getFieldOpenSearchTypes());
    return new OpenSearchExprValueFactory(
        allFields, settings.getSettingValue(Settings.Key.FIELD_TYPE_TOLERANCE));
  }

  public boolean isFieldTypeTolerance() {
    return settings.getSettingValue(Settings.Key.FIELD_TYPE_TOLERANCE);
  }

  @VisibleForTesting
  @RequiredArgsConstructor
  public static class OpenSearchDefaultImplementor extends DefaultImplementor<OpenSearchIndexScan> {

    private final OpenSearchClient client;

    @Override
    public PhysicalPlan visitMLCommons(LogicalMLCommons node, OpenSearchIndexScan context) {
      Optional<NodeClient> nc = client.getNodeClient();
      if (nc.isEmpty()) {
        throw new UnsupportedOperationException(
            "Unable to run Machine Learning operators on clients outside of the local node");
      }
      return new MLCommonsOperator(
          visitChild(node, context), node.getAlgorithm(), node.getArguments(), nc.get());
    }

    @Override
    public PhysicalPlan visitAD(LogicalAD node, OpenSearchIndexScan context) {
      Optional<NodeClient> nc = client.getNodeClient();
      if (nc.isEmpty()) {
        throw new UnsupportedOperationException(
            "Unable to run Anomaly Detector operators on clients outside of the local node");
      }
      return new ADOperator(visitChild(node, context), node.getArguments(), nc.get());
    }

    @Override
    public PhysicalPlan visitML(LogicalML node, OpenSearchIndexScan context) {
      Optional<NodeClient> nc = client.getNodeClient();
      if (nc.isEmpty()) {
        throw new UnsupportedOperationException(
            "Unable to run Machine Learning operators on clients outside of the local node");
      }
      return new MLOperator(visitChild(node, context), node.getArguments(), nc.get());
    }

    @Override
    public PhysicalPlan visitEval(LogicalEval node, OpenSearchIndexScan context) {
      Optional<NodeClient> nc = client.getNodeClient();
      if (nc.isEmpty()) {
        throw new UnsupportedOperationException(
            "Unable to run Eval operators on clients outside of the local node");
      }
      return new OpenSearchEvalOperator(visitChild(node, context), node.getExpressions(), nc.get());
    }
  }

  public OpenSearchRequestBuilder createRequestBuilder() {
    return new OpenSearchRequestBuilder(createExprValueFactory(), getMaxResultWindow(), settings);
  }

  public OpenSearchResourceMonitor createOpenSearchResourceMonitor() {
    return new OpenSearchResourceMonitor(getSettings(), new OpenSearchMemoryHealthy(settings));
  }

  /** The v3 API to build an OpenSearchRequest, calling by CalciteEnumerableIndexScan */
  public OpenSearchRequest buildRequest(OpenSearchRequestBuilder requestBuilder) {
    final TimeValue cursorKeepAlive = settings.getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE);
    return requestBuilder.build(
        indexName, cursorKeepAlive, client, cachedFieldOpenSearchTypes.isEmpty());
  }
}
