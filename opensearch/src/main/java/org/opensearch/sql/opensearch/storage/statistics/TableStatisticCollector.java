/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.statistics;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.sampler.InternalSampler;
import org.opensearch.search.aggregations.bucket.sampler.SamplerAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalCardinality;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Orchestrates asynchronous table-statistic collection: builds an aggregation request covering the
 * whole flattened field schema, sends it to {@link NodeClient}, and persists the parsed {@link
 * TableStatistic} into {@link TableStatisticStorage}.
 *
 * <p>Aggregation shape:
 *
 * <ul>
 *   <li>Cardinality (HLL, approximate distinct count) is wrapped in a {@link
 *       SamplerAggregationBuilder sampler} so per-shard work is bounded by {@link
 *       #SAMPLER_SHARD_SIZE} documents.
 *   <li>Min / max are <b>top-level</b> aggregations (not under the sampler). Lucene's {@code
 *       MinAggregator} / {@code MaxAggregator} short-circuit to {@code
 *       PointValues.getMinPackedValue} / {@code getMaxPackedValue} on numeric / date fields, so
 *       they are O(1) per segment regardless of doc count — cheap enough to read exactly instead of
 *       approximating from a sample.
 * </ul>
 *
 * <p>The single public entrypoint {@link #refreshAsync(String, Map)} is fire-and-forget; internal
 * steps chain via {@link ActionListener}s. Race-protection is enforced by reading the raw stored
 * document first: if a {@code GENERATING} marker is fresh (< {@link #STALE_GENERATING}) the refresh
 * is skipped; otherwise a new marker is written, the aggregation is executed, and either a
 * completed record or a {@code FAILED} marker is written on finish.
 */
public class TableStatisticCollector {

  private static final Logger LOG = LogManager.getLogger(TableStatisticCollector.class);

  /** Sampler aggregation shard-size: cap documents seen per shard to bound memory. */
  private static final int SAMPLER_SHARD_SIZE = 100_000;

  /** If a GENERATING marker is older than this, treat it as abandoned and re-issue a collect. */
  private static final Duration STALE_GENERATING = Duration.ofMinutes(10);

  /** Aggregation name prefixes (package-private so tests can reference them). */
  static final String SAMPLER_AGG = "sample";

  static final String CARDINALITY_PREFIX = "cardinality_";
  static final String MIN_PREFIX = "min_";
  static final String MAX_PREFIX = "max_";
  static final String COUNT_PREFIX = "count_";

  /**
   * Field types for which an approximate-distinct-count (cardinality) aggregation is meaningful.
   * Text is handled separately (requires a {@code .keyword} sub-field).
   */
  private static final EnumSet<MappingType> CARDINALITY_TYPES =
      EnumSet.of(
          MappingType.Keyword,
          MappingType.Long,
          MappingType.Integer,
          MappingType.Short,
          MappingType.Byte);

  /** Field types for which numeric min/max is meaningful. */
  private static final EnumSet<MappingType> MIN_MAX_TYPES =
      EnumSet.of(
          MappingType.Long,
          MappingType.Integer,
          MappingType.Short,
          MappingType.Byte,
          MappingType.Float,
          MappingType.HalfFloat,
          MappingType.ScaledFloat,
          MappingType.Double,
          MappingType.Date,
          MappingType.DateNanos);

  private static final ActionListener<Void> NOOP_COMPLETION = ActionListener.wrap(v -> {}, e -> {});

  private final NodeClient nodeClient;
  private final TableStatisticStorage storage;

  public TableStatisticCollector(NodeClient nodeClient, TableStatisticStorage storage) {
    this.nodeClient = nodeClient;
    this.storage = storage;
  }

  /**
   * Asynchronously collect statistics for {@code indexName} with the given field schema and persist
   * the result. Never blocks, never throws — internal errors are logged and a {@code FAILED} marker
   * is written when appropriate. Safe to call from any thread.
   */
  public void refreshAsync(String indexName, Map<String, OpenSearchDataType> fieldTypes) {
    refreshAsync(indexName, fieldTypes, NOOP_COMPLETION);
  }

  /**
   * Three-arg variant that additionally invokes {@code completion} exactly once on every terminal
   * path (success, FAILED-marker write, GENERATING-dedup skip, synchronous throw, rejection skip).
   * Used by the cron refresher to chain one index's refresh into the next.
   */
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
              // getRaw never calls onFailure, but defend anyway.
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

  private void proceedWithCollect(
      String indexName,
      Map<String, OpenSearchDataType> fieldTypes,
      ActionListener<Void> completion) {
    // Mark GENERATING (fire-and-forget — log failures at DEBUG).
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
            if (ExceptionsHelper.unwrap(e, OpenSearchRejectedExecutionException.class) != null) {
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

  /**
   * Wrap {@code inner} so its {@code onResponse} / {@code onFailure} fires at most once regardless
   * of how many terminal paths invoke it. Treats failure as a completion: callers only need to know
   * "done" — errors from this async pipeline are already logged by the time we get here.
   */
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
        onResponse(null);
      }
    };
  }

  /**
   * Return {@code true} if the stored source map has {@code status=GENERATING} and its {@code
   * last_updated_time} is within {@link #STALE_GENERATING} of now. Malformed timestamps are treated
   * as stale (proceed with refresh).
   */
  private static boolean isFreshGenerating(Map<String, Object> sourceMap) {
    Object status = sourceMap.get("status");
    if (!TableStatistic.STATUS_GENERATING.equals(status)) {
      return false;
    }
    Object rawTs = sourceMap.get("last_updated_time");
    if (!(rawTs instanceof String)) {
      return false;
    }
    try {
      Instant ts = Instant.parse((String) rawTs);
      return Duration.between(ts, Instant.now()).compareTo(STALE_GENERATING) <= 0;
    } catch (RuntimeException e) {
      return false;
    }
  }

  /**
   * Build the aggregation request covering every eligible field. Cardinality goes inside a sampler
   * (bounded per-shard cost); min/max are top-level (Lucene BKD short-circuit, O(1) per segment).
   * See class javadoc for the full shape and field-type handling.
   */
  SearchRequest buildAggregationRequest(
      String indexName, Map<String, OpenSearchDataType> fieldTypes) {
    Map<String, OpenSearchDataType> flat = OpenSearchDataType.traverseAndFlatten(fieldTypes);

    SamplerAggregationBuilder samplerAgg =
        AggregationBuilders.sampler(SAMPLER_AGG).shardSize(SAMPLER_SHARD_SIZE);

    SearchSourceBuilder source =
        new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(0).trackTotalHits(true);

    for (Map.Entry<String, OpenSearchDataType> entry : flat.entrySet()) {
      String name = entry.getKey();
      OpenSearchDataType type = entry.getValue();
      MappingType mt = type.getMappingType();
      if (mt == null) {
        continue;
      }

      String aggField;
      boolean doCardinality;
      boolean doMinMax;

      if (mt == MappingType.Text) {
        aggField = OpenSearchTextType.toKeywordSubField(name, type.getExprType());
        if (aggField == null) {
          // text without a keyword sub-field — cannot run cardinality on it.
          continue;
        }
        doCardinality = true;
        doMinMax = false;
      } else if (CARDINALITY_TYPES.contains(mt) || MIN_MAX_TYPES.contains(mt)) {
        aggField = name;
        doCardinality = CARDINALITY_TYPES.contains(mt);
        doMinMax = MIN_MAX_TYPES.contains(mt);
      } else {
        continue;
      }

      if (doCardinality) {
        samplerAgg.subAggregation(
            AggregationBuilders.cardinality(CARDINALITY_PREFIX + name).field(aggField));
      }
      if (doMinMax) {
        source.aggregation(AggregationBuilders.min(MIN_PREFIX + name).field(aggField));
        source.aggregation(AggregationBuilders.max(MAX_PREFIX + name).field(aggField));
      }
      if (doCardinality || doMinMax) {
        source.aggregation(AggregationBuilders.count(COUNT_PREFIX + name).field(aggField));
      }
    }

    // Sampler with zero sub-aggregations is rejected by OpenSearch ("all shards failed"), so add
    // it only when at least one cardinality sub-agg exists. When no fields are eligible we still
    // get the trackTotalHits doc_count.
    if (!samplerAgg.getSubAggregations().isEmpty()) {
      source.aggregation(samplerAgg);
    }
    return new SearchRequest(indexName).source(source);
  }

  /**
   * Extract per-field statistics and combine with the total hit count to produce a {@link
   * TableStatistic}. Cardinality is read from the sampler's sub-aggregations; min/max are read from
   * the top-level aggregations. Missing aggregations for a field (e.g. unsupported type) result in
   * the field being omitted from the output.
   */
  TableStatistic parseSearchResponse(
      SearchResponse response, Map<String, OpenSearchDataType> fieldTypes) {
    long docCount = response.getHits().getTotalHits().value();

    Aggregations topAggs = response.getAggregations();
    InternalAggregations samplerSubAggs = null;
    if (topAggs != null) {
      Object rawSampler = topAggs.get(SAMPLER_AGG);
      if (rawSampler instanceof InternalSampler sampler) {
        samplerSubAggs = sampler.getAggregations();
      } else if (rawSampler != null) {
        LOG.warn(
            "Expected sampler aggregation at '{}' but got {}",
            SAMPLER_AGG,
            rawSampler.getClass().getSimpleName());
      }
    }

    Map<String, OpenSearchDataType> flat = OpenSearchDataType.traverseAndFlatten(fieldTypes);
    Map<String, FieldStatistic> fields = new LinkedHashMap<>();
    for (Map.Entry<String, OpenSearchDataType> entry : flat.entrySet()) {
      String name = entry.getKey();
      OpenSearchDataType type = entry.getValue();
      MappingType mt = type.getMappingType();
      if (mt == null) {
        continue;
      }

      InternalCardinality card =
          samplerSubAggs == null ? null : samplerSubAggs.get(CARDINALITY_PREFIX + name);
      InternalMin min = topAggs == null ? null : topAggs.get(MIN_PREFIX + name);
      InternalMax max = topAggs == null ? null : topAggs.get(MAX_PREFIX + name);
      InternalValueCount count = topAggs == null ? null : topAggs.get(COUNT_PREFIX + name);

      if (card == null && min == null && max == null && count == null) {
        // No aggregation was requested for this field (e.g. unsupported type).
        continue;
      }

      long cardinality = card == null ? 0L : card.getValue();
      Object minValue = min == null || Double.isInfinite(min.getValue()) ? null : min.getValue();
      Object maxValue = max == null || Double.isInfinite(max.getValue()) ? null : max.getValue();
      double nullRatio = 0.0;
      if (count != null && docCount > 0) {
        double computed = 1.0 - ((double) count.getValue() / (double) docCount);
        nullRatio = computed < 0.0 ? 0.0 : computed;
      }

      fields.put(
          name,
          new FieldStatistic(
              mt.toString(), cardinality, minValue, maxValue, Collections.emptyList(), nullRatio));
    }

    return TableStatistic.fromFields(docCount, fields);
  }

  /** Listener that discards success and logs failures at DEBUG. Used for fire-and-forget writes. */
  private static ActionListener<Void> noopListener(String indexName) {
    return new ActionListener<Void>() {
      @Override
      public void onResponse(Void ignored) {}

      @Override
      public void onFailure(Exception e) {
        LOG.debug("Fire-and-forget write failed for {}: {}", indexName, e.getMessage());
      }
    };
  }
}
