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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.sampler.InternalSampler;
import org.opensearch.search.aggregations.bucket.sampler.SamplerAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalCardinality;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Orchestrates asynchronous table-statistic collection: builds a single sampler-backed aggregation
 * request covering the whole flattened field schema, sends it to {@link NodeClient}, and persists
 * the parsed {@link TableStatistic} into {@link TableStatisticStorage}.
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
    try {
      storage.getRaw(
          indexName,
          new ActionListener<Optional<Map<String, Object>>>() {
            @Override
            public void onResponse(Optional<Map<String, Object>> maybeDoc) {
              if (maybeDoc.isPresent() && isFreshGenerating(maybeDoc.get())) {
                LOG.debug("refresh for {} already in progress; skipping", indexName);
                return;
              }
              proceedWithCollect(indexName, fieldTypes);
            }

            @Override
            public void onFailure(Exception e) {
              // getRaw never calls onFailure, but defend anyway.
              LOG.debug(
                  "getRaw failed for {}: {}; proceeding with refresh anyway",
                  indexName,
                  e.getMessage());
              proceedWithCollect(indexName, fieldTypes);
            }
          });
    } catch (RuntimeException e) {
      LOG.warn("refreshAsync for {} threw synchronously: {}", indexName, e.getMessage());
    }
  }

  private void proceedWithCollect(String indexName, Map<String, OpenSearchDataType> fieldTypes) {
    // Mark GENERATING (fire-and-forget — log failures at DEBUG).
    storage.putStatus(indexName, TableStatistic.STATUS_GENERATING, noopListener(indexName));

    final SearchRequest request;
    try {
      request = buildAggregationRequest(indexName, fieldTypes);
    } catch (RuntimeException e) {
      LOG.warn("Failed to build aggregation request for {}: {}", indexName, e.getMessage());
      storage.putStatus(indexName, TableStatistic.STATUS_FAILED, noopListener(indexName));
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
                  }

                  @Override
                  public void onFailure(Exception e) {
                    LOG.warn("Failed to persist statistic for {}: {}", indexName, e.getMessage());
                    storage.putStatus(
                        indexName, TableStatistic.STATUS_FAILED, noopListener(indexName));
                  }
                });
          }

          @Override
          public void onFailure(Exception e) {
            LOG.warn("Failed to collect statistic for {}: {}", indexName, e.getMessage());
            storage.putStatus(indexName, TableStatistic.STATUS_FAILED, noopListener(indexName));
          }
        });
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
   * Build the sampler-wrapped aggregation request covering every eligible field. See class javadoc
   * for field-type handling.
   */
  SearchRequest buildAggregationRequest(
      String indexName, Map<String, OpenSearchDataType> fieldTypes) {
    Map<String, OpenSearchDataType> flat = OpenSearchDataType.traverseAndFlatten(fieldTypes);

    SamplerAggregationBuilder samplerAgg =
        AggregationBuilders.sampler(SAMPLER_AGG).shardSize(SAMPLER_SHARD_SIZE);

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
        samplerAgg.subAggregation(AggregationBuilders.min(MIN_PREFIX + name).field(aggField));
        samplerAgg.subAggregation(AggregationBuilders.max(MAX_PREFIX + name).field(aggField));
      }
    }

    SearchSourceBuilder source =
        new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery())
            .size(0)
            .trackTotalHits(true)
            .aggregation(samplerAgg);
    return new SearchRequest(indexName).source(source);
  }

  /**
   * Extract the per-field statistics from the sampler's sub-aggregations and combine with the total
   * hit count to produce a {@link TableStatistic}. When the sampler aggregation is missing from the
   * response, returns a statistic with just {@code docCount} populated and no fields.
   */
  TableStatistic parseSearchResponse(
      SearchResponse response, Map<String, OpenSearchDataType> fieldTypes) {
    long docCount = response.getHits().getTotalHits().value();

    Aggregations topAggs = response.getAggregations();
    InternalSampler sampler = topAggs == null ? null : topAggs.get(SAMPLER_AGG);
    if (sampler == null) {
      return TableStatistic.fromFields(docCount, Collections.emptyMap());
    }

    InternalAggregations subAggs = sampler.getAggregations();
    Map<String, OpenSearchDataType> flat = OpenSearchDataType.traverseAndFlatten(fieldTypes);

    Map<String, FieldStatistic> fields = new LinkedHashMap<>();
    for (Map.Entry<String, OpenSearchDataType> entry : flat.entrySet()) {
      String name = entry.getKey();
      OpenSearchDataType type = entry.getValue();
      MappingType mt = type.getMappingType();
      if (mt == null) {
        continue;
      }

      InternalCardinality card = subAggs == null ? null : subAggs.get(CARDINALITY_PREFIX + name);
      InternalMin min = subAggs == null ? null : subAggs.get(MIN_PREFIX + name);
      InternalMax max = subAggs == null ? null : subAggs.get(MAX_PREFIX + name);

      if (card == null && min == null && max == null) {
        // No aggregation was requested for this field (e.g. unsupported type).
        continue;
      }

      long cardinality = card == null ? 0L : card.getValue();
      Object minValue = min == null || Double.isInfinite(min.getValue()) ? null : min.getValue();
      Object maxValue = max == null || Double.isInfinite(max.getValue()) ? null : max.getValue();

      fields.put(
          name,
          new FieldStatistic(
              mt.toString(), cardinality, minValue, maxValue, Collections.emptyList(), 0.0));
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
