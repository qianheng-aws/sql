/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

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

/**
 * Calcite {@link Statistic} implementation backed by ml-commons Index Insight STATISTICAL_DATA
 * content. Parses the JSON {@code content} payload into per-field {@link FieldStatistic} entries
 * and exposes the row count from the provided document count.
 */
public class IndexInsightStatistic implements Statistic {

  private static final Logger LOG = LogManager.getLogger(IndexInsightStatistic.class);
  private static final String IMPORTANT_COLUMN_KEY = "important_column_and_distribution";
  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {}.getType();

  private final double rowCount;

  @Getter private final Map<String, FieldStatistic> fieldStatistics;

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
