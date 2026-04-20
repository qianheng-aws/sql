/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.statistics;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Per-field statistics parsed from Index Insight STATISTICAL_DATA content.
 *
 * @param type OpenSearch field type (keyword, long, date, etc.)
 * @param cardinality approximate number of distinct values
 * @param minValue minimum value (numeric or date string), null if unavailable
 * @param maxValue maximum value (numeric or date string), null if unavailable
 * @param topTerms top-K frequent terms
 * @param nullRatio fraction of documents where this field is null (0.0-1.0)
 */
public record FieldStatistic(
    String type,
    long cardinality,
    Object minValue,
    Object maxValue,
    List<Object> topTerms,
    double nullRatio) {

  private static final double DEFAULT_EQUALITY_SELECTIVITY = 0.15;
  private static final double DEFAULT_RANGE_SELECTIVITY = 0.5;

  @SuppressWarnings("unchecked")
  public static FieldStatistic fromInsightMap(Map<String, Object> fieldData) {
    String type = (String) fieldData.getOrDefault("type", "unknown");

    double rawCardinality =
        fieldData.containsKey("unique_count")
            ? ((Number) fieldData.get("unique_count")).doubleValue()
            : 0.0;
    long cardinality = (long) rawCardinality;

    Object minValue = fieldData.get("min_value");
    Object maxValue = fieldData.get("max_value");

    List<Object> topTerms =
        fieldData.containsKey("unique_terms")
            ? (List<Object>) fieldData.get("unique_terms")
            : Collections.emptyList();

    double nullRatio =
        fieldData.containsKey("null_ratio")
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
