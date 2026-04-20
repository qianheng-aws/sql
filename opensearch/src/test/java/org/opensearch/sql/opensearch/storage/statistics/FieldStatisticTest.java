/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.statistics;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FieldStatisticTest {

  @Test
  void parseFromInsightMap_keywordField() {
    Map<String, Object> fieldData =
        Map.of(
            "type",
            "keyword",
            "unique_terms",
            List.of("GET", "POST", "PUT", "DELETE", "PATCH"),
            "unique_count",
            5.0);
    FieldStatistic stat = FieldStatistic.fromInsightMap(fieldData);
    assertEquals("keyword", stat.type());
    assertEquals(5L, stat.cardinality());
    assertEquals(List.of("GET", "POST", "PUT", "DELETE", "PATCH"), stat.topTerms());
    assertNull(stat.minValue());
    assertNull(stat.maxValue());
  }

  @Test
  void parseFromInsightMap_longField() {
    Map<String, Object> fieldData =
        Map.of(
            "type", "long",
            "unique_count", 10000.0,
            "unique_terms", List.of(200.0, 301.0, 404.0, 500.0, 502.0),
            "min_value", 100.0,
            "max_value", 99999.0);
    FieldStatistic stat = FieldStatistic.fromInsightMap(fieldData);
    assertEquals(10000L, stat.cardinality());
    assertEquals(100.0, stat.minValue());
    assertEquals(99999.0, stat.maxValue());
  }

  @Test
  void parseFromInsightMap_dateField() {
    Map<String, Object> fieldData =
        Map.of(
            "type", "date",
            "min_value", "2024-01-01T00:00:00Z",
            "max_value", "2024-12-31T23:59:59Z");
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
    // Range [20, 80] -> (80-20)/(100-0) = 0.6
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
