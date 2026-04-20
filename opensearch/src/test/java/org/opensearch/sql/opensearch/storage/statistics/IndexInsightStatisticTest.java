/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.statistics;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.calcite.schema.Statistic;
import org.junit.jupiter.api.Test;

class IndexInsightStatisticTest {

  private static final String SAMPLE_CONTENT =
      """
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
