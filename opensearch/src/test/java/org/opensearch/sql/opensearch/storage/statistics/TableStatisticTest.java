/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.statistics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.schema.Statistic;
import org.junit.jupiter.api.Test;

class TableStatisticTest {

  @Test
  void fromFields_roundtrip_preservesDocCountAndFields() {
    Instant beforeBuild = Instant.now();
    FieldStatistic status = new FieldStatistic("keyword", 5L, null, null, List.of("200"), 0.0);
    TableStatistic stat = TableStatistic.fromFields(137L, Map.of("status", status));

    assertEquals(137.0, stat.getRowCount());
    assertEquals(137L, stat.getDocCount());
    assertNotNull(stat.getFieldStatistic("status"));
    assertEquals(5L, stat.getFieldStatistic("status").cardinality());
    // lastUpdatedTime should be within the last second (generously allow 5 seconds to avoid
    // flakiness).
    Duration elapsed = Duration.between(beforeBuild, stat.getLastUpdatedTime());
    assertTrue(
        elapsed.toMillis() >= 0 && elapsed.toMillis() < 5000,
        "lastUpdatedTime should be within 5s of build time, elapsed=" + elapsed.toMillis() + "ms");
  }

  @Test
  void fromFields_fieldsMapIsImmutable() {
    Map<String, FieldStatistic> mutableSource = new HashMap<>();
    FieldStatistic f = new FieldStatistic("keyword", 5L, null, null, List.of("a"), 0.0);
    mutableSource.put("f", f);

    TableStatistic stat = TableStatistic.fromFields(10L, mutableSource);

    // Returned map must be immutable
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            stat.getFields()
                .put("g", new FieldStatistic("keyword", 1L, null, null, List.of(), 0.0)));

    // Mutating the original input must not affect the statistic
    mutableSource.put("g", new FieldStatistic("keyword", 99L, null, null, List.of(), 0.0));
    assertFalse(stat.getFields().containsKey("g"));
  }

  @Test
  void fromStoredDoc_parsesAllFields() {
    String timestamp = "2026-04-20T05:17:42.123Z";
    Map<String, Object> statusField = new LinkedHashMap<>();
    statusField.put("type", "keyword");
    statusField.put("unique_count", 5.0);
    statusField.put("unique_terms", List.of("200", "404"));
    statusField.put("null_ratio", 0.0);

    Map<String, Object> latencyField = new LinkedHashMap<>();
    latencyField.put("type", "long");
    latencyField.put("unique_count", 8500.0);
    latencyField.put("min_value", 1.0);
    latencyField.put("max_value", 30000.0);
    latencyField.put("null_ratio", 0.0);

    Map<String, Object> fields = new LinkedHashMap<>();
    fields.put("status", statusField);
    fields.put("latency", latencyField);

    Map<String, Object> source = new LinkedHashMap<>();
    source.put("status", "COMPLETED");
    source.put("last_updated_time", timestamp);
    source.put("doc_count", 123456);
    source.put("fields", fields);

    TableStatistic stat = TableStatistic.fromStoredDoc(source);

    assertEquals(123456L, stat.getDocCount());
    assertEquals(123456.0, stat.getRowCount());
    assertEquals(Instant.parse(timestamp), stat.getLastUpdatedTime());
    assertNotNull(stat.getFieldStatistic("status"));
    assertEquals("keyword", stat.getFieldStatistic("status").type());
    assertEquals(5L, stat.getFieldStatistic("status").cardinality());
    assertEquals(List.of("200", "404"), stat.getFieldStatistic("status").topTerms());

    assertNotNull(stat.getFieldStatistic("latency"));
    assertEquals("long", stat.getFieldStatistic("latency").type());
    assertEquals(8500L, stat.getFieldStatistic("latency").cardinality());
    assertEquals(1.0, stat.getFieldStatistic("latency").minValue());
    assertEquals(30000.0, stat.getFieldStatistic("latency").maxValue());
  }

  @Test
  void fromStoredDoc_missingDocCount_throwsIAE() {
    Map<String, Object> source = new LinkedHashMap<>();
    source.put("last_updated_time", "2026-04-20T05:17:42.123Z");
    source.put("fields", Map.of());

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> TableStatistic.fromStoredDoc(source));
    assertTrue(
        ex.getMessage().contains("doc_count"),
        "Exception message should mention doc_count, got: " + ex.getMessage());
  }

  @Test
  void fromStoredDoc_missingLastUpdatedTime_throwsIAE() {
    Map<String, Object> source = new LinkedHashMap<>();
    source.put("doc_count", 42);
    source.put("fields", Map.of());

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> TableStatistic.fromStoredDoc(source));
    assertTrue(
        ex.getMessage().contains("last_updated_time"),
        "Exception message should mention last_updated_time, got: " + ex.getMessage());
  }

  @Test
  void fromStoredDoc_missingFieldsKey_returnsEmptyFieldsMap() {
    Map<String, Object> source = new LinkedHashMap<>();
    source.put("doc_count", 42);
    source.put("last_updated_time", "2026-04-20T05:17:42.123Z");

    TableStatistic stat = TableStatistic.fromStoredDoc(source);
    assertEquals(42L, stat.getDocCount());
    assertTrue(stat.getFields().isEmpty());
  }

  @Test
  void toStoredDocSource_shape() {
    FieldStatistic withMinMax =
        new FieldStatistic("long", 100L, 1.0, 999.0, List.of(1.0, 2.0), 0.0);
    FieldStatistic noMinMaxNoTerms = new FieldStatistic("keyword", 7L, null, null, List.of(), 0.1);

    Map<String, FieldStatistic> fields = new LinkedHashMap<>();
    fields.put("latency", withMinMax);
    fields.put("status", noMinMaxNoTerms);

    TableStatistic stat = TableStatistic.fromFields(55L, fields);
    Map<String, Object> source = stat.toStoredDocSource();

    // Top-level keys
    assertEquals(
        List.of("status", "last_updated_time", "doc_count", "fields"),
        List.copyOf(source.keySet()));
    assertEquals("COMPLETED", source.get("status"));
    assertEquals(55L, source.get("doc_count"));
    assertNotNull(source.get("last_updated_time"));
    // Last updated time should be parseable as Instant
    Instant.parse((String) source.get("last_updated_time"));

    @SuppressWarnings("unchecked")
    Map<String, Object> fieldsOut = (Map<String, Object>) source.get("fields");
    assertNotNull(fieldsOut);

    @SuppressWarnings("unchecked")
    Map<String, Object> latencyOut = (Map<String, Object>) fieldsOut.get("latency");
    assertEquals("long", latencyOut.get("type"));
    assertEquals(100L, latencyOut.get("unique_count"));
    assertEquals(List.of(1.0, 2.0), latencyOut.get("unique_terms"));
    assertEquals(1.0, latencyOut.get("min_value"));
    assertEquals(999.0, latencyOut.get("max_value"));
    assertEquals(0.0, latencyOut.get("null_ratio"));

    @SuppressWarnings("unchecked")
    Map<String, Object> statusOut = (Map<String, Object>) fieldsOut.get("status");
    assertEquals("keyword", statusOut.get("type"));
    assertEquals(7L, statusOut.get("unique_count"));
    assertEquals(0.1, statusOut.get("null_ratio"));
    // null minValue should be omitted
    assertFalse(statusOut.containsKey("min_value"), "min_value should be omitted when null");
    assertFalse(statusOut.containsKey("max_value"), "max_value should be omitted when null");
    // empty topTerms should be omitted
    assertFalse(statusOut.containsKey("unique_terms"), "unique_terms should be omitted when empty");
  }

  @Test
  void toStoredDocSource_thenFromStoredDoc_roundtrip() {
    FieldStatistic latency = new FieldStatistic("long", 8500L, 1.0, 30000.0, List.of(), 0.0);
    FieldStatistic status =
        new FieldStatistic("keyword", 5L, null, null, List.of("200", "404"), 0.0);

    Map<String, FieldStatistic> fields = new LinkedHashMap<>();
    fields.put("latency", latency);
    fields.put("status", status);

    TableStatistic original = TableStatistic.fromFields(9999L, fields);
    Map<String, Object> serialized = original.toStoredDocSource();

    TableStatistic restored = TableStatistic.fromStoredDoc(serialized);
    assertEquals(original.getDocCount(), restored.getDocCount());
    assertEquals(original.getLastUpdatedTime(), restored.getLastUpdatedTime());
    assertEquals(original.getFields().keySet(), restored.getFields().keySet());

    FieldStatistic restoredLatency = restored.getFieldStatistic("latency");
    assertEquals("long", restoredLatency.type());
    assertEquals(8500L, restoredLatency.cardinality());
    assertEquals(1.0, restoredLatency.minValue());
    assertEquals(30000.0, restoredLatency.maxValue());

    FieldStatistic restoredStatus = restored.getFieldStatistic("status");
    assertEquals("keyword", restoredStatus.type());
    assertEquals(5L, restoredStatus.cardinality());
    assertEquals(List.of("200", "404"), restoredStatus.topTerms());
  }

  @Test
  void isStale_withinTtl_returnsFalse() {
    TableStatistic stat = TableStatistic.fromFields(1L, Map.of());
    assertFalse(stat.isStale(Duration.ofHours(24)));
  }

  @Test
  void isStale_beyondTtl_returnsTrue() {
    Instant oldTime = Instant.now().minus(Duration.ofHours(25));
    TableStatistic stat = TableStatistic.forTesting(1L, Map.of(), oldTime);
    assertTrue(stat.isStale(Duration.ofHours(24)));
  }

  @Test
  void implementsCalciteStatistic() {
    TableStatistic stat = TableStatistic.fromFields(1L, Map.of());
    assertInstanceOf(Statistic.class, stat);
    assertNotNull(stat.getKeys());
    assertTrue(stat.getKeys().isEmpty());
    assertNotNull(stat.getCollations());
    assertTrue(stat.getCollations().isEmpty());
  }
}
