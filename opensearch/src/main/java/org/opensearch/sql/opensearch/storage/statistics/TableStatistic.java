/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.statistics;

import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * Calcite {@link Statistic} implementation backed by table-level statistics produced by our
 * SQL-internal collector. Holds a document count and per-field {@link FieldStatistic} entries, and
 * supports serialization to/from the stored document schema used by the statistics index.
 */
public class TableStatistic implements Statistic {

  /** Statuses written into the stored document. */
  public static final String STATUS_COMPLETED = "COMPLETED";

  public static final String STATUS_GENERATING = "GENERATING";
  public static final String STATUS_FAILED = "FAILED";

  private final long docCount;
  private final Map<String, FieldStatistic> fields;
  private final Instant lastUpdatedTime;

  private TableStatistic(
      long docCount, Map<String, FieldStatistic> fields, Instant lastUpdatedTime) {
    this.docCount = docCount;
    this.fields = Collections.unmodifiableMap(new LinkedHashMap<>(fields));
    this.lastUpdatedTime = Objects.requireNonNull(lastUpdatedTime, "lastUpdatedTime");
  }

  /**
   * Build a {@link TableStatistic} from freshly-collected field stats, stamping {@code
   * lastUpdatedTime} to {@link Instant#now()}.
   */
  public static TableStatistic fromFields(long docCount, Map<String, FieldStatistic> fields) {
    return new TableStatistic(docCount, fields, Instant.now());
  }

  /**
   * Package-private factory for tests that need to control {@code lastUpdatedTime} (e.g. to
   * exercise {@link #isStale(Duration)}).
   */
  static TableStatistic forTesting(
      long docCount, Map<String, FieldStatistic> fields, Instant lastUpdatedTime) {
    return new TableStatistic(docCount, fields, lastUpdatedTime);
  }

  /**
   * Parse a stored document source map (as retrieved from the statistics index) into a {@link
   * TableStatistic}. The writer controls the document schema, so missing required keys ({@code
   * doc_count}, {@code last_updated_time}), non-{@code COMPLETED} status, or malformed values
   * (wrong types, unparseable timestamp) are all surfaced as {@link IllegalArgumentException}.
   *
   * <p>Callers should catch {@code IllegalArgumentException} for any parse failure; no other
   * exception type escapes.
   */
  @SuppressWarnings("unchecked")
  public static TableStatistic fromStoredDoc(Map<String, Object> sourceMap) {
    try {
      if (!sourceMap.containsKey("doc_count")) {
        throw new IllegalArgumentException("TableStatistic document is missing doc_count");
      }
      if (!sourceMap.containsKey("last_updated_time")) {
        throw new IllegalArgumentException("TableStatistic document is missing last_updated_time");
      }
      Object status = sourceMap.get("status");
      if (status != null && !STATUS_COMPLETED.equals(status)) {
        throw new IllegalArgumentException(
            "TableStatistic document has non-COMPLETED status: " + status);
      }

      long docCount = ((Number) sourceMap.get("doc_count")).longValue();
      Instant lastUpdatedTime = Instant.parse((String) sourceMap.get("last_updated_time"));

      Map<String, FieldStatistic> fieldStats = new LinkedHashMap<>();
      Object rawFields = sourceMap.get("fields");
      if (rawFields instanceof Map<?, ?> fieldsMap) {
        for (Map.Entry<?, ?> entry : fieldsMap.entrySet()) {
          if (!(entry.getKey() instanceof String fieldName)) {
            throw new IllegalArgumentException(
                "TableStatistic fields entry has non-String key: " + entry.getKey());
          }
          if (!(entry.getValue() instanceof Map<?, ?> rawFieldData)) {
            throw new IllegalArgumentException(
                "TableStatistic fields entry '" + fieldName + "' has non-Map value");
          }
          fieldStats.put(
              fieldName, FieldStatistic.fromInsightMap((Map<String, Object>) rawFieldData));
        }
      }

      return new TableStatistic(docCount, fieldStats, lastUpdatedTime);
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(
          "Failed to parse TableStatistic document: " + e.getMessage(), e);
    }
  }

  /**
   * Serialize this {@link TableStatistic} to a {@link LinkedHashMap} suitable for indexing. The
   * returned map uses stable ordering so JSON output is deterministic.
   *
   * <p>For each field, {@code min_value} / {@code max_value} are omitted when null, and {@code
   * unique_terms} is omitted when the list is empty.
   */
  public Map<String, Object> toStoredDocSource() {
    Map<String, Object> source = new LinkedHashMap<>();
    source.put("status", STATUS_COMPLETED);
    source.put("last_updated_time", lastUpdatedTime.toString());
    source.put("doc_count", docCount);

    Map<String, Object> fieldsOut = new LinkedHashMap<>();
    for (Map.Entry<String, FieldStatistic> entry : fields.entrySet()) {
      FieldStatistic fs = entry.getValue();
      Map<String, Object> fieldOut = new LinkedHashMap<>();
      fieldOut.put("type", fs.type());
      fieldOut.put("unique_count", fs.cardinality());
      if (fs.topTerms() != null && !fs.topTerms().isEmpty()) {
        fieldOut.put("unique_terms", fs.topTerms());
      }
      if (fs.minValue() != null) {
        fieldOut.put("min_value", fs.minValue());
      }
      if (fs.maxValue() != null) {
        fieldOut.put("max_value", fs.maxValue());
      }
      fieldOut.put("null_ratio", fs.nullRatio());
      fieldsOut.put(entry.getKey(), fieldOut);
    }
    source.put("fields", fieldsOut);
    return source;
  }

  /** Returns {@code true} if this statistic is older than the supplied TTL. */
  public boolean isStale(Duration ttl) {
    return Duration.between(lastUpdatedTime, Instant.now()).compareTo(ttl) > 0;
  }

  public Map<String, FieldStatistic> getFields() {
    return fields;
  }

  public FieldStatistic getFieldStatistic(String name) {
    return fields.get(name);
  }

  public Instant getLastUpdatedTime() {
    return lastUpdatedTime;
  }

  public long getDocCount() {
    return docCount;
  }

  @Override
  public Double getRowCount() {
    return (double) docCount;
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
