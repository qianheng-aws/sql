/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.statistics;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Supplies per-column distinct-row-count estimates to Calcite's optimizer using values drawn from a
 * {@link TableStatistic}. Returned through {@link
 * org.opensearch.sql.opensearch.storage.OpenSearchIndex#unwrap(Class)} so Calcite's default {@code
 * RelMdDistinctRowCount.getDistinctRowCount(TableScan, ...)} will dispatch to this handler for our
 * scans.
 *
 * <p>When the handler returns a value, {@code RelMdRowCount#getRowCount(Aggregate)} uses it as the
 * group cardinality, avoiding Calcite's default {@code inputRowCount / 10} fallback.
 *
 * <p>Behaviour:
 *
 * <ul>
 *   <li>Empty {@code groupKey} → {@code null} (let the default catch-all apply)
 *   <li>Non-null {@code predicate} → {@code null} (we have no filter-aware stats here)
 *   <li>Any grouping column lacks a stored {@link FieldStatistic} → {@code null}
 *   <li>Single-column group → column's stored cardinality, capped at the table row count
 *   <li>Multi-column group → {@link RelMdUtil#numDistinctVals} applied to the product of per-column
 *       cardinalities, capped at the table row count
 * </ul>
 */
public class TableStatisticDistinctRowCountHandler
    implements BuiltInMetadata.DistinctRowCount.Handler {

  private final TableStatistic statistic;

  public TableStatisticDistinctRowCountHandler(TableStatistic statistic) {
    this.statistic = statistic;
  }

  @Override
  public @Nullable Double getDistinctRowCount(
      RelNode rel, RelMetadataQuery mq, ImmutableBitSet groupKey, @Nullable RexNode predicate) {
    if (!(rel instanceof TableScan scan)) {
      return null;
    }
    if (predicate != null && !predicate.isAlwaysTrue()) {
      return null;
    }
    if (groupKey.isEmpty()) {
      return null;
    }

    double rowCount = statistic.getRowCount();
    if (rowCount <= 0) {
      return null;
    }

    List<RelDataTypeField> fields = scan.getRowType().getFieldList();
    double product = 1.0;
    for (int idx : groupKey) {
      if (idx < 0 || idx >= fields.size()) {
        return null;
      }
      String name = fields.get(idx).getName();
      FieldStatistic fieldStat = statistic.getFieldStatistic(name);
      if (fieldStat == null || fieldStat.cardinality() <= 0) {
        return null;
      }
      product *= fieldStat.cardinality();
      if (Double.isInfinite(product)) {
        break;
      }
    }

    if (groupKey.cardinality() == 1) {
      return Math.min(product, rowCount);
    }
    return RelMdUtil.numDistinctVals(product, rowCount);
  }
}
