/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.statistics;

import java.util.List;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Supplies filter-selectivity estimates to Calcite's optimizer using values drawn from a {@link
 * TableStatistic}. Returned through {@link
 * org.opensearch.sql.opensearch.storage.OpenSearchIndex#unwrap(Class)} so Calcite's default {@code
 * RelMdSelectivity.getSelectivity(TableScan, ...)} will dispatch to this handler for our scans.
 *
 * <p>Phase A coverage — the common filter shapes where per-column stats give a reliably-better
 * estimate than {@link org.apache.calcite.rel.metadata.RelMdUtil#guessSelectivity}:
 *
 * <ul>
 *   <li>{@code col = literal} &rarr; {@code 1 / cardinality}
 *   <li>{@code col IS NULL} &rarr; {@code nullRatio}
 *   <li>{@code col IS NOT NULL} &rarr; {@code 1 - nullRatio}
 *   <li>AND of the above &rarr; product of individual selectivities (independence assumption)
 * </ul>
 *
 * <p>For any other predicate shape (range, OR/NOT, IN-list, LIKE, script), a conjunct is
 * represented by Calcite's heuristic via {@link
 * org.apache.calcite.rel.metadata.RelMdUtil#guessSelectivity(RexNode)} so the final result remains
 * a reasonable mix. The whole-predicate result is never null when at least one conjunct is handled;
 * returning null would discard the stat-informed conjuncts and let Calcite fall back to {@code
 * guessSelectivity} for the whole predicate.
 */
public class TableStatisticSelectivityHandler implements BuiltInMetadata.Selectivity.Handler {

  private final TableStatistic statistic;

  public TableStatisticSelectivityHandler(TableStatistic statistic) {
    this.statistic = statistic;
  }

  @Override
  public @Nullable Double getSelectivity(
      RelNode rel, RelMetadataQuery mq, @Nullable RexNode predicate) {
    if (predicate == null || predicate.isAlwaysTrue()) {
      return 1.0;
    }
    if (!(rel instanceof TableScan scan)) {
      return null;
    }

    List<RelDataTypeField> fields = scan.getRowType().getFieldList();
    double selectivity = 1.0;

    for (RexNode conjunct : RelOptUtil.conjunctions(predicate)) {
      Double conjunctSel = selectivityOfConjunct(conjunct, fields);
      if (conjunctSel == null) {
        // Fall back to Calcite's heuristic for conjuncts we can't reason about — keeps the stat-
        // informed factors in play without forcing the whole predicate onto the default path.
        conjunctSel = org.apache.calcite.rel.metadata.RelMdUtil.guessSelectivity(conjunct);
      }
      selectivity *= conjunctSel;
    }
    return clamp(selectivity);
  }

  private @Nullable Double selectivityOfConjunct(RexNode conjunct, List<RelDataTypeField> fields) {
    if (!(conjunct instanceof RexCall call)) {
      return null;
    }

    SqlKind kind = call.getKind();
    List<RexNode> operands = call.getOperands();

    if (kind == SqlKind.IS_NULL || kind == SqlKind.IS_NOT_NULL) {
      if (operands.size() != 1) {
        return null;
      }
      FieldStatistic fieldStat = fieldStatOfOperand(operands.get(0), fields);
      if (fieldStat == null) {
        return null;
      }
      return kind == SqlKind.IS_NULL ? fieldStat.nullRatio() : 1.0 - fieldStat.nullRatio();
    }

    if (kind == SqlKind.EQUALS) {
      FieldStatistic fieldStat = equalityFieldStat(operands, fields);
      if (fieldStat == null) {
        return null;
      }
      return fieldStat.equalitySelectivity();
    }

    return null;
  }

  /**
   * Equality operators are symmetric — look for a {@code (RexInputRef, RexLiteral)} pair in either
   * order. If neither arrangement matches (e.g. both sides are expressions, or neither is a
   * literal), we can't apply per-column stats and return null.
   */
  private @Nullable FieldStatistic equalityFieldStat(
      List<RexNode> operands, List<RelDataTypeField> fields) {
    if (operands.size() != 2) {
      return null;
    }
    RexNode left = operands.get(0);
    RexNode right = operands.get(1);
    FieldStatistic viaLeft = isLiteral(right) ? fieldStatOfOperand(left, fields) : null;
    if (viaLeft != null) {
      return viaLeft;
    }
    return isLiteral(left) ? fieldStatOfOperand(right, fields) : null;
  }

  private @Nullable FieldStatistic fieldStatOfOperand(
      RexNode operand, List<RelDataTypeField> fields) {
    if (!(operand instanceof RexInputRef ref)) {
      return null;
    }
    int idx = ref.getIndex();
    if (idx < 0 || idx >= fields.size()) {
      return null;
    }
    String name = fields.get(idx).getName();
    return statistic.getFieldStatistic(name);
  }

  private static boolean isLiteral(RexNode node) {
    return node instanceof RexLiteral;
  }

  private static double clamp(double value) {
    if (value < 0.0) {
      return 0.0;
    }
    if (value > 1.0) {
      return 1.0;
    }
    return value;
  }
}
