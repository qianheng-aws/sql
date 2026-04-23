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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Supplies filter-selectivity estimates to Calcite's optimizer using values drawn from a {@link
 * TableStatistic}. Returned through {@link
 * org.opensearch.sql.opensearch.storage.OpenSearchIndex#unwrap(Class)} so Calcite's default {@code
 * RelMdSelectivity.getSelectivity(TableScan, ...)} will dispatch to this handler for our scans.
 *
 * <p>Covered filter shapes — those where per-column stats give a reliably-better estimate than
 * {@link org.apache.calcite.rel.metadata.RelMdUtil#guessSelectivity}:
 *
 * <ul>
 *   <li>{@code col = literal} &rarr; {@code 1 / cardinality}
 *   <li>{@code col IS NULL} &rarr; {@code nullRatio}
 *   <li>{@code col IS NOT NULL} &rarr; {@code 1 - nullRatio}
 *   <li>{@code col > / >= / < / <= literal} on a numeric/date field with stored min/max &rarr;
 *       linear interpolation over {@code [min, max]}. Assumes uniform distribution — known to
 *       overshoot on skewed data; histogram support is deferred (see design doc §3.4).
 *   <li>AND of the above &rarr; product of individual selectivities (independence assumption). A
 *       {@code BETWEEN} predicate is lowered by Calcite into {@code AND(>=, <=)}, so it falls out
 *       of the conjunct-decomposition path automatically.
 * </ul>
 *
 * <p>For any other predicate shape (OR/NOT, IN-list, LIKE, script, range on a string column), a
 * conjunct is represented by Calcite's heuristic via {@link
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

    // Calcite's RexSimplify collapses multiple comparisons on the same column into a
    // SEARCH(col, Sarg[...]) call. Expand those back to plain comparisons first, otherwise
    // conjunct decomposition would treat the whole Sarg as one opaque predicate and miss
    // the stat-informed selectivity of each bound.
    RexBuilder rexBuilder = scan.getCluster().getRexBuilder();
    RexNode expanded = RexUtil.expandSearch(rexBuilder, null, predicate);

    List<RelDataTypeField> fields = scan.getRowType().getFieldList();
    double selectivity = 1.0;

    for (RexNode conjunct : RelOptUtil.conjunctions(expanded)) {
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

    if (kind == SqlKind.GREATER_THAN
        || kind == SqlKind.GREATER_THAN_OR_EQUAL
        || kind == SqlKind.LESS_THAN
        || kind == SqlKind.LESS_THAN_OR_EQUAL) {
      return rangeSelectivity(kind, operands, fields);
    }

    return null;
  }

  /**
   * Estimate selectivity of a one-sided range predicate {@code col OP literal}. Supports both
   * {@code (col, literal)} and {@code (literal, col)} orderings — Calcite normalises the operator
   * when flipping, so we just match on shape.
   *
   * <p>Requires the column's stored min/max to be numeric (or date-epoch-millis, which is also a
   * {@link Number}). String ranges are deferred until we collect min/max for keyword fields.
   *
   * <p>Let {@code [lo, hi]} be the stored range and {@code v} the literal. Selectivity under a
   * uniform-distribution assumption:
   *
   * <ul>
   *   <li>{@code col > v} &rarr; {@code (hi - v) / (hi - lo)}
   *   <li>{@code col >= v} &rarr; same as {@code >} at this coarse level (±1-row difference below
   *       the resolution of the formula)
   *   <li>{@code col < v} &rarr; {@code (v - lo) / (hi - lo)}
   *   <li>{@code col <= v} &rarr; same as {@code <}
   * </ul>
   *
   * Clamps to {@code [0, 1]}. If {@code hi == lo} (single-value column), returns {@code null} so
   * the caller falls back to Calcite's heuristic — the degenerate formula would be {@code 0/0}.
   */
  private @Nullable Double rangeSelectivity(
      SqlKind kind, List<RexNode> operands, List<RelDataTypeField> fields) {
    if (operands.size() != 2) {
      return null;
    }
    RexNode left = operands.get(0);
    RexNode right = operands.get(1);

    RexInputRef ref;
    RexLiteral literal;
    boolean columnOnLeft;
    if (left instanceof RexInputRef l && right instanceof RexLiteral r) {
      ref = l;
      literal = r;
      columnOnLeft = true;
    } else if (left instanceof RexLiteral l && right instanceof RexInputRef r) {
      ref = r;
      literal = l;
      columnOnLeft = false;
    } else {
      return null;
    }

    FieldStatistic fieldStat = fieldStatOfOperand(ref, fields);
    if (fieldStat == null) {
      return null;
    }
    if (!(fieldStat.minValue() instanceof Number minNum)
        || !(fieldStat.maxValue() instanceof Number maxNum)) {
      return null;
    }
    Number literalNum = literal.getValueAs(Number.class);
    if (literalNum == null) {
      return null;
    }

    double lo = minNum.doubleValue();
    double hi = maxNum.doubleValue();
    double v = literalNum.doubleValue();
    double range = hi - lo;
    if (range <= 0.0) {
      return null;
    }

    // Normalise to "column OP literal" form; when literal is on the left, invert the operator.
    SqlKind effective = columnOnLeft ? kind : flipInequality(kind);
    double selectivity =
        switch (effective) {
          case GREATER_THAN, GREATER_THAN_OR_EQUAL -> (hi - v) / range;
          case LESS_THAN, LESS_THAN_OR_EQUAL -> (v - lo) / range;
          default -> -1.0; // unreachable — caller already filtered kind
        };
    return clamp(selectivity);
  }

  private static SqlKind flipInequality(SqlKind kind) {
    return switch (kind) {
      case GREATER_THAN -> SqlKind.LESS_THAN;
      case GREATER_THAN_OR_EQUAL -> SqlKind.LESS_THAN_OR_EQUAL;
      case LESS_THAN -> SqlKind.GREATER_THAN;
      case LESS_THAN_OR_EQUAL -> SqlKind.GREATER_THAN_OR_EQUAL;
      default -> kind;
    };
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
