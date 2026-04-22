/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.statistics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import java.util.AbstractList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

class TableStatisticSelectivityHandlerTest {

  private static final RelDataTypeFactory TYPE_FACTORY =
      new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  private static final RexBuilder REX = new RexBuilder(TYPE_FACTORY);

  // Use nullable types so IS_NULL / IS_NOT_NULL predicates don't get
  // constant-folded away by Calcite's RexBuilder.
  private static final RelDataType STRING =
      TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR), true);
  private static final RelDataType INT =
      TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), true);

  // ---- equality ----

  @Test
  void equals_returnsOneOverCardinality() {
    TableStatistic stat =
        TableStatistic.fromFields(
            137L, Map.of("status", new FieldStatistic("keyword", 5L, null, null, List.of(), 0.0)));

    TableScan scan = mockScan(List.of("status"), List.of(STRING));
    RexNode predicate =
        REX.makeCall(
            SqlStdOperatorTable.EQUALS, REX.makeInputRef(STRING, 0), REX.makeLiteral("OK"));

    Double result =
        new TableStatisticSelectivityHandler(stat).getSelectivity(scan, mockMq(), predicate);

    assertNotNull(result);
    assertEquals(0.2, result, 1e-9);
  }

  @Test
  void equals_literalOnLeftStillResolved() {
    TableStatistic stat =
        TableStatistic.fromFields(
            137L, Map.of("status", new FieldStatistic("keyword", 4L, null, null, List.of(), 0.0)));

    TableScan scan = mockScan(List.of("status"), List.of(STRING));
    RexNode predicate =
        REX.makeCall(
            SqlStdOperatorTable.EQUALS, REX.makeLiteral("OK"), REX.makeInputRef(STRING, 0));

    Double result =
        new TableStatisticSelectivityHandler(stat).getSelectivity(scan, mockMq(), predicate);

    assertEquals(0.25, result, 1e-9);
  }

  @Test
  void equals_noFieldStat_fallsBackToGuess() {
    TableStatistic stat = TableStatistic.fromFields(100L, Map.of());

    TableScan scan = mockScan(List.of("missing"), List.of(STRING));
    RexNode predicate =
        REX.makeCall(SqlStdOperatorTable.EQUALS, REX.makeInputRef(STRING, 0), REX.makeLiteral("X"));

    Double result =
        new TableStatisticSelectivityHandler(stat).getSelectivity(scan, mockMq(), predicate);

    // Calcite's guessSelectivity for EQUALS is 0.15 — fallback path exercised.
    assertEquals(0.15, result, 1e-9);
  }

  @Test
  void equals_nonLiteralRhs_fallsBackToGuess() {
    TableStatistic stat =
        TableStatistic.fromFields(
            100L, Map.of("a", new FieldStatistic("long", 5L, null, null, List.of(), 0.0)));

    TableScan scan = mockScan(List.of("a", "b"), List.of(INT, INT));
    // col_a = col_b — no literal, so per-column stat can't be applied.
    RexNode predicate =
        REX.makeCall(
            SqlStdOperatorTable.EQUALS, REX.makeInputRef(INT, 0), REX.makeInputRef(INT, 1));

    Double result =
        new TableStatisticSelectivityHandler(stat).getSelectivity(scan, mockMq(), predicate);

    assertEquals(0.15, result, 1e-9);
  }

  // ---- null / not-null ----

  @Test
  void isNull_usesNullRatio() {
    TableStatistic stat =
        TableStatistic.fromFields(
            200L, Map.of("status", new FieldStatistic("keyword", 5L, null, null, List.of(), 0.25)));

    TableScan scan = mockScan(List.of("status"), List.of(STRING));
    RexNode predicate = REX.makeCall(SqlStdOperatorTable.IS_NULL, REX.makeInputRef(STRING, 0));

    Double result =
        new TableStatisticSelectivityHandler(stat).getSelectivity(scan, mockMq(), predicate);

    assertEquals(0.25, result, 1e-9);
  }

  @Test
  void isNotNull_usesOneMinusNullRatio() {
    TableStatistic stat =
        TableStatistic.fromFields(
            200L, Map.of("status", new FieldStatistic("keyword", 5L, null, null, List.of(), 0.25)));

    TableScan scan = mockScan(List.of("status"), List.of(STRING));
    RexNode predicate = REX.makeCall(SqlStdOperatorTable.IS_NOT_NULL, REX.makeInputRef(STRING, 0));

    Double result =
        new TableStatisticSelectivityHandler(stat).getSelectivity(scan, mockMq(), predicate);

    assertEquals(0.75, result, 1e-9);
  }

  // ---- AND combining ----

  @Test
  void and_multipliesStatConjuncts() {
    TableStatistic stat =
        TableStatistic.fromFields(
            1000L,
            Map.of(
                "status", new FieldStatistic("keyword", 5L, null, null, List.of(), 0.0),
                "region", new FieldStatistic("keyword", 4L, null, null, List.of(), 0.0)));

    TableScan scan = mockScan(List.of("status", "region"), List.of(STRING, STRING));
    RexNode predicate =
        REX.makeCall(
            SqlStdOperatorTable.AND,
            REX.makeCall(
                SqlStdOperatorTable.EQUALS, REX.makeInputRef(STRING, 0), REX.makeLiteral("OK")),
            REX.makeCall(
                SqlStdOperatorTable.EQUALS, REX.makeInputRef(STRING, 1), REX.makeLiteral("us")));

    Double result =
        new TableStatisticSelectivityHandler(stat).getSelectivity(scan, mockMq(), predicate);

    // 1/5 * 1/4 = 0.05
    assertEquals(0.05, result, 1e-9);
  }

  @Test
  void and_mixesStatAndGuess() {
    TableStatistic stat =
        TableStatistic.fromFields(
            1000L, Map.of("status", new FieldStatistic("keyword", 5L, null, null, List.of(), 0.0)));

    TableScan scan = mockScan(List.of("status", "latency"), List.of(STRING, INT));
    // col=lit (stat: 0.2) AND col > lit (no handler support → guess 0.5)
    RexNode predicate =
        REX.makeCall(
            SqlStdOperatorTable.AND,
            REX.makeCall(
                SqlStdOperatorTable.EQUALS, REX.makeInputRef(STRING, 0), REX.makeLiteral("OK")),
            REX.makeCall(
                SqlStdOperatorTable.GREATER_THAN,
                REX.makeInputRef(INT, 1),
                REX.makeExactLiteral(new java.math.BigDecimal(100))));

    Double result =
        new TableStatisticSelectivityHandler(stat).getSelectivity(scan, mockMq(), predicate);

    // 0.2 * 0.5 = 0.1
    assertEquals(0.1, result, 1e-9);
  }

  // ---- edge cases ----

  @Test
  void nullPredicate_returnsOne() {
    TableStatistic stat = TableStatistic.fromFields(100L, Map.of());
    TableScan scan = mockScan(List.of("status"), List.of(STRING));

    Double result = new TableStatisticSelectivityHandler(stat).getSelectivity(scan, mockMq(), null);

    assertEquals(1.0, result, 1e-9);
  }

  @Test
  void alwaysTruePredicate_returnsOne() {
    TableStatistic stat = TableStatistic.fromFields(100L, Map.of());
    TableScan scan = mockScan(List.of("status"), List.of(STRING));
    RexNode predicate = REX.makeLiteral(true);

    Double result =
        new TableStatisticSelectivityHandler(stat).getSelectivity(scan, mockMq(), predicate);

    assertEquals(1.0, result, 1e-9);
  }

  @Test
  void nonTableScan_returnsNull() {
    TableStatistic stat =
        TableStatistic.fromFields(
            100L, Map.of("status", new FieldStatistic("keyword", 5L, null, null, List.of(), 0.0)));
    RelNode nonScan = mock(RelNode.class);
    RexNode predicate =
        REX.makeCall(
            SqlStdOperatorTable.EQUALS, REX.makeInputRef(STRING, 0), REX.makeLiteral("OK"));

    Double result =
        new TableStatisticSelectivityHandler(stat).getSelectivity(nonScan, mockMq(), predicate);

    assertNull(result);
  }

  @Test
  void isNull_withNullRatioZero_returnsZero() {
    TableStatistic stat =
        TableStatistic.fromFields(
            100L, Map.of("status", new FieldStatistic("keyword", 5L, null, null, List.of(), 0.0)));
    TableScan scan = mockScan(List.of("status"), List.of(STRING));
    RexNode predicate = REX.makeCall(SqlStdOperatorTable.IS_NULL, REX.makeInputRef(STRING, 0));

    Double result =
        new TableStatisticSelectivityHandler(stat).getSelectivity(scan, mockMq(), predicate);

    assertEquals(0.0, result, 1e-9);
  }

  // ---- helpers ----

  private static TableScan mockScan(List<String> fieldNames, List<RelDataType> fieldTypes) {
    if (fieldNames.size() != fieldTypes.size()) {
      throw new IllegalArgumentException("mockScan: names and types must line up");
    }
    TableScan scan = mock(TableScan.class);
    RelOptTable table = mock(RelOptTable.class);
    RelDataType rowType = mock(RelDataType.class);
    lenient().when(scan.getTable()).thenReturn(table);
    lenient().when(scan.getRowType()).thenReturn(rowType);
    lenient().when(rowType.getFieldList()).thenReturn(new FieldList(fieldNames, fieldTypes));
    return scan;
  }

  private static RelMetadataQuery mockMq() {
    return mock(RelMetadataQuery.class);
  }

  private static class FieldList extends AbstractList<RelDataTypeField> {
    private final List<RelDataTypeField> fields;

    FieldList(List<String> names, List<RelDataType> types) {
      ImmutableList.Builder<RelDataTypeField> builder = ImmutableList.builder();
      for (int i = 0; i < names.size(); i++) {
        builder.add(new RelDataTypeFieldImpl(names.get(i), i, types.get(i)));
      }
      this.fields = builder.build();
    }

    @Override
    public RelDataTypeField get(int index) {
      return fields.get(index);
    }

    @Override
    public int size() {
      return fields.size();
    }
  }
}
