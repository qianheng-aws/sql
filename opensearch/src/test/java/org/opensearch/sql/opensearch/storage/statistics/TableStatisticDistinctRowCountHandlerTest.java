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
import static org.mockito.Mockito.when;

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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.Test;

class TableStatisticDistinctRowCountHandlerTest {

  private static final RelDataTypeFactory TYPE_FACTORY =
      new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  @Test
  void singleColumn_returnsStoredCardinality() {
    TableStatistic stat =
        TableStatistic.fromFields(
            137L,
            Map.of(
                "status",
                new FieldStatistic("keyword", 5L, null, null, List.of(), 0.0),
                "latency",
                new FieldStatistic("long", 132L, 7, 996, List.of(), 0.0)));

    TableScan scan = mockScan(List.of("status", "latency"));

    Double result =
        new TableStatisticDistinctRowCountHandler(stat)
            .getDistinctRowCount(scan, mockMq(), ImmutableBitSet.of(0), null);

    assertNotNull(result);
    assertEquals(5.0, result);
  }

  @Test
  void singleColumn_capsAtRowCount() {
    TableStatistic stat =
        TableStatistic.fromFields(
            10L,
            Map.of(
                "status",
                // Pathological: stored cardinality exceeds the table row count.
                new FieldStatistic("keyword", 999L, null, null, List.of(), 0.0)));

    TableScan scan = mockScan(List.of("status"));

    Double result =
        new TableStatisticDistinctRowCountHandler(stat)
            .getDistinctRowCount(scan, mockMq(), ImmutableBitSet.of(0), null);

    assertEquals(10.0, result);
  }

  @Test
  void multiColumn_usesNumDistinctVals() {
    TableStatistic stat =
        TableStatistic.fromFields(
            1_000L,
            Map.of(
                "status",
                new FieldStatistic("keyword", 5L, null, null, List.of(), 0.0),
                "region",
                new FieldStatistic("keyword", 4L, null, null, List.of(), 0.0)));

    TableScan scan = mockScan(List.of("status", "region"));

    Double result =
        new TableStatisticDistinctRowCountHandler(stat)
            .getDistinctRowCount(scan, mockMq(), ImmutableBitSet.of(0, 1), null);

    // product = 20, rowCount = 1000 — numDistinctVals clamps to ~20.
    assertNotNull(result);
    assertEquals(20.0, result, 0.5);
  }

  @Test
  void emptyGroupKey_returnsNull() {
    TableStatistic stat =
        TableStatistic.fromFields(
            100L, Map.of("status", new FieldStatistic("keyword", 5L, null, null, List.of(), 0.0)));

    TableScan scan = mockScan(List.of("status"));

    Double result =
        new TableStatisticDistinctRowCountHandler(stat)
            .getDistinctRowCount(scan, mockMq(), ImmutableBitSet.of(), null);

    assertNull(result);
  }

  @Test
  void nonNullPredicate_returnsNull() {
    TableStatistic stat =
        TableStatistic.fromFields(
            100L, Map.of("status", new FieldStatistic("keyword", 5L, null, null, List.of(), 0.0)));

    TableScan scan = mockScan(List.of("status"));
    RexNode predicate = mock(RexNode.class);
    when(predicate.isAlwaysTrue()).thenReturn(false);

    Double result =
        new TableStatisticDistinctRowCountHandler(stat)
            .getDistinctRowCount(scan, mockMq(), ImmutableBitSet.of(0), predicate);

    assertNull(result);
  }

  @Test
  void missingFieldStatistic_returnsNull() {
    TableStatistic stat =
        TableStatistic.fromFields(
            100L, Map.of("status", new FieldStatistic("keyword", 5L, null, null, List.of(), 0.0)));

    // Group key references "region" which has no stored stat.
    TableScan scan = mockScan(List.of("status", "region"));

    Double result =
        new TableStatisticDistinctRowCountHandler(stat)
            .getDistinctRowCount(scan, mockMq(), ImmutableBitSet.of(1), null);

    assertNull(result);
  }

  @Test
  void zeroCardinality_returnsNull() {
    TableStatistic stat =
        TableStatistic.fromFields(
            100L, Map.of("status", new FieldStatistic("keyword", 0L, null, null, List.of(), 0.0)));

    TableScan scan = mockScan(List.of("status"));

    Double result =
        new TableStatisticDistinctRowCountHandler(stat)
            .getDistinctRowCount(scan, mockMq(), ImmutableBitSet.of(0), null);

    assertNull(result);
  }

  @Test
  void zeroRowCount_returnsNull() {
    TableStatistic stat =
        TableStatistic.fromFields(
            0L, Map.of("status", new FieldStatistic("keyword", 5L, null, null, List.of(), 0.0)));

    TableScan scan = mockScan(List.of("status"));

    Double result =
        new TableStatisticDistinctRowCountHandler(stat)
            .getDistinctRowCount(scan, mockMq(), ImmutableBitSet.of(0), null);

    assertNull(result);
  }

  @Test
  void nonTableScan_returnsNull() {
    TableStatistic stat =
        TableStatistic.fromFields(
            100L, Map.of("status", new FieldStatistic("keyword", 5L, null, null, List.of(), 0.0)));

    RelNode nonScan = mock(RelNode.class);

    Double result =
        new TableStatisticDistinctRowCountHandler(stat)
            .getDistinctRowCount(nonScan, mockMq(), ImmutableBitSet.of(0), null);

    assertNull(result);
  }

  private static TableScan mockScan(List<String> fieldNames) {
    TableScan scan = mock(TableScan.class);
    RelOptTable table = mock(RelOptTable.class);
    RelDataType rowType = mock(RelDataType.class);
    lenient().when(scan.getTable()).thenReturn(table);
    lenient().when(scan.getRowType()).thenReturn(rowType);
    lenient().when(rowType.getFieldList()).thenReturn(new FieldList(fieldNames));
    return scan;
  }

  private static RelMetadataQuery mockMq() {
    return mock(RelMetadataQuery.class);
  }

  private static class FieldList extends AbstractList<RelDataTypeField> {
    private final List<String> names;

    FieldList(List<String> names) {
      this.names = names;
    }

    @Override
    public RelDataTypeField get(int index) {
      return new RelDataTypeFieldImpl(
          names.get(index), index, TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR));
    }

    @Override
    public int size() {
      return names.size();
    }
  }
}
