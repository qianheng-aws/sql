/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.util.AbstractList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.context.AggSpec;
import org.opensearch.sql.opensearch.storage.scan.context.FilterDigest;
import org.opensearch.sql.opensearch.storage.scan.context.LimitDigest;
import org.opensearch.sql.opensearch.storage.scan.context.OSRequestBuilderAction;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownOperation;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownType;
import org.opensearch.sql.opensearch.storage.scan.context.RareTopDigest;
import org.opensearch.sql.opensearch.storage.statistics.FieldStatistic;
import org.opensearch.sql.opensearch.storage.statistics.TableStatistic;
import org.opensearch.sql.opensearch.storage.statistics.TableStatisticDistinctRowCountHandler;
import org.opensearch.sql.opensearch.storage.statistics.TableStatisticSelectivityHandler;

@ExtendWith(MockitoExtension.class)
public class CalciteIndexScanCostTest {
  static final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  private static final OSRequestBuilderAction NO_OP_ACTION = req -> {};
  final RexBuilder builder = new RexBuilder(typeFactory);

  @Mock private static RelOptCluster cluster;
  @Mock private static RelOptTable table;
  @Mock private static OpenSearchIndex osIndex;
  @Mock private static RelOptPlanner planner;
  @Mock private static RelMetadataQuery mq;

  @BeforeEach
  void setUp() {
    RelTraitSet traitSet = mock(RelTraitSet.class);
    when(cluster.traitSetOf(any(Convention.class))).thenReturn(traitSet);
    lenient().when(osIndex.getMaxResultWindow()).thenReturn(10000);
    Settings settings = mock(Settings.class);
    lenient()
        .when(settings.getSettingValue(Key.CALCITE_PUSHDOWN_ROWCOUNT_ESTIMATION_FACTOR))
        .thenReturn(0.9);
    lenient().when(osIndex.getSettings()).thenReturn(settings);

    RelOptCostFactory costFactory = mock(RelOptCostFactory.class);
    lenient().when(planner.getCostFactory()).thenReturn(costFactory);
    lenient()
        .when(costFactory.makeCost(anyDouble(), anyDouble(), anyDouble()))
        .thenAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              RelOptCost optCost = mock(RelOptCost.class);
              when(optCost.getRows()).thenReturn((Double) args[0]);
              return optCost;
            });
  }

  @Test
  void test_cost_on_non_pushdown() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
    lenient().when(table.getRowType()).thenReturn(relDataType);
    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);
    assertEquals(90000, scan.computeSelfCost(planner, mq).getRows());
  }

  @Test
  void test_cost_on_project_pushdown() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(table.getRowType()).thenReturn(relDataType);
    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);

    List<String> projectDigest = List.of("A");
    scan.getPushDownContext()
        .add(
            new PushDownOperation(
                PushDownType.PROJECT, projectDigest, (OSRequestBuilderAction) req -> {}));
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(projectDigest.size()));
    assertEquals(9000, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());

    projectDigest = List.of("A", "B", "C");
    scan.getPushDownContext()
        .add(
            new PushDownOperation(
                PushDownType.PROJECT, projectDigest, (OSRequestBuilderAction) req -> {}));
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(projectDigest.size()));
    assertEquals(27000, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());
  }

  @Test
  void test_cost_on_limit_pushdown() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
    lenient().when(table.getRowType()).thenReturn(relDataType);

    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);

    LimitDigest limitDigest = new LimitDigest(100, 0);
    scan.getPushDownContext()
        .add(
            new PushDownOperation(
                PushDownType.LIMIT, limitDigest, (OSRequestBuilderAction) req -> {}));
    assertEquals(891, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());
  }

  @Test
  void test_cost_on_filter_pushdown() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
    lenient().when(table.getRowType()).thenReturn(relDataType);

    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);

    RexNode condition =
        builder.makeCall(
            SqlStdOperatorTable.EQUALS,
            builder.makeInputRef(scan, 0),
            builder.makeLiteral("Hello"));
    FilterDigest filterDigest = new FilterDigest(0, condition);
    scan.getPushDownContext()
        .add(
            new PushDownOperation(
                PushDownType.FILTER, filterDigest, (OSRequestBuilderAction) req -> {}));
    assertEquals(13500, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());
  }

  @Test
  void test_cost_on_filter_script_pushdown() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
    lenient().when(table.getRowType()).thenReturn(relDataType);

    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);

    RexNode condition =
        builder.makeCall(
            SqlStdOperatorTable.EQUALS,
            builder.makeInputRef(scan, 0),
            builder.makeLiteral("Hello"));
    FilterDigest filterDigest = new FilterDigest(1, condition);
    scan.getPushDownContext()
        .add(
            new PushDownOperation(
                PushDownType.SCRIPT, filterDigest, (OSRequestBuilderAction) req -> {}));
    assertEquals(14985, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());
  }

  @Test
  void test_cost_on_sort_pushdown() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
    lenient().when(table.getRowType()).thenReturn(relDataType);

    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);
    scan.getPushDownContext()
        .add(new PushDownOperation(PushDownType.SORT, null, (OSRequestBuilderAction) req -> {}));
    assertEquals(99000, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());
  }

  @Test
  void test_cost_on_aggregate_pushdown() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
    lenient().when(relDataType.getFieldCount()).thenReturn(10);
    lenient().when(cluster.getTypeFactory()).thenReturn(typeFactory);
    lenient().when(table.getRowType()).thenReturn(relDataType);

    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);
    Aggregate aggregate =
        new LogicalAggregate(
            cluster,
            cluster.traitSetOf(Convention.NONE),
            List.of(),
            scan,
            ImmutableBitSet.of(0),
            null,
            List.of());
    when(mq.getRowCount(aggregate)).thenReturn(1000d);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(1));
    lenient().when(relDataType.getFieldCount()).thenReturn(1);
    lenient().when(table.getRowType()).thenReturn(relDataType);

    scan.getPushDownContext()
        .add(new PushDownOperation(PushDownType.AGGREGATION, aggregate, NO_OP_ACTION));
    assertEquals(1800, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());
  }

  @Test
  void test_cost_on_aggregate_pushdown_with_one_aggCall() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
    lenient().when(relDataType.getFieldCount()).thenReturn(10);
    lenient().when(cluster.getTypeFactory()).thenReturn(typeFactory);
    lenient().when(table.getRowType()).thenReturn(relDataType);

    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);
    AggregateCall countCall =
        AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            ImmutableList.of(),
            ImmutableList.of(),
            -1,
            null,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt");
    Aggregate aggregate =
        new LogicalAggregate(
            cluster,
            cluster.traitSetOf(Convention.NONE),
            List.of(),
            scan,
            ImmutableBitSet.of(0),
            null,
            List.of(countCall));
    when(mq.getRowCount(aggregate)).thenReturn(1000d);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(2));
    lenient().when(relDataType.getFieldCount()).thenReturn(2);
    lenient().when(table.getRowType()).thenReturn(relDataType);

    scan.getPushDownContext()
        .add(new PushDownOperation(PushDownType.AGGREGATION, aggregate, NO_OP_ACTION));
    assertEquals(2812.5, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());
  }

  @Test
  void test_cost_on_aggregate_pushdown_with_two_aggCall() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
    lenient().when(relDataType.getFieldCount()).thenReturn(10);
    lenient().when(cluster.getTypeFactory()).thenReturn(typeFactory);
    lenient().when(table.getRowType()).thenReturn(relDataType);

    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);
    AggregateCall countCall =
        AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            ImmutableList.of(),
            ImmutableList.of(),
            -1,
            null,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt");
    AggregateCall sumCall =
        AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            false,
            false,
            ImmutableList.of(),
            ImmutableList.of(1),
            -1,
            null,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "sum");
    Aggregate aggregate =
        new LogicalAggregate(
            cluster,
            cluster.traitSetOf(Convention.NONE),
            List.of(),
            scan,
            ImmutableBitSet.of(0),
            null,
            List.of(countCall, sumCall));
    when(mq.getRowCount(aggregate)).thenReturn(1000d);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(3));
    lenient().when(relDataType.getFieldCount()).thenReturn(3);
    lenient().when(table.getRowType()).thenReturn(relDataType);

    scan.getPushDownContext()
        .add(new PushDownOperation(PushDownType.AGGREGATION, aggregate, NO_OP_ACTION));
    assertEquals(
        3836.2500429153442, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());
  }

  @Test
  void test_cost_on_aggregate_pushdown_with_one_aggCall_with_script() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
    lenient().when(relDataType.getFieldCount()).thenReturn(10);
    lenient().when(cluster.getTypeFactory()).thenReturn(typeFactory);
    lenient().when(table.getRowType()).thenReturn(relDataType);

    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);
    AggSpec aggSpec = mock(AggSpec.class);
    when(aggSpec.getScriptCount()).thenReturn(1L);
    AggregateCall countCall =
        AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            ImmutableList.of(),
            ImmutableList.of(),
            -1,
            null,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt");
    Aggregate aggregate =
        new LogicalAggregate(
            cluster,
            cluster.traitSetOf(Convention.NONE),
            List.of(),
            scan,
            ImmutableBitSet.of(0),
            null,
            List.of(countCall));
    when(mq.getRowCount(aggregate)).thenReturn(1000d);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(2));
    lenient().when(relDataType.getFieldCount()).thenReturn(2);
    lenient().when(table.getRowType()).thenReturn(relDataType);
    scan.getPushDownContext().setAggSpec(aggSpec);

    scan.getPushDownContext()
        .add(new PushDownOperation(PushDownType.AGGREGATION, aggregate, NO_OP_ACTION));
    assertEquals(
        2913.7500643730164, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());
  }

  @Test
  void test_cost_on_highlight_pushdown() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
    lenient().when(table.getRowType()).thenReturn(relDataType);
    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);

    List<String> highlightArgs = List.of("*");
    scan.getPushDownContext()
        .add(
            new PushDownOperation(
                PushDownType.HIGHLIGHT, highlightArgs, (OSRequestBuilderAction) req -> {}));
    // Highlight should not change cost compared to non-pushdown (same as PROJECT behavior)
    assertEquals(90000, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());
  }

  @Test
  void test_cost_on_project_limit_pushdown() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(table.getRowType()).thenReturn(relDataType);
    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);

    List<String> projectDigest = List.of("A");
    scan.getPushDownContext()
        .add(
            new PushDownOperation(
                PushDownType.PROJECT, projectDigest, (OSRequestBuilderAction) req -> {}));
    LimitDigest limitDigest = new LimitDigest(100, 0);
    scan.getPushDownContext()
        .add(
            new PushDownOperation(
                PushDownType.LIMIT, limitDigest, (OSRequestBuilderAction) req -> {}));
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(projectDigest.size()));
    assertEquals(
        89.10000000000001, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());

    // Reverse the push down sequence won't change the cost
    scan = new CalciteLogicalIndexScan(cluster, table, osIndex);
    scan.getPushDownContext()
        .add(
            new PushDownOperation(
                PushDownType.LIMIT, limitDigest, (OSRequestBuilderAction) req -> {}));
    scan.getPushDownContext()
        .add(
            new PushDownOperation(
                PushDownType.PROJECT, projectDigest, (OSRequestBuilderAction) req -> {}));
    assertEquals(
        89.10000000000001, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());
  }

  @Test
  void test_cost_on_multi_operator_pushdown_without_agg() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
    lenient().when(table.getRowType()).thenReturn(relDataType);
    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);

    List<String> projectDigest = List.of("A", "B");
    scan.getPushDownContext()
        .add(
            new PushDownOperation(
                PushDownType.PROJECT, projectDigest, (OSRequestBuilderAction) req -> {}));
    RexNode condition =
        builder.makeCall(
            SqlStdOperatorTable.EQUALS,
            builder.makeInputRef(scan, 0),
            builder.makeLiteral("Hello"));
    FilterDigest filterDigest = new FilterDigest(0, condition);
    scan.getPushDownContext()
        .add(
            new PushDownOperation(
                PushDownType.FILTER, filterDigest, (OSRequestBuilderAction) req -> {}));
    scan.getPushDownContext()
        .add(new PushDownOperation(PushDownType.SORT, null, (OSRequestBuilderAction) req -> {}));
    LimitDigest limitDigest = new LimitDigest(100, 0);
    scan.getPushDownContext()
        .add(
            new PushDownOperation(
                PushDownType.LIMIT, limitDigest, (OSRequestBuilderAction) req -> {}));
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(projectDigest.size()));
    assertEquals(1528.2, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());
  }

  @Test
  void test_cost_on_aggregate_pushdown_along_with_others() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
    lenient().when(relDataType.getFieldCount()).thenReturn(10);
    lenient().when(cluster.getTypeFactory()).thenReturn(typeFactory);
    lenient().when(table.getRowType()).thenReturn(relDataType);

    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);
    AggSpec aggSpec = mock(AggSpec.class);
    when(aggSpec.getScriptCount()).thenReturn(1L);
    AggregateCall countCall =
        AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            ImmutableList.of(),
            ImmutableList.of(),
            -1,
            null,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt");
    Aggregate aggregate =
        new LogicalAggregate(
            cluster,
            cluster.traitSetOf(Convention.NONE),
            List.of(),
            scan,
            ImmutableBitSet.of(0),
            null,
            List.of(countCall));
    when(mq.getRowCount(aggregate)).thenReturn(1000d);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(2));
    lenient().when(relDataType.getFieldCount()).thenReturn(2);
    lenient().when(table.getRowType()).thenReturn(relDataType);
    scan.getPushDownContext().setAggSpec(aggSpec);

    List<String> projectDigest1 = List.of("A", "B");
    scan.getPushDownContext()
        .add(
            new PushDownOperation(
                PushDownType.PROJECT, projectDigest1, (OSRequestBuilderAction) req -> {}));
    scan.getPushDownContext()
        .add(new PushDownOperation(PushDownType.AGGREGATION, aggregate, NO_OP_ACTION));
    List<String> projectDigest2 = List.of("COUNT");
    scan.getPushDownContext()
        .add(new PushDownOperation(PushDownType.PROJECT, projectDigest2, NO_OP_ACTION));
    scan.getPushDownContext()
        .add(new PushDownOperation(PushDownType.SORT, null, (OSRequestBuilderAction) req -> {}));
    LimitDigest limitDigest = new LimitDigest(100, 0);
    scan.getPushDownContext()
        .add(new PushDownOperation(PushDownType.LIMIT, limitDigest, NO_OP_ACTION));
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(projectDigest2.size()));
    assertEquals(
        2102.8500643730163, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());
  }

  @Test
  void test_cost_with_table_statistic_baseline() {
    TableStatistic stat = TableStatistic.fromFields(500_000L, Map.of());
    when(osIndex.getStatistic()).thenReturn(stat);

    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
    lenient().when(table.getRowType()).thenReturn(relDataType);
    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);

    // Cost should use 500,000 as baseline instead of 10,000 (maxResultWindow)
    // non-pushdown cost = rows * fields * factor = 500,000 * 10 * 0.9 = 4,500,000
    assertEquals(4_500_000, scan.computeSelfCost(planner, mq).getRows());
  }

  @Test
  void test_estimateRowCount_with_table_statistic() {
    TableStatistic stat = TableStatistic.fromFields(500_000L, Map.of());
    when(osIndex.getStatistic()).thenReturn(stat);

    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(table.getRowType()).thenReturn(relDataType);
    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);

    assertEquals(500_000.0, scan.estimateRowCount(mq));
  }

  @Test
  void test_rareTop_rowCount_no_by_clamps_to_rowCount() {
    // baseline 3 rows, N=100, no by-columns → min(100, 3) = 3
    TableStatistic stat = TableStatistic.fromFields(3L, Map.of());
    when(osIndex.getStatistic()).thenReturn(stat);
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(table.getRowType()).thenReturn(relDataType);
    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);

    RareTopDigest digest = new RareTopDigest("foo", List.of(), 100, Direction.DESCENDING);
    scan.getPushDownContext()
        .add(new PushDownOperation(PushDownType.RARE_TOP, digest, NO_OP_ACTION));

    assertEquals(3.0, scan.estimateRowCount(mq));
  }

  @Test
  void test_rareTop_rowCount_single_by_uses_cardinality() {
    // baseline 1000, N=3, by status (cardinality=5) → min(3 * 5, 1000) = 15
    FieldStatistic statusStat = new FieldStatistic("keyword", 5L, null, null, List.of(), 0.0);
    TableStatistic stat = TableStatistic.fromFields(1000L, Map.of("status", statusStat));
    when(osIndex.getStatistic()).thenReturn(stat);
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(table.getRowType()).thenReturn(relDataType);
    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);

    RareTopDigest digest = new RareTopDigest("latency", List.of("status"), 3, Direction.DESCENDING);
    scan.getPushDownContext()
        .add(new PushDownOperation(PushDownType.RARE_TOP, digest, NO_OP_ACTION));

    assertEquals(15.0, scan.estimateRowCount(mq));
  }

  @Test
  void test_rareTop_rowCount_multi_by_uses_numDistinctVals() {
    // baseline 1000, N=2, by status(5) × region(4) = 20 raw product.
    // numDistinctVals(20, 1000) ≈ 19.80 (Calcite's inclusion-exclusion formula),
    // then emitted = min(2 * 19.80, 1000) ≈ 39.60.
    FieldStatistic statusStat = new FieldStatistic("keyword", 5L, null, null, List.of(), 0.0);
    FieldStatistic regionStat = new FieldStatistic("keyword", 4L, null, null, List.of(), 0.0);
    TableStatistic stat =
        TableStatistic.fromFields(1000L, Map.of("status", statusStat, "region", regionStat));
    when(osIndex.getStatistic()).thenReturn(stat);
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(table.getRowType()).thenReturn(relDataType);
    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);

    RareTopDigest digest =
        new RareTopDigest("latency", List.of("status", "region"), 2, Direction.DESCENDING);
    scan.getPushDownContext()
        .add(new PushDownOperation(PushDownType.RARE_TOP, digest, NO_OP_ACTION));

    double expected = 2 * RelMdUtil.numDistinctVals(20.0, 1000.0);
    assertEquals(expected, scan.estimateRowCount(mq));
  }

  @Test
  void test_rareTop_rowCount_missing_stats_falls_back_to_heuristic() {
    // byList has "status" but stat for it is missing → fallback to N * rowCount * (1 - 0.5^G)
    TableStatistic stat = TableStatistic.fromFields(1000L, Map.of());
    when(osIndex.getStatistic()).thenReturn(stat);
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(table.getRowType()).thenReturn(relDataType);
    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);

    RareTopDigest digest = new RareTopDigest("latency", List.of("status"), 3, Direction.DESCENDING);
    scan.getPushDownContext()
        .add(new PushDownOperation(PushDownType.RARE_TOP, digest, NO_OP_ACTION));

    // heuristic: 3 * 1000 * (1 - 0.5) = 1500
    assertEquals(1500.0, scan.estimateRowCount(mq));
  }

  @Test
  void test_filter_pushdown_uses_stat_aware_selectivity_when_handler_available() {
    // With a TableStatistic wired and a Selectivity.Handler exposed via unwrap, the FILTER
    // branch of estimateRowCount must route through the handler (`1/cardinality`) instead of
    // RelMdUtil.guessSelectivity (default EQUALS = 0.15).
    // Input field at index 0 is "dummy" (see MockFieldList). Store the stat under that name so
    // the handler resolves the InputRef -> column properly.
    FieldStatistic dummyStat = new FieldStatistic("keyword", 5L, null, null, List.of(), 0.0);
    TableStatistic stat = TableStatistic.fromFields(2000L, Map.of("dummy", dummyStat));
    when(osIndex.getStatistic()).thenReturn(stat);
    when(table.unwrap(org.apache.calcite.rel.metadata.BuiltInMetadata.Selectivity.Handler.class))
        .thenReturn(new TableStatisticSelectivityHandler(stat));

    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
    lenient().when(table.getRowType()).thenReturn(relDataType);
    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);

    RexNode cond =
        builder.makeCall(
            SqlStdOperatorTable.EQUALS, builder.makeInputRef(scan, 0), builder.makeLiteral("OK"));
    FilterDigest filterDigest = new FilterDigest(0, cond);
    scan.getPushDownContext()
        .add(new PushDownOperation(PushDownType.FILTER, filterDigest, NO_OP_ACTION));

    // 2000 × 1/5 = 400 (stat-aware), not 2000 × 0.15 = 300 (guessSelectivity)
    assertEquals(400.0, scan.estimateRowCount(mq), 0.001);
  }

  @Test
  void test_aggregate_pushdown_uses_distinct_row_count_handler_when_available() {
    // Stat-backed: status.cardinality = 5, rowCount = 2000. Aggregate group-by-status should
    // emit 5 rows (min(cardinality, rowCount)), NOT the default inputRowCount/10 = 200.
    FieldStatistic dummyStat = new FieldStatistic("keyword", 5L, null, null, List.of(), 0.0);
    TableStatistic stat = TableStatistic.fromFields(2000L, Map.of("dummy", dummyStat));
    when(osIndex.getStatistic()).thenReturn(stat);
    when(table.unwrap(
            org.apache.calcite.rel.metadata.BuiltInMetadata.DistinctRowCount.Handler.class))
        .thenReturn(new TableStatisticDistinctRowCountHandler(stat));

    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
    lenient().when(relDataType.getFieldCount()).thenReturn(10);
    lenient().when(table.getRowType()).thenReturn(relDataType);
    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);

    Aggregate aggregate =
        new LogicalAggregate(
            cluster,
            cluster.traitSetOf(Convention.NONE),
            List.of(),
            scan,
            ImmutableBitSet.of(0),
            null,
            List.of());
    scan.getPushDownContext()
        .add(new PushDownOperation(PushDownType.AGGREGATION, aggregate, NO_OP_ACTION));

    assertEquals(5.0, scan.estimateRowCount(mq), 0.001);
  }

  @Test
  void test_cost_fallback_when_no_table_statistic() {
    // getStatistic returns default Statistics.UNKNOWN (not TableStatistic)
    when(osIndex.getStatistic()).thenReturn(Statistics.UNKNOWN);

    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
    lenient().when(table.getRowType()).thenReturn(relDataType);
    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);

    // Falls back to maxResultWindow = 10,000
    assertEquals(90_000, scan.computeSelfCost(planner, mq).getRows());
  }

  private static class MockFieldList extends AbstractList<RelDataTypeField> {
    private final int size;

    public MockFieldList(int size) {
      this.size = size;
    }

    @Override
    public RelDataTypeField get(int index) {
      RelDataType type =
          typeFactory.createSqlType(index == 0 ? SqlTypeName.VARCHAR : SqlTypeName.BIGINT);
      return new RelDataTypeFieldImpl("dummy", index, type);
    }

    @Override
    public int size() {
      return size;
    }
  }
}
