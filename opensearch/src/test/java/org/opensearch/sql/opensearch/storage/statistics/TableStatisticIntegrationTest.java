/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.statistics;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.*;

import java.util.AbstractList;
import java.util.Map;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Statistics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;

@ExtendWith(MockitoExtension.class)
class TableStatisticIntegrationTest {

  @Mock private RelOptCluster cluster;
  @Mock private RelOptTable table;
  @Mock private OpenSearchIndex osIndex;
  @Mock private RelOptPlanner planner;
  @Mock private RelMetadataQuery mq;

  @BeforeEach
  void setUp() {
    RelTraitSet traitSet = mock(RelTraitSet.class);
    when(cluster.traitSetOf(any(Convention.class))).thenReturn(traitSet);
    lenient().when(osIndex.getMaxResultWindow()).thenReturn(10_000);
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
            inv -> {
              RelOptCost cost = mock(RelOptCost.class);
              when(cost.getRows()).thenReturn((Double) inv.getArguments()[0]);
              return cost;
            });
  }

  @Test
  void costDiffers_withAndWithoutTableStats() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(5));
    lenient().when(table.getRowType()).thenReturn(relDataType);

    // Without table stats: baseline = maxResultWindow (10,000)
    when(osIndex.getStatistic()).thenReturn(Statistics.UNKNOWN);
    CalciteLogicalIndexScan scanWithout = new CalciteLogicalIndexScan(cluster, table, osIndex);
    double costWithout = scanWithout.computeSelfCost(planner, mq).getRows();

    // With table stats: baseline = 1,000,000
    TableStatistic stat = TableStatistic.fromFields(1_000_000L, Map.of());
    when(osIndex.getStatistic()).thenReturn(stat);
    CalciteLogicalIndexScan scanWith = new CalciteLogicalIndexScan(cluster, table, osIndex);
    double costWith = scanWith.computeSelfCost(planner, mq).getRows();

    // Cost with real stats should be 100x larger (1M vs 10K baseline)
    assertTrue(
        costWith > costWithout * 50,
        "Cost with table stats (%s) should be much larger than without (%s)"
            .formatted(costWith, costWithout));
  }

  @Test
  void rowCountEstimate_moreAccurate_withTableStats() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(table.getRowType()).thenReturn(relDataType);

    TableStatistic stat = TableStatistic.fromFields(2_000_000L, Map.of());
    when(osIndex.getStatistic()).thenReturn(stat);
    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);

    assertEquals(2_000_000.0, scan.estimateRowCount(mq));
  }

  static class MockFieldList extends AbstractList<RelDataTypeField> {
    private final int size;

    MockFieldList(int size) {
      this.size = size;
    }

    @Override
    public RelDataTypeField get(int index) {
      return mock(RelDataTypeField.class);
    }

    @Override
    public int size() {
      return size;
    }
  }
}
