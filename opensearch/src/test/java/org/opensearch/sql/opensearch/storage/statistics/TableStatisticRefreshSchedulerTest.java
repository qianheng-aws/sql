/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.statistics;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;

class TableStatisticRefreshSchedulerTest {

  private ClusterService clusterService;
  private ClusterSettings clusterSettings;
  private ThreadPool threadPool;
  private OpenSearchSettings settings;
  private TableStatisticStorage storage;
  private TableStatisticCollector collector;
  private TableStatisticsMappingResolver resolver;

  @BeforeEach
  void setUp() {
    clusterService = mock(ClusterService.class);
    clusterSettings = mock(ClusterSettings.class);
    when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
    threadPool = mock(ThreadPool.class);
    settings = mock(OpenSearchSettings.class);
    when(settings.getSettingValue(Key.TABLE_STATISTICS_ENABLED)).thenReturn(true);
    when(settings.getSettingValue(Key.TABLE_STATISTICS_REFRESH_INTERVAL))
        .thenReturn(TimeValue.timeValueSeconds(60));
    when(settings.getSettingValue(Key.TABLE_STATISTICS_TTL))
        .thenReturn(TimeValue.timeValueHours(24));
    when(settings.getSettingValue(Key.TABLE_STATISTICS_REFRESH_MAX_IN_FLIGHT)).thenReturn(4);
    storage = mock(TableStatisticStorage.class);
    collector = mock(TableStatisticCollector.class);
    resolver = mock(TableStatisticsMappingResolver.class);
  }

  @Test
  void onClusterManagerSchedulesCancellable() {
    Cancellable cancellable = mock(Cancellable.class);
    when(threadPool.scheduleWithFixedDelay(any(), any(), any())).thenReturn(cancellable);

    TableStatisticRefreshScheduler scheduler =
        new TableStatisticRefreshScheduler(
            clusterService, threadPool, settings, storage, collector, resolver);
    scheduler.register();
    scheduler.onClusterManager();

    verify(threadPool)
        .scheduleWithFixedDelay(
            any(), eq(TimeValue.timeValueSeconds(60)), eq(ThreadPool.Names.GENERIC));
    assertNotNull(scheduler.currentSemaphore());
  }

  @Test
  void offClusterManagerCancelsCancellable() {
    Cancellable cancellable = mock(Cancellable.class);
    when(threadPool.scheduleWithFixedDelay(any(), any(), any())).thenReturn(cancellable);

    TableStatisticRefreshScheduler scheduler =
        new TableStatisticRefreshScheduler(
            clusterService, threadPool, settings, storage, collector, resolver);
    scheduler.register();
    scheduler.onClusterManager();
    scheduler.offClusterManager();

    verify(cancellable).cancel();
  }

  @Test
  void onClusterManagerDoesNotScheduleWhenFlagOff() {
    when(settings.getSettingValue(Key.TABLE_STATISTICS_ENABLED)).thenReturn(false);

    TableStatisticRefreshScheduler scheduler =
        new TableStatisticRefreshScheduler(
            clusterService, threadPool, settings, storage, collector, resolver);
    scheduler.register();
    scheduler.onClusterManager();

    verify(threadPool, org.mockito.Mockito.never()).scheduleWithFixedDelay(any(), any(), any());
  }

  @Test
  void maxInFlightChangeSwapsSemaphore() {
    TableStatisticRefreshScheduler scheduler =
        new TableStatisticRefreshScheduler(
            clusterService, threadPool, settings, storage, collector, resolver);
    java.util.concurrent.Semaphore before = scheduler.currentSemaphore();
    scheduler.onMaxInFlightChanged(8);
    java.util.concurrent.Semaphore after = scheduler.currentSemaphore();
    org.junit.jupiter.api.Assertions.assertNotSame(before, after);
    org.junit.jupiter.api.Assertions.assertEquals(8, after.availablePermits());
  }

  @Test
  void intervalChangeReschedules() {
    Cancellable first = mock(Cancellable.class);
    Cancellable second = mock(Cancellable.class);
    when(threadPool.scheduleWithFixedDelay(any(), any(), any()))
        .thenReturn(first)
        .thenReturn(second);

    TableStatisticRefreshScheduler scheduler =
        new TableStatisticRefreshScheduler(
            clusterService, threadPool, settings, storage, collector, resolver);
    scheduler.register();
    scheduler.onClusterManager();

    when(settings.getSettingValue(Key.TABLE_STATISTICS_REFRESH_INTERVAL))
        .thenReturn(TimeValue.timeValueSeconds(10));
    scheduler.onIntervalChanged();

    verify(first).cancel();
    verify(threadPool)
        .scheduleWithFixedDelay(
            any(), eq(TimeValue.timeValueSeconds(10)), eq(ThreadPool.Names.GENERIC));
  }
}
