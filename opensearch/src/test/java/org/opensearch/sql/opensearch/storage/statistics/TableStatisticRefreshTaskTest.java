/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.statistics;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;

class TableStatisticRefreshTaskTest {

  private TableStatisticStorage storage;
  private TableStatisticCollector collector;
  private TableStatisticsMappingResolver resolver;
  private ThreadPool threadPool;
  private Supplier<Boolean> enabledFlag;
  private Supplier<Duration> ttlSupplier;
  private Supplier<Semaphore> semaphoreSupplier;
  private Semaphore semaphore;

  @BeforeEach
  void setUp() {
    storage = mock(TableStatisticStorage.class);
    collector = mock(TableStatisticCollector.class);
    resolver = mock(TableStatisticsMappingResolver.class);
    threadPool = mock(ThreadPool.class);
    // GENERIC executor: run inline so tests stay deterministic.
    ExecutorService sameThread = mock(ExecutorService.class);
    doAnswer(
            inv -> {
              Runnable r = inv.getArgument(0);
              r.run();
              return null;
            })
        .when(sameThread)
        .execute(any(Runnable.class));
    when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(sameThread);

    enabledFlag = () -> true;
    ttlSupplier = () -> Duration.ofHours(1);
    semaphore = new Semaphore(2);
    semaphoreSupplier = () -> semaphore;
  }

  @Test
  void runDoesNothingWhenFlagOff() {
    enabledFlag = () -> false;
    new TableStatisticRefreshTask(
            storage, collector, resolver, threadPool, enabledFlag, ttlSupplier, semaphoreSupplier)
        .run();
    verify(storage, never()).listStale(any(), anyInt(), any());
  }

  @Test
  void runRespectsSemaphoreLimit() {
    // 3 stale indices, semaphore size 2 → only 2 refreshAsync calls
    doAnswer(
            inv -> {
              ActionListener<List<String>> l = inv.getArgument(2);
              l.onResponse(List.of("a", "b", "c"));
              return null;
            })
        .when(storage)
        .listStale(any(), anyInt(), any());
    when(resolver.resolve(anyString())).thenReturn(Map.of());

    new TableStatisticRefreshTask(
            storage, collector, resolver, threadPool, enabledFlag, ttlSupplier, semaphoreSupplier)
        .run();

    verify(collector, org.mockito.Mockito.times(2)).refreshAsync(anyString(), any(), any());
  }

  @Test
  void refreshOneReleasesPermitOnCompletion() {
    doAnswer(
            inv -> {
              ActionListener<List<String>> l = inv.getArgument(2);
              l.onResponse(List.of("a"));
              return null;
            })
        .when(storage)
        .listStale(any(), anyInt(), any());
    when(resolver.resolve(anyString())).thenReturn(Map.of());
    // Simulate collector completing synchronously.
    doAnswer(
            inv -> {
              ActionListener<Void> completion = inv.getArgument(2);
              completion.onResponse(null);
              return null;
            })
        .when(collector)
        .refreshAsync(anyString(), any(), any());

    new TableStatisticRefreshTask(
            storage, collector, resolver, threadPool, enabledFlag, ttlSupplier, semaphoreSupplier)
        .run();

    // Completion released the permit → both permits should be acquirable again.
    org.junit.jupiter.api.Assertions.assertTrue(semaphore.tryAcquire(2));
  }

  @Test
  void refreshOneReleasesPermitOnResolverThrow() {
    doAnswer(
            inv -> {
              ActionListener<List<String>> l = inv.getArgument(2);
              l.onResponse(List.of("a"));
              return null;
            })
        .when(storage)
        .listStale(any(), anyInt(), any());
    when(resolver.resolve(anyString())).thenThrow(new RuntimeException("boom"));

    new TableStatisticRefreshTask(
            storage, collector, resolver, threadPool, enabledFlag, ttlSupplier, semaphoreSupplier)
        .run();

    verify(collector, never()).refreshAsync(anyString(), any(), any());
    org.junit.jupiter.api.Assertions.assertTrue(semaphore.tryAcquire(2));
  }
}
