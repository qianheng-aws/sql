/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.statistics;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.threadpool.ThreadPool;

/**
 * Single tick body for the cron refresh sweep. Runs only on the cluster-manager node — the
 * scheduler that owns this task is gated by {@code LocalNodeClusterManagerListener}.
 *
 * <p>Flow: (1) short-circuit when the feature flag is off; (2) call {@link
 * TableStatisticStorage#listStale} with the current TTL; (3) for each returned index name, {@code
 * tryAcquire} a permit from the live semaphore — on failure, skip (next tick picks it up); on
 * success, dispatch to the generic pool to resolve mapping synchronously and fire {@link
 * TableStatisticCollector#refreshAsync} with a completion listener that releases the permit.
 *
 * <p>Stateless beyond the injected collaborators. The {@code Supplier<Semaphore>} indirection lets
 * the scheduler swap the semaphore reference when {@code refresh_max_in_flight} changes.
 */
public class TableStatisticRefreshTask implements Runnable {

  private static final Logger LOG = LogManager.getLogger(TableStatisticRefreshTask.class);

  /** Cap on page size per sweep. A larger cluster will self-resolve across successive ticks. */
  static final int LIST_STALE_LIMIT = 1000;

  private final TableStatisticStorage storage;
  private final TableStatisticCollector collector;
  private final TableStatisticsMappingResolver mappingResolver;
  private final ThreadPool threadPool;
  private final Supplier<Boolean> enabledFlag;
  private final Supplier<Duration> ttlSupplier;
  private final Supplier<Semaphore> semaphoreSupplier;

  public TableStatisticRefreshTask(
      TableStatisticStorage storage,
      TableStatisticCollector collector,
      TableStatisticsMappingResolver mappingResolver,
      ThreadPool threadPool,
      Supplier<Boolean> enabledFlag,
      Supplier<Duration> ttlSupplier,
      Supplier<Semaphore> semaphoreSupplier) {
    this.storage = storage;
    this.collector = collector;
    this.mappingResolver = mappingResolver;
    this.threadPool = threadPool;
    this.enabledFlag = enabledFlag;
    this.ttlSupplier = ttlSupplier;
    this.semaphoreSupplier = semaphoreSupplier;
  }

  @Override
  public void run() {
    if (!Boolean.TRUE.equals(enabledFlag.get())) {
      return;
    }
    Duration ttl = ttlSupplier.get();
    Semaphore semaphore = semaphoreSupplier.get();
    storage.listStale(
        ttl,
        LIST_STALE_LIMIT,
        new ActionListener<List<String>>() {
          @Override
          public void onResponse(List<String> staleNames) {
            for (String name : staleNames) {
              if (!semaphore.tryAcquire()) {
                LOG.debug("Semaphore exhausted; skipping remaining stale indices this tick");
                return;
              }
              threadPool
                  .executor(ThreadPool.Names.GENERIC)
                  .execute(() -> refreshOne(name, semaphore));
            }
          }

          @Override
          public void onFailure(Exception e) {
            // listStale contract: never called. Release no permit (none acquired).
            LOG.debug("listStale onFailure called unexpectedly: {}", e.getMessage());
          }
        });
  }

  private void refreshOne(String name, Semaphore semaphore) {
    try {
      Map<String, OpenSearchDataType> fieldTypes = mappingResolver.resolve(name);
      collector.refreshAsync(
          name,
          fieldTypes,
          ActionListener.wrap(v -> semaphore.release(), e -> semaphore.release()));
    } catch (RuntimeException ex) {
      LOG.debug("refreshOne failed for {}: {}", name, ex.getMessage());
      semaphore.release();
    }
  }
}
