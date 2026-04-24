/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.statistics;

import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.LocalNodeClusterManagerListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;

/**
 * Lifecycle owner for the cron-based table-statistic refresh. Registered on every node via {@link
 * LocalNodeClusterManagerListener}; the tick only actually runs on the elected cluster-manager.
 *
 * <p>Mutable state — the {@link Cancellable} tick handle and the live {@link Semaphore} — is kept
 * here and read through {@link TableStatisticRefreshTask}'s supplier indirection so that
 * dynamic-setting updates can swap either atomically.
 */
public class TableStatisticRefreshScheduler implements LocalNodeClusterManagerListener {

  private static final Logger LOG = LogManager.getLogger(TableStatisticRefreshScheduler.class);

  private final ClusterService clusterService;
  private final ThreadPool threadPool;
  private final OpenSearchSettings settings;
  private final TableStatisticStorage storage;
  private final TableStatisticCollector collector;
  private final TableStatisticsMappingResolver mappingResolver;

  private final AtomicReference<Cancellable> cancellable = new AtomicReference<>();
  private final AtomicReference<Semaphore> semaphore = new AtomicReference<>();
  private volatile boolean isClusterManager = false;

  public TableStatisticRefreshScheduler(
      ClusterService clusterService,
      ThreadPool threadPool,
      OpenSearchSettings settings,
      TableStatisticStorage storage,
      TableStatisticCollector collector,
      TableStatisticsMappingResolver mappingResolver) {
    this.clusterService = clusterService;
    this.threadPool = threadPool;
    this.settings = settings;
    this.storage = storage;
    this.collector = collector;
    this.mappingResolver = mappingResolver;
    int initial = settings.getSettingValue(Key.TABLE_STATISTICS_REFRESH_MAX_IN_FLIGHT);
    this.semaphore.set(new Semaphore(initial));
  }

  /**
   * Register this instance as a cluster-manager listener. Must be called exactly once from plugin
   * startup (caller: {@code SQLPlugin.createComponents}).
   */
  public void register() {
    clusterService.addLocalNodeClusterManagerListener(this);
  }

  @Override
  public synchronized void onClusterManager() {
    isClusterManager = true;
    scheduleTickIfEligible();
  }

  @Override
  public synchronized void offClusterManager() {
    isClusterManager = false;
    cancelTick();
  }

  /** Visible for testing — reads the currently live semaphore reference. */
  public Semaphore currentSemaphore() {
    return semaphore.get();
  }

  /** Visible for testing — cancellable handle. */
  Cancellable currentCancellable() {
    return cancellable.get();
  }

  private void scheduleTickIfEligible() {
    if (!isClusterManager) {
      return;
    }
    if (!Boolean.TRUE.equals(settings.getSettingValue(Key.TABLE_STATISTICS_ENABLED))) {
      return;
    }
    cancelTick();
    TimeValue interval = settings.getSettingValue(Key.TABLE_STATISTICS_REFRESH_INTERVAL);
    TableStatisticRefreshTask task = newTask();
    Cancellable c = threadPool.scheduleWithFixedDelay(task, interval, ThreadPool.Names.GENERIC);
    cancellable.set(c);
    LOG.debug("Table-statistics cron scheduled with interval {}", interval);
  }

  private void cancelTick() {
    Cancellable c = cancellable.getAndSet(null);
    if (c != null) {
      c.cancel();
      LOG.debug("Table-statistics cron cancelled");
    }
  }

  private TableStatisticRefreshTask newTask() {
    return new TableStatisticRefreshTask(
        storage,
        collector,
        mappingResolver,
        threadPool,
        () -> Boolean.TRUE.equals(settings.getSettingValue(Key.TABLE_STATISTICS_ENABLED)),
        () -> {
          TimeValue ttl = settings.getSettingValue(Key.TABLE_STATISTICS_TTL);
          return Duration.ofMillis(ttl.millis());
        },
        this::currentSemaphore);
  }

  /**
   * Called externally when {@code refresh_max_in_flight} changes. Swaps the semaphore; in-flight
   * permits against the old semaphore are simply lost when they release (correct — we don't care to
   * preserve them).
   */
  public synchronized void onMaxInFlightChanged(int newMax) {
    semaphore.set(new Semaphore(newMax));
  }

  /** Called externally when {@code refresh_interval} changes. */
  public synchronized void onIntervalChanged() {
    if (isClusterManager) {
      scheduleTickIfEligible();
    }
  }

  /** Called externally when {@code TABLE_STATISTICS_ENABLED} changes. */
  public synchronized void onEnabledChanged() {
    if (Boolean.TRUE.equals(settings.getSettingValue(Key.TABLE_STATISTICS_ENABLED))) {
      scheduleTickIfEligible();
    } else {
      cancelTick();
    }
  }
}
