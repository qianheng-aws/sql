/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.statistics;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.common.indexInsight.MLIndexInsightType;
import org.opensearch.ml.common.transport.indexInsight.MLIndexInsightGetAction;
import org.opensearch.ml.common.transport.indexInsight.MLIndexInsightGetRequest;
import org.opensearch.ml.common.transport.indexInsight.MLIndexInsightGetResponse;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Fetches Index Insight {@code STATISTICAL_DATA} via the ml-commons transport action and caches the
 * parsed {@link IndexInsightStatistic} per index. On any failure (transport error, timeout, parse
 * error) {@link #getStatistic(String, long)} returns {@code null} so callers can fall back to
 * default cost estimates.
 *
 * <p>Note: {@link ConcurrentHashMap#computeIfAbsent} does not cache {@code null} mapping values, so
 * failed lookups are naturally retried on subsequent calls.
 */
public class IndexInsightStatisticProvider {

  private static final Logger LOG = LogManager.getLogger(IndexInsightStatisticProvider.class);
  private static final long FETCH_TIMEOUT_SECONDS = 5;

  private final NodeClient nodeClient;
  private final ConcurrentHashMap<String, IndexInsightStatistic> cache = new ConcurrentHashMap<>();

  public IndexInsightStatisticProvider(NodeClient nodeClient) {
    this.nodeClient = nodeClient;
  }

  /**
   * Get statistic for the given index. Returns cached result if available. Returns {@code null} if
   * Index Insight is unavailable or the fetch fails.
   *
   * @param indexName the OpenSearch index name
   * @param docCount estimated document count (from {@code _stats} or {@code maxResultWindow})
   * @return IndexInsightStatistic or {@code null} if unavailable
   */
  public IndexInsightStatistic getStatistic(String indexName, long docCount) {
    return cache.computeIfAbsent(indexName, key -> fetchStatistic(key, docCount));
  }

  private IndexInsightStatistic fetchStatistic(String indexName, long docCount) {
    MLIndexInsightGetRequest request =
        new MLIndexInsightGetRequest(indexName, MLIndexInsightType.STATISTICAL_DATA, null);

    AtomicReference<IndexInsightStatistic> resultRef = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);

    nodeClient.execute(
        MLIndexInsightGetAction.INSTANCE,
        request,
        new ActionListener<MLIndexInsightGetResponse>() {
          @Override
          public void onResponse(MLIndexInsightGetResponse response) {
            try {
              String content = response.getIndexInsight().getContent();
              resultRef.set(IndexInsightStatistic.fromContentJson(content, docCount));
            } catch (Exception e) {
              LOG.warn("Failed to parse Index Insight response for {}", indexName, e);
            } finally {
              latch.countDown();
            }
          }

          @Override
          public void onFailure(Exception e) {
            LOG.debug("Index Insight unavailable for {}: {}", indexName, e.getMessage());
            latch.countDown();
          }
        });

    try {
      if (!latch.await(FETCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        LOG.warn("Index Insight fetch timed out for {}", indexName);
        return null;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    }
    return resultRef.get();
  }

  public void invalidate(String indexName) {
    cache.remove(indexName);
  }

  public void invalidateAll() {
    cache.clear();
  }
}
