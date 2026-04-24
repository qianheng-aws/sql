/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.statistics;

import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Asynchronous CRUD facade over the dedicated {@value #STORAGE_INDEX} system index that persists
 * {@link TableStatistic} records.
 *
 * <p>All read operations swallow failures and return {@link Optional#empty()} so that callers (e.g.
 * cost estimation) can treat "no statistic available" and "the statistic index isn't reachable"
 * uniformly and trigger a refresh. Write operations propagate failures to the listener so
 * collectors can react (retry, fail, mark {@code FAILED}).
 */
public class TableStatisticStorage {

  private static final Logger LOG = LogManager.getLogger(TableStatisticStorage.class);

  /** The statistics index name. */
  static final String STORAGE_INDEX = ".opensearch-statistics";

  /**
   * Mapping for the statistics index. {@code fields} is stored with {@code "enabled": false} so
   * Lucene does not index its contents — we never search on per-field stats, only retrieve the
   * whole document. This avoids mapping-explosion for indices with many fields.
   */
  private static final Map<String, Object> INDEX_MAPPING =
      Map.of(
          "properties",
          Map.of(
              "status",
              Map.of("type", "keyword"),
              "last_updated_time",
              Map.of("type", "date", "format", "strict_date_time||epoch_millis"),
              "index_name",
              Map.of("type", "keyword"),
              "doc_count",
              Map.of("type", "long"),
              "fields",
              Map.of("type", "object", "enabled", false)));

  private final NodeClient nodeClient;

  public TableStatisticStorage(NodeClient nodeClient) {
    this.nodeClient = nodeClient;
  }

  /**
   * Deterministic per-index document id: lowercase hex sha256 of the index name (64 chars). We hash
   * the name so that exotic index names (dots, dashes, unicode) can't collide with the
   * stored-document id space.
   */
  public static String docId(String indexName) {
    return Hashing.sha256().hashString(indexName, StandardCharsets.UTF_8).toString();
  }

  /**
   * Fetch the {@link TableStatistic} for {@code indexName}. Returns {@link Optional#empty()} when
   * the doc is missing, the storage index does not exist, parsing fails, the doc status is
   * non-{@code COMPLETED}, or the transport call fails. Never calls {@code onFailure}.
   *
   * <p>Callers that need to observe the raw record (including a {@code GENERATING} or {@code
   * FAILED} status — e.g. the collector's race-protection step) should use {@link #getRaw(String,
   * ActionListener)} instead.
   */
  public void get(String indexName, ActionListener<Optional<TableStatistic>> listener) {
    GetRequest request = new GetRequest(STORAGE_INDEX, docId(indexName));
    nodeClient.get(
        request,
        new ActionListener<GetResponse>() {
          @Override
          public void onResponse(GetResponse response) {
            if (!response.isExists()) {
              listener.onResponse(Optional.empty());
              return;
            }
            try {
              TableStatistic stat = TableStatistic.fromStoredDoc(response.getSourceAsMap());
              listener.onResponse(Optional.of(stat));
            } catch (IllegalArgumentException e) {
              LOG.debug("Ignoring unparseable stat doc for {}: {}", indexName, e.getMessage());
              listener.onResponse(Optional.empty());
            }
          }

          @Override
          public void onFailure(Exception e) {
            if (ExceptionsHelper.unwrap(e, IndexNotFoundException.class) != null) {
              LOG.debug("Statistics index {} does not exist yet", STORAGE_INDEX);
            } else {
              LOG.warn(
                  "Failed to get stat doc for {} ({}): {}",
                  indexName,
                  e.getClass().getSimpleName(),
                  e.getMessage());
            }
            listener.onResponse(Optional.empty());
          }
        });
  }

  /**
   * Fetch the raw source map for {@code indexName}'s stored record regardless of status. Used by
   * the collector to check whether a {@code GENERATING} document already exists (for race
   * protection).
   *
   * <p>Returns {@link Optional#empty()} on doc-missing, index-not-found, or transport failure.
   * Never calls {@code onFailure}. Does NOT parse into a {@link TableStatistic}; the caller
   * inspects fields directly (typically {@code status} and {@code last_updated_time}).
   */
  public void getRaw(String indexName, ActionListener<Optional<Map<String, Object>>> listener) {
    GetRequest request = new GetRequest(STORAGE_INDEX, docId(indexName));
    nodeClient.get(
        request,
        new ActionListener<GetResponse>() {
          @Override
          public void onResponse(GetResponse response) {
            if (!response.isExists()) {
              listener.onResponse(Optional.empty());
              return;
            }
            listener.onResponse(Optional.of(response.getSourceAsMap()));
          }

          @Override
          public void onFailure(Exception e) {
            if (ExceptionsHelper.unwrap(e, IndexNotFoundException.class) != null) {
              LOG.debug("Statistics index {} does not exist yet", STORAGE_INDEX);
            } else {
              LOG.warn(
                  "Failed to get raw stat doc for {} ({}): {}",
                  indexName,
                  e.getClass().getSimpleName(),
                  e.getMessage());
            }
            listener.onResponse(Optional.empty());
          }
        });
  }

  /**
   * List index names whose stored stat is stale (older than {@code ttl}) and whose status is
   * actionable ({@code COMPLETED} or {@code FAILED} — {@code GENERATING} is skipped since the
   * collector has its own abandonment logic).
   *
   * <p>Returns an empty list on missing storage index or any transport failure. Never calls {@code
   * onFailure}. {@code maxResults} caps the page size; if the sweep ever returns exactly {@code
   * maxResults}, the next tick will pick up the remainder.
   */
  public void listStale(Duration ttl, int maxResults, ActionListener<List<String>> listener) {
    long cutoffMillis = System.currentTimeMillis() - ttl.toMillis();
    BoolQueryBuilder query =
        QueryBuilders.boolQuery()
            .filter(
                QueryBuilders.termsQuery(
                    "status", TableStatistic.STATUS_COMPLETED, TableStatistic.STATUS_FAILED))
            .filter(QueryBuilders.rangeQuery("last_updated_time").lt(cutoffMillis));
    SearchRequest request =
        new SearchRequest(STORAGE_INDEX)
            .source(
                new SearchSourceBuilder()
                    .query(query)
                    .size(maxResults)
                    .fetchSource(new String[] {"index_name"}, null)
                    .trackTotalHits(false));
    nodeClient.search(
        request,
        new ActionListener<SearchResponse>() {
          @Override
          public void onResponse(SearchResponse response) {
            List<String> names = new ArrayList<>();
            for (SearchHit hit : response.getHits().getHits()) {
              Object name =
                  hit.getSourceAsMap() == null ? null : hit.getSourceAsMap().get("index_name");
              if (name instanceof String s && !s.isEmpty()) {
                names.add(s);
              }
            }
            listener.onResponse(names);
          }

          @Override
          public void onFailure(Exception e) {
            if (ExceptionsHelper.unwrap(e, IndexNotFoundException.class) != null) {
              LOG.debug("Statistics index {} does not exist yet", STORAGE_INDEX);
            } else {
              LOG.warn(
                  "Failed to listStale stat docs ({}): {}",
                  e.getClass().getSimpleName(),
                  e.getMessage());
            }
            listener.onResponse(Collections.emptyList());
          }
        });
  }

  /**
   * Persist a completed {@link TableStatistic}. Creates the storage index on first write (no-op if
   * it already exists). Failures at the index-write step are surfaced to {@code listener.onFailure}
   * so the collector can decide whether to retry or mark the record {@code FAILED}.
   */
  public void put(String indexName, TableStatistic stat, ActionListener<Void> listener) {
    Map<String, Object> source = new LinkedHashMap<>(stat.toStoredDocSource());
    source.put("index_name", indexName);
    ensureIndexExists(
        ActionListener.wrap(ignored -> writeDoc(indexName, source, listener), listener::onFailure));
  }

  /**
   * Write a minimal status-only document ({@code status} + {@code last_updated_time}) for the given
   * index. Intended for short-lived markers ({@code GENERATING}, {@code FAILED}) during a refresh;
   * this overwrites any previous stat.
   */
  public void putStatus(String indexName, String status, ActionListener<Void> listener) {
    Map<String, Object> source = new LinkedHashMap<>();
    source.put("status", status);
    source.put("last_updated_time", Instant.now().toString());
    source.put("index_name", indexName);
    ensureIndexExists(
        ActionListener.wrap(ignored -> writeDoc(indexName, source, listener), listener::onFailure));
  }

  private void writeDoc(String indexName, Map<String, Object> source, ActionListener<Void> cb) {
    IndexRequest request = new IndexRequest(STORAGE_INDEX).id(docId(indexName)).source(source);
    nodeClient.index(
        request,
        new ActionListener<IndexResponse>() {
          @Override
          public void onResponse(IndexResponse response) {
            cb.onResponse(null);
          }

          @Override
          public void onFailure(Exception e) {
            cb.onFailure(e);
          }
        });
  }

  /**
   * Ensure the storage index exists, creating it with {@link #INDEX_MAPPING} if not. Idempotent — a
   * "create failed because index already exists" race is absorbed and reported as success. Only a
   * failure of the {@code exists()} call itself propagates, because without that signal we
   * genuinely cannot proceed.
   */
  void ensureIndexExists(ActionListener<Boolean> listener) {
    nodeClient
        .admin()
        .indices()
        .exists(
            new IndicesExistsRequest(STORAGE_INDEX),
            new ActionListener<IndicesExistsResponse>() {
              @Override
              public void onResponse(IndicesExistsResponse response) {
                if (response.isExists()) {
                  listener.onResponse(true);
                  return;
                }
                createIndex(listener);
              }

              @Override
              public void onFailure(Exception e) {
                LOG.warn("Failed to check if statistics index exists", e);
                listener.onFailure(e);
              }
            });
  }

  private void createIndex(ActionListener<Boolean> listener) {
    CreateIndexRequest request = new CreateIndexRequest(STORAGE_INDEX).mapping(INDEX_MAPPING);
    nodeClient
        .admin()
        .indices()
        .create(
            request,
            new ActionListener<CreateIndexResponse>() {
              @Override
              public void onResponse(CreateIndexResponse response) {
                listener.onResponse(true);
              }

              @Override
              public void onFailure(Exception e) {
                // Idempotency-tolerant ONLY for the concurrent-create race. Any other failure
                // (mapping conflict from a manually-created index, cluster block, etc.) must
                // surface so the caller — and operators — can act on it.
                if (ExceptionsHelper.unwrap(e, ResourceAlreadyExistsException.class) != null) {
                  LOG.debug("Statistics index was created concurrently by another node");
                  listener.onResponse(true);
                } else {
                  LOG.warn("Failed to create statistics index {}", STORAGE_INDEX, e);
                  listener.onFailure(e);
                }
              }
            });
  }
}
