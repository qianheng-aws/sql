/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.opensearch.client.OpenSearchNodeClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.system.OpenSearchDescribeIndexRequest;
import org.opensearch.sql.opensearch.storage.statistics.TableStatistic;
import org.opensearch.sql.opensearch.storage.statistics.TableStatisticCollector;
import org.opensearch.sql.opensearch.storage.statistics.TableStatisticStorage;
import org.opensearch.transport.client.node.NodeClient;

/**
 * REST handler for the table statistics API.
 *
 * <ul>
 *   <li>{@code GET /_plugins/_sql/_statistics/{index}} — return the stored {@link TableStatistic}
 *       JSON, or 404 if absent.
 *   <li>{@code POST /_plugins/_sql/_statistics/{index}/analyze} — trigger an async refresh via
 *       {@link TableStatisticCollector#refreshAsync(String, Map)} and return 202.
 * </ul>
 */
public class RestTableStatisticsAction extends BaseRestHandler {

  private static final Logger LOG = LogManager.getLogger(RestTableStatisticsAction.class);

  public static final String STATISTICS_API_BASE = "/_plugins/_sql/_statistics";
  public static final String GET_ROUTE = STATISTICS_API_BASE + "/{index}";
  public static final String ANALYZE_ROUTE = STATISTICS_API_BASE + "/{index}/analyze";

  private final TableStatisticStorage storage;
  private final TableStatisticCollector collector;

  public RestTableStatisticsAction(
      TableStatisticStorage storage, TableStatisticCollector collector) {
    this.storage = storage;
    this.collector = collector;
  }

  @Override
  public String getName() {
    return "table_statistics_action";
  }

  @Override
  public List<Route> routes() {
    return ImmutableList.of(
        new Route(RestRequest.Method.GET, GET_ROUTE),
        new Route(RestRequest.Method.POST, ANALYZE_ROUTE));
  }

  @Override
  protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
    String indexName = request.param("index");
    if (indexName == null || indexName.isEmpty()) {
      return channel ->
          channel.sendResponse(
              new BytesRestResponse(RestStatus.BAD_REQUEST, "Missing {index} path parameter"));
    }
    String validationError = validateConcreteIndexName(indexName);
    if (validationError != null) {
      return channel ->
          channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, validationError));
    }
    if (request.method() == RestRequest.Method.POST) {
      return channel -> handleAnalyze(indexName, client, channel);
    }
    return channel -> handleGet(indexName, channel);
  }

  /**
   * Reject index-name path parameters that could resolve to multiple indices or otherwise surprise
   * the caller: wildcards ({@code *}, {@code ?}), comma-separated lists, exclusion ({@code -})
   * prefix, and date-math ({@code <}/{@code >}) are all ambiguous here because a single stored
   * statistic doc can only describe one concrete index. Returns a human-readable error message, or
   * {@code null} when {@code indexName} is a single concrete-looking name.
   */
  static String validateConcreteIndexName(String indexName) {
    if (indexName.indexOf('*') >= 0 || indexName.indexOf('?') >= 0 || indexName.indexOf(',') >= 0) {
      return "Wildcard and comma-separated index expressions are not supported: " + indexName;
    }
    char first = indexName.charAt(0);
    if (first == '-' || first == '+' || first == '<' || first == '>') {
      return "Index-name must be a single concrete index; got: " + indexName;
    }
    return null;
  }

  private void handleGet(String indexName, RestChannel channel) {
    storage.get(
        indexName,
        new ActionListener<Optional<TableStatistic>>() {
          @Override
          public void onResponse(Optional<TableStatistic> maybeStat) {
            if (maybeStat.isEmpty()) {
              channel.sendResponse(
                  new BytesRestResponse(
                      RestStatus.NOT_FOUND,
                      "{\"error\":\"no statistic for index " + escapeJson(indexName) + "\"}"));
              return;
            }
            Map<String, Object> source = maybeStat.get().toStoredDocSource();
            channel.sendResponse(
                new BytesRestResponse(RestStatus.OK, "application/json", serializeJson(source)));
          }

          @Override
          public void onFailure(Exception e) {
            // storage.get contract: never calls onFailure. Defensive guard.
            channel.sendResponse(
                new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
          }
        });
  }

  /**
   * Trigger an async refresh. The collector's aggregation request is built from a field map, and a
   * missing map produces a docCount-only stat — almost useless to the optimizer. So before handing
   * off to the collector we resolve the mapping via {@link OpenSearchDescribeIndexRequest}, which
   * blocks on a cluster admin call. That blocking work cannot run on the Netty transport thread
   * (OpenSearch asserts against it), so we dispatch it to the generic thread pool and respond 202
   * immediately — callers should poll {@code GET /_statistics/{index}} for {@code COMPLETED} before
   * trusting the numbers, which they already had to do because of the collector's async search
   * anyway.
   */
  private void handleAnalyze(String indexName, NodeClient nodeClient, RestChannel channel) {
    try {
      nodeClient
          .threadPool()
          .generic()
          .submit(() -> resolveMappingsAndTriggerRefresh(indexName, nodeClient));
      channel.sendResponse(
          new BytesRestResponse(
              RestStatus.ACCEPTED, "application/json", "{\"status\":\"triggered\"}"));
    } catch (Exception e) {
      LOG.warn("Failed to dispatch analyze for {}: {}", indexName, e.getMessage());
      channel.sendResponse(
          new BytesRestResponse(
              RestStatus.INTERNAL_SERVER_ERROR,
              "application/json",
              "{\"error\":\"" + escapeJson(e.getMessage()) + "\"}"));
    }
  }

  private void resolveMappingsAndTriggerRefresh(String indexName, NodeClient nodeClient) {
    Map<String, OpenSearchDataType> fieldTypes = Map.of();
    try {
      fieldTypes =
          new OpenSearchDescribeIndexRequest(
                  new OpenSearchNodeClient(nodeClient), new OpenSearchRequest.IndexName(indexName))
              .getFieldTypes();
    } catch (Exception e) {
      LOG.debug(
          "analyze: failed to resolve mappings for {} — proceeding with empty field map: {}",
          indexName,
          e.getMessage());
    }
    try {
      collector.refreshAsync(indexName, fieldTypes);
    } catch (Exception e) {
      LOG.warn("Failed to trigger collector.refreshAsync for {}: {}", indexName, e.getMessage());
    }
  }

  private static String serializeJson(Map<String, Object> source) {
    return new Gson().toJson(source);
  }

  private static String escapeJson(String s) {
    if (s == null) {
      return "";
    }
    return s.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}
