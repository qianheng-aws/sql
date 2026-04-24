/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.statistics;

import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.opensearch.client.OpenSearchNodeClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.system.OpenSearchDescribeIndexRequest;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Resolves an index name into its flattened field-type map. Shared between the REST {@code
 * /analyze} handler and the cron refresh scheduler. Blocking — callers must run it on {@code
 * GENERIC} (never on the transport thread).
 *
 * <p>Failure is silent: a missing / unreachable index yields an empty map. The collector treats an
 * empty map as "no per-field aggregations" and writes a doc-count-only stat, which is a gentler
 * failure mode than a thrown exception from a background sweep.
 */
public class TableStatisticsMappingResolver {

  private static final Logger LOG = LogManager.getLogger(TableStatisticsMappingResolver.class);

  private final NodeClient nodeClient;

  public TableStatisticsMappingResolver(NodeClient nodeClient) {
    this.nodeClient = nodeClient;
  }

  public Map<String, OpenSearchDataType> resolve(String indexName) {
    try {
      return new OpenSearchDescribeIndexRequest(
              new OpenSearchNodeClient(nodeClient), new OpenSearchRequest.IndexName(indexName))
          .getFieldTypes();
    } catch (Exception e) {
      LOG.debug(
          "Failed to resolve mappings for {} — returning empty field map: {}",
          indexName,
          e.getMessage());
      return Map.of();
    }
  }
}
