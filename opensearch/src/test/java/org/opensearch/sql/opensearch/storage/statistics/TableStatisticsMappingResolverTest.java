/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.statistics;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.transport.client.node.NodeClient;

class TableStatisticsMappingResolverTest {

  @Test
  void resolveReturnsEmptyMapOnFailure() {
    NodeClient client = mock(NodeClient.class);
    // No mappings are wired — the underlying describe call will fail internally;
    // the resolver must swallow the exception and return an empty map.
    TableStatisticsMappingResolver resolver = new TableStatisticsMappingResolver(client);

    Map<String, OpenSearchDataType> fieldTypes = resolver.resolve("nonexistent-index");

    assertNotNull(fieldTypes);
    assertTrue(fieldTypes.isEmpty());
  }
}
