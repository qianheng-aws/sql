/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.statistics;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.common.indexInsight.IndexInsight;
import org.opensearch.ml.common.indexInsight.IndexInsightTaskStatus;
import org.opensearch.ml.common.indexInsight.MLIndexInsightType;
import org.opensearch.ml.common.transport.indexInsight.MLIndexInsightGetAction;
import org.opensearch.ml.common.transport.indexInsight.MLIndexInsightGetRequest;
import org.opensearch.ml.common.transport.indexInsight.MLIndexInsightGetResponse;
import org.opensearch.transport.client.node.NodeClient;

@ExtendWith(MockitoExtension.class)
class IndexInsightStatisticProviderTest {

  @Mock private NodeClient nodeClient;

  private IndexInsightStatisticProvider provider;

  private static final String SAMPLE_CONTENT =
      """
      {
        "important_column_and_distribution": {
          "status": {"type": "keyword", "unique_count": 5}
        }
      }
      """;

  @BeforeEach
  void setUp() {
    provider = new IndexInsightStatisticProvider(nodeClient);
  }

  @Test
  void getStatistic_success_returnsStatWithFieldStats() {
    IndexInsight insight =
        IndexInsight.builder()
            .index("test-index")
            .content(SAMPLE_CONTENT)
            .status(IndexInsightTaskStatus.COMPLETED)
            .taskType(MLIndexInsightType.STATISTICAL_DATA)
            .lastUpdatedTime(Instant.now())
            .build();
    MLIndexInsightGetResponse response =
        MLIndexInsightGetResponse.builder().indexInsight(insight).build();

    mockTransportSuccess(response);

    IndexInsightStatistic result = provider.getStatistic("test-index", 50_000L);
    assertNotNull(result);
    assertEquals(50_000.0, result.getRowCount());
    assertNotNull(result.getFieldStatistic("status"));
    assertEquals(5L, result.getFieldStatistic("status").cardinality());
  }

  @Test
  void getStatistic_transportFailure_returnsNull() {
    mockTransportFailure(new RuntimeException("Index insight not available"));

    IndexInsightStatistic result = provider.getStatistic("test-index", 10_000L);
    assertNull(result);
  }

  @Test
  void getStatistic_cachedResult_doesNotCallTransportAgain() {
    IndexInsight insight =
        IndexInsight.builder()
            .index("test-index")
            .content(SAMPLE_CONTENT)
            .status(IndexInsightTaskStatus.COMPLETED)
            .taskType(MLIndexInsightType.STATISTICAL_DATA)
            .lastUpdatedTime(Instant.now())
            .build();
    MLIndexInsightGetResponse response =
        MLIndexInsightGetResponse.builder().indexInsight(insight).build();

    mockTransportSuccess(response);

    provider.getStatistic("test-index", 50_000L);
    provider.getStatistic("test-index", 50_000L);

    // Transport action called only once
    verify(nodeClient, times(1)).execute(eq(MLIndexInsightGetAction.INSTANCE), any(), any());
  }

  @Test
  void getStatistic_sendsCorrectRequest() {
    mockTransportFailure(new RuntimeException("test"));

    provider.getStatistic("my-logs-2024", 100L);

    ArgumentCaptor<MLIndexInsightGetRequest> captor =
        ArgumentCaptor.forClass(MLIndexInsightGetRequest.class);
    verify(nodeClient).execute(eq(MLIndexInsightGetAction.INSTANCE), captor.capture(), any());

    MLIndexInsightGetRequest request = captor.getValue();
    assertEquals("my-logs-2024", request.getIndexName());
    assertEquals(MLIndexInsightType.STATISTICAL_DATA, request.getTargetIndexInsight());
  }

  @SuppressWarnings("unchecked")
  private void mockTransportSuccess(MLIndexInsightGetResponse response) {
    doAnswer(
            invocation -> {
              ActionListener<MLIndexInsightGetResponse> listener = invocation.getArgument(2);
              listener.onResponse(response);
              return null;
            })
        .when(nodeClient)
        .execute(eq(MLIndexInsightGetAction.INSTANCE), any(), any());
  }

  @SuppressWarnings("unchecked")
  private void mockTransportFailure(Exception e) {
    doAnswer(
            invocation -> {
              ActionListener<MLIndexInsightGetResponse> listener = invocation.getArgument(2);
              listener.onFailure(e);
              return null;
            })
        .when(nodeClient)
        .execute(eq(MLIndexInsightGetAction.INSTANCE), any(), any());
  }
}
