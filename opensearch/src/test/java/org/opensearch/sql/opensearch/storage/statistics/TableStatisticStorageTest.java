/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.statistics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.IndicesAdminClient;
import org.opensearch.transport.client.node.NodeClient;

@ExtendWith(MockitoExtension.class)
class TableStatisticStorageTest {

  @Mock private NodeClient nodeClient;
  @Mock private AdminClient adminClient;
  @Mock private IndicesAdminClient indicesAdminClient;

  private TableStatisticStorage storage;

  @BeforeEach
  void setUp() {
    storage = new TableStatisticStorage(nodeClient);
  }

  // ---- docId ---------------------------------------------------------------

  @Test
  void docId_isSha256HexLowercase_64chars() {
    String id = TableStatisticStorage.docId("my-index");
    assertEquals(64, id.length(), "sha256 hex should be 64 chars");
    assertTrue(id.matches("[0-9a-f]+"), "sha256 hex should be lowercase hex, got: " + id);
    assertNotEquals(TableStatisticStorage.docId("x"), TableStatisticStorage.docId("y"));
  }

  // ---- get -----------------------------------------------------------------

  @Test
  void get_docExists_parsesAndReturns() {
    FieldStatistic status = new FieldStatistic("keyword", 5L, null, null, List.of("200"), 0.0);
    TableStatistic expected = TableStatistic.fromFields(137L, Map.of("status", status));
    mockGetSuccess(expected.toStoredDocSource());

    @SuppressWarnings("unchecked")
    ActionListener<Optional<TableStatistic>> listener = mock(ActionListener.class);
    storage.get("idx", listener);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Optional<TableStatistic>> captor = ArgumentCaptor.forClass(Optional.class);
    verify(listener).onResponse(captor.capture());
    verify(listener, never()).onFailure(any());

    Optional<TableStatistic> result = captor.getValue();
    assertTrue(result.isPresent(), "expected a TableStatistic to be returned");
    assertEquals(137L, result.get().getDocCount());
    assertNotNull(result.get().getFieldStatistic("status"));
  }

  @Test
  void get_docMissing_returnsEmpty() {
    mockGetMissing();

    @SuppressWarnings("unchecked")
    ActionListener<Optional<TableStatistic>> listener = mock(ActionListener.class);
    storage.get("idx", listener);

    verify(listener).onResponse(Optional.empty());
    verify(listener, never()).onFailure(any());
  }

  @Test
  void get_nonCompletedStatus_returnsEmpty() {
    Map<String, Object> generating =
        Map.of(
            "status",
            TableStatistic.STATUS_GENERATING,
            "doc_count",
            100,
            "last_updated_time",
            "2026-04-20T05:17:42.123Z");
    mockGetSuccess(generating);

    @SuppressWarnings("unchecked")
    ActionListener<Optional<TableStatistic>> listener = mock(ActionListener.class);
    storage.get("idx", listener);

    verify(listener).onResponse(Optional.empty());
    verify(listener, never()).onFailure(any());
  }

  @Test
  void get_indexNotFoundException_returnsEmpty() {
    mockGetFailure(new IndexNotFoundException(".opensearch-statistics"));

    @SuppressWarnings("unchecked")
    ActionListener<Optional<TableStatistic>> listener = mock(ActionListener.class);
    storage.get("idx", listener);

    verify(listener).onResponse(Optional.empty());
    verify(listener, never()).onFailure(any());
  }

  @Test
  void get_otherTransportFailure_returnsEmpty() {
    mockGetFailure(new RuntimeException("boom"));

    @SuppressWarnings("unchecked")
    ActionListener<Optional<TableStatistic>> listener = mock(ActionListener.class);
    storage.get("idx", listener);

    verify(listener).onResponse(Optional.empty());
    verify(listener, never()).onFailure(any());
  }

  // ---- put -----------------------------------------------------------------

  @Test
  void put_indexExists_skipsCreation_writesDoc() {
    wireAdminChain();
    mockIndexExistsTrue();
    mockIndexSuccess();

    FieldStatistic status = new FieldStatistic("keyword", 5L, null, null, List.of("200"), 0.0);
    TableStatistic stat = TableStatistic.fromFields(42L, Map.of("status", status));

    @SuppressWarnings("unchecked")
    ActionListener<Void> listener = mock(ActionListener.class);
    storage.put("idx", stat, listener);

    // Verify exists() was called but create() was NOT.
    verify(indicesAdminClient).exists(any(IndicesExistsRequest.class), any());
    verify(indicesAdminClient, never()).create(any(CreateIndexRequest.class), any());

    ArgumentCaptor<IndexRequest> captor = ArgumentCaptor.forClass(IndexRequest.class);
    verify(nodeClient).index(captor.capture(), any());

    IndexRequest indexed = captor.getValue();
    assertEquals(".opensearch-statistics", indexed.index());
    assertEquals(TableStatisticStorage.docId("idx"), indexed.id());
    assertTrue(indexed.sourceAsMap().containsKey("doc_count"));

    verify(listener).onResponse(null);
    verify(listener, never()).onFailure(any());
  }

  @Test
  void put_indexMissing_createsThenWrites() {
    wireAdminChain();
    mockIndexExistsFalse();
    mockCreateIndexSuccess();
    mockIndexSuccess();

    TableStatistic stat = TableStatistic.fromFields(7L, Map.of());

    @SuppressWarnings("unchecked")
    ActionListener<Void> listener = mock(ActionListener.class);
    storage.put("idx", stat, listener);

    ArgumentCaptor<CreateIndexRequest> createCaptor =
        ArgumentCaptor.forClass(CreateIndexRequest.class);
    verify(indicesAdminClient).create(createCaptor.capture(), any());

    CreateIndexRequest createReq = createCaptor.getValue();
    assertEquals(".opensearch-statistics", createReq.index());
    // The mapping should have been applied — mappings() returns a non-empty JSON string.
    assertNotNull(createReq.mappings());
    assertFalse(createReq.mappings().isEmpty(), "mapping source should be populated");
    // The serialized mapping should reference our known top-level keys.
    assertTrue(
        createReq.mappings().contains("status"),
        "mapping should include 'status' field: " + createReq.mappings());
    assertTrue(
        createReq.mappings().contains("last_updated_time"),
        "mapping should include 'last_updated_time' field: " + createReq.mappings());
    assertTrue(
        createReq.mappings().contains("doc_count"),
        "mapping should include 'doc_count' field: " + createReq.mappings());
    assertTrue(
        createReq.mappings().contains("fields"),
        "mapping should include 'fields' field: " + createReq.mappings());

    verify(nodeClient).index(any(IndexRequest.class), any());
    verify(listener).onResponse(null);
    verify(listener, never()).onFailure(any());
  }

  @Test
  void put_concurrentCreateRace_isTolerated() {
    // ResourceAlreadyExistsException means another node just created the index — proceed to write.
    wireAdminChain();
    mockIndexExistsFalse();
    mockCreateIndexFailure(
        new ResourceAlreadyExistsException("index [.opensearch-statistics] already exists"));
    mockIndexSuccess();

    TableStatistic stat = TableStatistic.fromFields(5L, Map.of());

    @SuppressWarnings("unchecked")
    ActionListener<Void> listener = mock(ActionListener.class);
    storage.put("idx", stat, listener);

    verify(indicesAdminClient).create(any(CreateIndexRequest.class), any());
    verify(nodeClient).index(any(IndexRequest.class), any());
    verify(listener).onResponse(null);
    verify(listener, never()).onFailure(any());
  }

  @Test
  void put_genuineCreateFailure_propagates() {
    // Any failure other than ResourceAlreadyExistsException (e.g. mapping conflict from a
    // pre-existing manually-created index, or a cluster block) must surface to the caller.
    wireAdminChain();
    mockIndexExistsFalse();
    RuntimeException boom = new RuntimeException("cluster is read-only");
    mockCreateIndexFailure(boom);

    TableStatistic stat = TableStatistic.fromFields(5L, Map.of());

    @SuppressWarnings("unchecked")
    ActionListener<Void> listener = mock(ActionListener.class);
    storage.put("idx", stat, listener);

    verify(listener).onFailure(boom);
    verify(nodeClient, never()).index(any(IndexRequest.class), any());
  }

  @Test
  void put_writesIndexNameField() {
    // The stored doc must carry an `index_name` keyword field so downstream sweeps (listStale)
    // can return plain index names without reversing the sha256 doc id.
    wireAdminChain();
    mockIndexExistsTrue();
    mockIndexSuccess();

    TableStatistic stat = TableStatistic.fromFields(42L, Map.of());

    @SuppressWarnings("unchecked")
    ActionListener<Void> listener = mock(ActionListener.class);
    storage.put("logs-2026", stat, listener);

    ArgumentCaptor<IndexRequest> captor = ArgumentCaptor.forClass(IndexRequest.class);
    verify(nodeClient).index(captor.capture(), any());
    assertEquals("logs-2026", captor.getValue().sourceAsMap().get("index_name"));

    verify(listener).onResponse(null);
    verify(listener, never()).onFailure(any());
  }

  @Test
  void putStatus_writesIndexNameField() {
    wireAdminChain();
    mockIndexExistsTrue();
    mockIndexSuccess();

    @SuppressWarnings("unchecked")
    ActionListener<Void> listener = mock(ActionListener.class);
    storage.putStatus("logs-2026", TableStatistic.STATUS_GENERATING, listener);

    ArgumentCaptor<IndexRequest> captor = ArgumentCaptor.forClass(IndexRequest.class);
    verify(nodeClient).index(captor.capture(), any());
    assertEquals("logs-2026", captor.getValue().sourceAsMap().get("index_name"));
  }

  @Test
  void put_indexRequestFailure_propagates() {
    wireAdminChain();
    mockIndexExistsTrue();
    RuntimeException boom = new RuntimeException("write failed");
    mockIndexFailure(boom);

    TableStatistic stat = TableStatistic.fromFields(1L, Map.of());

    @SuppressWarnings("unchecked")
    ActionListener<Void> listener = mock(ActionListener.class);
    storage.put("idx", stat, listener);

    verify(listener).onFailure(boom);
    verify(listener, never()).onResponse(any());
  }

  // ---- putStatus -----------------------------------------------------------

  @Test
  void putStatus_writesMinimalDoc() {
    wireAdminChain();
    mockIndexExistsTrue();
    mockIndexSuccess();

    @SuppressWarnings("unchecked")
    ActionListener<Void> listener = mock(ActionListener.class);
    storage.putStatus("idx", TableStatistic.STATUS_GENERATING, listener);

    ArgumentCaptor<IndexRequest> captor = ArgumentCaptor.forClass(IndexRequest.class);
    verify(nodeClient).index(captor.capture(), any());

    IndexRequest req = captor.getValue();
    assertEquals(".opensearch-statistics", req.index());
    assertEquals(TableStatisticStorage.docId("idx"), req.id());

    Map<String, Object> source = req.sourceAsMap();
    // Minimal doc: status + last_updated_time + index_name; no doc_count, no fields.
    assertEquals(3, source.size(), "putStatus should write exactly three keys, got: " + source);
    assertEquals("GENERATING", source.get("status"));
    assertNotNull(source.get("last_updated_time"));
    assertEquals("idx", source.get("index_name"));
    assertFalse(source.containsKey("doc_count"));
    assertFalse(source.containsKey("fields"));

    verify(listener).onResponse(null);
  }

  @Test
  void get_afterPutStatusGenerating_returnsEmpty() {
    // Contract test: when a GENERATING marker written by putStatus is subsequently read back via
    // get(), fromStoredDoc must reject it (missing doc_count + non-COMPLETED status), and get()
    // must swallow the IAE into Optional.empty(). This is the coordination path used by the
    // collector (Task 3) for race protection.
    Map<String, Object> generatingDoc = new LinkedHashMap<>();
    generatingDoc.put("status", TableStatistic.STATUS_GENERATING);
    generatingDoc.put("last_updated_time", java.time.Instant.now().toString());
    mockGetSuccess(generatingDoc);

    @SuppressWarnings("unchecked")
    ActionListener<Optional<TableStatistic>> listener = mock(ActionListener.class);
    storage.get("idx", listener);

    verify(listener).onResponse(Optional.empty());
    verify(listener, never()).onFailure(any());
  }

  // ---- getRaw --------------------------------------------------------------

  @Test
  void getRaw_docExists_returnsSourceMap() {
    Map<String, Object> source = new LinkedHashMap<>();
    source.put("status", TableStatistic.STATUS_GENERATING);
    source.put("last_updated_time", java.time.Instant.now().toString());
    mockGetSuccess(source);

    @SuppressWarnings("unchecked")
    ActionListener<Optional<Map<String, Object>>> listener = mock(ActionListener.class);
    storage.getRaw("idx", listener);

    ArgumentCaptor<Optional<Map<String, Object>>> captor = ArgumentCaptor.forClass(Optional.class);
    verify(listener).onResponse(captor.capture());
    Optional<Map<String, Object>> result = captor.getValue();
    assertTrue(result.isPresent());
    assertEquals(TableStatistic.STATUS_GENERATING, result.get().get("status"));
    verify(listener, never()).onFailure(any());
  }

  @Test
  void getRaw_docMissing_returnsEmpty() {
    mockGetMissing();

    @SuppressWarnings("unchecked")
    ActionListener<Optional<Map<String, Object>>> listener = mock(ActionListener.class);
    storage.getRaw("idx", listener);

    verify(listener).onResponse(Optional.empty());
    verify(listener, never()).onFailure(any());
  }

  @Test
  void getRaw_indexNotFound_returnsEmpty() {
    mockGetFailure(new IndexNotFoundException(".opensearch-statistics"));

    @SuppressWarnings("unchecked")
    ActionListener<Optional<Map<String, Object>>> listener = mock(ActionListener.class);
    storage.getRaw("idx", listener);

    verify(listener).onResponse(Optional.empty());
    verify(listener, never()).onFailure(any());
  }

  // ---- mocking helpers -----------------------------------------------------

  private void wireAdminChain() {
    when(nodeClient.admin()).thenReturn(adminClient);
    when(adminClient.indices()).thenReturn(indicesAdminClient);
  }

  @SuppressWarnings("unchecked")
  private void mockGetSuccess(Map<String, Object> sourceMap) {
    GetResponse response = mock(GetResponse.class);
    when(response.isExists()).thenReturn(true);
    when(response.getSourceAsMap()).thenReturn(sourceMap);
    doAnswer(
            invocation -> {
              ActionListener<GetResponse> listener = invocation.getArgument(1);
              listener.onResponse(response);
              return null;
            })
        .when(nodeClient)
        .get(any(GetRequest.class), any());
  }

  @SuppressWarnings("unchecked")
  private void mockGetMissing() {
    GetResponse response = mock(GetResponse.class);
    when(response.isExists()).thenReturn(false);
    doAnswer(
            invocation -> {
              ActionListener<GetResponse> listener = invocation.getArgument(1);
              listener.onResponse(response);
              return null;
            })
        .when(nodeClient)
        .get(any(GetRequest.class), any());
  }

  @SuppressWarnings("unchecked")
  private void mockGetFailure(Exception e) {
    doAnswer(
            invocation -> {
              ActionListener<GetResponse> listener = invocation.getArgument(1);
              listener.onFailure(e);
              return null;
            })
        .when(nodeClient)
        .get(any(GetRequest.class), any());
  }

  @SuppressWarnings("unchecked")
  private void mockIndexSuccess() {
    IndexResponse response = mock(IndexResponse.class);
    doAnswer(
            invocation -> {
              ActionListener<IndexResponse> listener = invocation.getArgument(1);
              listener.onResponse(response);
              return null;
            })
        .when(nodeClient)
        .index(any(IndexRequest.class), any());
  }

  @SuppressWarnings("unchecked")
  private void mockIndexFailure(Exception e) {
    doAnswer(
            invocation -> {
              ActionListener<IndexResponse> listener = invocation.getArgument(1);
              listener.onFailure(e);
              return null;
            })
        .when(nodeClient)
        .index(any(IndexRequest.class), any());
  }

  @SuppressWarnings("unchecked")
  private void mockIndexExistsTrue() {
    IndicesExistsResponse response = new IndicesExistsResponse(true);
    doAnswer(
            invocation -> {
              ActionListener<IndicesExistsResponse> listener = invocation.getArgument(1);
              listener.onResponse(response);
              return null;
            })
        .when(indicesAdminClient)
        .exists(any(IndicesExistsRequest.class), any());
  }

  @SuppressWarnings("unchecked")
  private void mockIndexExistsFalse() {
    IndicesExistsResponse response = new IndicesExistsResponse(false);
    doAnswer(
            invocation -> {
              ActionListener<IndicesExistsResponse> listener = invocation.getArgument(1);
              listener.onResponse(response);
              return null;
            })
        .when(indicesAdminClient)
        .exists(any(IndicesExistsRequest.class), any());
  }

  @SuppressWarnings("unchecked")
  private void mockCreateIndexSuccess() {
    CreateIndexResponse response = new CreateIndexResponse(true, true, ".opensearch-statistics");
    doAnswer(
            invocation -> {
              ActionListener<CreateIndexResponse> listener = invocation.getArgument(1);
              listener.onResponse(response);
              return null;
            })
        .when(indicesAdminClient)
        .create(any(CreateIndexRequest.class), any());
  }

  @SuppressWarnings("unchecked")
  private void mockCreateIndexFailure(Exception e) {
    doAnswer(
            invocation -> {
              ActionListener<CreateIndexResponse> listener = invocation.getArgument(1);
              listener.onFailure(e);
              return null;
            })
        .when(indicesAdminClient)
        .create(any(CreateIndexRequest.class), any());
  }
}
