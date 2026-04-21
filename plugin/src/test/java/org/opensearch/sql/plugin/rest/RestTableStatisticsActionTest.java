/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.sql.opensearch.storage.statistics.TableStatistic;
import org.opensearch.sql.opensearch.storage.statistics.TableStatisticCollector;
import org.opensearch.sql.opensearch.storage.statistics.TableStatisticStorage;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

/** Unit tests for {@link RestTableStatisticsAction}. */
@RunWith(MockitoJUnitRunner.class)
public class RestTableStatisticsActionTest {

  @Mock private TableStatisticStorage storage;
  @Mock private TableStatisticCollector collector;
  @Mock private NodeClient nodeClient;

  private RestTableStatisticsAction action;

  @Before
  public void setUp() {
    action = new RestTableStatisticsAction(storage, collector);
  }

  @Test
  public void routes_containsBothEndpoints() {
    List<Route> routes = action.routes();
    assertEquals(2, routes.size());
    // GET route
    assertEquals(RestRequest.Method.GET, routes.get(0).getMethod());
    assertEquals(RestTableStatisticsAction.GET_ROUTE, routes.get(0).getPath());
    // POST route
    assertEquals(RestRequest.Method.POST, routes.get(1).getMethod());
    assertEquals(RestTableStatisticsAction.ANALYZE_ROUTE, routes.get(1).getPath());
  }

  @Test
  public void getName_returnsExpected() {
    assertEquals("table_statistics_action", action.getName());
  }

  @Test
  public void GET_returnsStoredStat_whenPresent() throws Exception {
    TableStatistic stat = TableStatistic.fromFields(42L, Collections.emptyMap());
    doAnswer(
            invocation -> {
              ActionListener<Optional<TableStatistic>> listener = invocation.getArgument(1);
              listener.onResponse(Optional.of(stat));
              return null;
            })
        .when(storage)
        .get(eq("idx"), any());

    FakeRestRequest request = newGetRequest("idx");
    MockRestChannel channel = new MockRestChannel(request);
    action.handleRequest(request, channel, nodeClient);

    RestResponse response = channel.getResponse();
    assertNotNull(response);
    assertEquals(RestStatus.OK, response.status());
    assertEquals("application/json", response.contentType());
    String body = ((BytesRestResponse) response).content().utf8ToString();
    assertTrue("Body should contain doc_count=42: " + body, body.contains("\"doc_count\":42"));
  }

  @Test
  public void GET_returns404_whenAbsent() throws Exception {
    doAnswer(
            invocation -> {
              ActionListener<Optional<TableStatistic>> listener = invocation.getArgument(1);
              listener.onResponse(Optional.empty());
              return null;
            })
        .when(storage)
        .get(eq("idx"), any());

    FakeRestRequest request = newGetRequest("idx");
    MockRestChannel channel = new MockRestChannel(request);
    action.handleRequest(request, channel, nodeClient);

    assertEquals(RestStatus.NOT_FOUND, channel.getResponse().status());
  }

  @Test
  public void POST_analyze_triggersRefresh_andReturns202() throws Exception {
    FakeRestRequest request = newAnalyzeRequest("idx");
    MockRestChannel channel = new MockRestChannel(request);
    action.handleRequest(request, channel, nodeClient);

    verify(collector).refreshAsync(eq("idx"), any());
    RestResponse response = channel.getResponse();
    assertEquals(RestStatus.ACCEPTED, response.status());
    String body = ((BytesRestResponse) response).content().utf8ToString();
    assertTrue("Body should contain 'triggered': " + body, body.contains("triggered"));
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void POST_analyze_usesEmptyFieldTypes() throws Exception {
    FakeRestRequest request = newAnalyzeRequest("idx");
    MockRestChannel channel = new MockRestChannel(request);
    action.handleRequest(request, channel, nodeClient);

    ArgumentCaptor<Map> fieldTypesCaptor = ArgumentCaptor.forClass(Map.class);
    verify(collector).refreshAsync(eq("idx"), fieldTypesCaptor.capture());
    assertTrue("fieldTypes should be empty", fieldTypesCaptor.getValue().isEmpty());
  }

  @Test
  public void prepareRequest_missingIndexParam_returns400() throws Exception {
    RestRequest request = mock(RestRequest.class);
    RestChannel channel = mock(RestChannel.class);
    // Simulate a request with no {index} path param resolution.
    // We can't easily build a FakeRestRequest that yields a null param("index"), so mock directly.
    // handleRequest will call prepareRequest, which returns a consumer that sends the 400.
    action.handleRequest(request, channel, nodeClient);

    ArgumentCaptor<RestResponse> responseCaptor = ArgumentCaptor.forClass(RestResponse.class);
    verify(channel).sendResponse(responseCaptor.capture());
    assertEquals(RestStatus.BAD_REQUEST, responseCaptor.getValue().status());
  }

  @Test
  public void validateConcreteIndexName_acceptsConcreteNames() {
    assertNull(RestTableStatisticsAction.validateConcreteIndexName("my-index"));
    assertNull(RestTableStatisticsAction.validateConcreteIndexName("logs-2026.04"));
    assertNull(RestTableStatisticsAction.validateConcreteIndexName(".internal-index"));
  }

  @Test
  public void validateConcreteIndexName_rejectsWildcardsAndCommaList() {
    assertNotNull(RestTableStatisticsAction.validateConcreteIndexName("logs-*"));
    assertNotNull(RestTableStatisticsAction.validateConcreteIndexName("logs-?"));
    assertNotNull(RestTableStatisticsAction.validateConcreteIndexName("a,b"));
    assertNotNull(RestTableStatisticsAction.validateConcreteIndexName("-excluded"));
    assertNotNull(RestTableStatisticsAction.validateConcreteIndexName("<date-math>"));
  }

  @Test
  public void POST_analyze_wildcardIndex_returns400() throws Exception {
    FakeRestRequest request = newAnalyzeRequest("logs-*");
    RestChannel channel = new MockRestChannel(request);
    action.handleRequest(request, channel, nodeClient);

    RestResponse response = ((MockRestChannel) channel).getResponse();
    assertEquals(RestStatus.BAD_REQUEST, response.status());
    // Wildcard requests must NOT trigger any collector work.
    verify(collector, never()).refreshAsync(any(), any());
  }

  private static FakeRestRequest newGetRequest(String index) {
    Map<String, String> params = new HashMap<>();
    params.put("index", index);
    return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
        .withMethod(RestRequest.Method.GET)
        .withPath(RestTableStatisticsAction.STATISTICS_API_BASE + "/" + index)
        .withParams(params)
        .build();
  }

  private static FakeRestRequest newAnalyzeRequest(String index) {
    Map<String, String> params = new HashMap<>();
    params.put("index", index);
    return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
        .withMethod(RestRequest.Method.POST)
        .withPath(RestTableStatisticsAction.STATISTICS_API_BASE + "/" + index + "/analyze")
        .withParams(params)
        .build();
  }

  /** Mock RestChannel to capture responses (mirrors RestPPLGrammarActionTest). */
  private static class MockRestChannel implements RestChannel {
    private final RestRequest request;
    private RestResponse response;

    MockRestChannel(RestRequest request) {
      this.request = request;
    }

    @Override
    public void sendResponse(RestResponse response) {
      this.response = response;
    }

    public RestResponse getResponse() {
      return response;
    }

    @Override
    public RestRequest request() {
      return request;
    }

    @Override
    public boolean detailedErrorsEnabled() {
      return true;
    }

    @Override
    public boolean detailedErrorStackTraceEnabled() {
      return false;
    }

    @Override
    public XContentBuilder newBuilder() throws IOException {
      return XContentBuilder.builder(XContentType.JSON.xContent());
    }

    @Override
    public XContentBuilder newErrorBuilder() throws IOException {
      return XContentBuilder.builder(XContentType.JSON.xContent());
    }

    @Override
    public XContentBuilder newBuilder(MediaType mediaType, boolean useFiltering)
        throws IOException {
      return XContentBuilder.builder(XContentType.JSON.xContent());
    }

    @Override
    public XContentBuilder newBuilder(
        MediaType requestContentType, MediaType responseContentType, boolean useFiltering)
        throws IOException {
      return XContentBuilder.builder(XContentType.JSON.xContent());
    }

    @Override
    public BytesStreamOutput bytesOutput() {
      return new BytesStreamOutput();
    }
  }
}
