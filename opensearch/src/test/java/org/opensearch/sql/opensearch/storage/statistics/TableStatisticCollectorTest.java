/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.statistics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.search.TotalHits;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.sampler.InternalSampler;
import org.opensearch.search.aggregations.bucket.sampler.SamplerAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalCardinality;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.transport.client.node.NodeClient;

@ExtendWith(MockitoExtension.class)
class TableStatisticCollectorTest {

  @Mock private NodeClient nodeClient;
  @Mock private TableStatisticStorage storage;

  private TableStatisticCollector collector;

  @BeforeEach
  void setUp() {
    collector = new TableStatisticCollector(nodeClient, storage);
  }

  // ---- buildAggregationRequest --------------------------------------------

  @Test
  void buildAggregationRequest_addsCorrectAggregationsPerFieldType() {
    Map<String, OpenSearchDataType> fieldTypes = new LinkedHashMap<>();
    fieldTypes.put("status", OpenSearchDataType.of(MappingType.Keyword));
    fieldTypes.put("latency", OpenSearchDataType.of(MappingType.Long));
    // text with a keyword sub-field
    Map<String, OpenSearchDataType> subKw = new LinkedHashMap<>();
    subKw.put("keyword", OpenSearchDataType.of(MappingType.Keyword));
    fieldTypes.put("message", OpenSearchTextType.of(subKw));
    // text without sub-field — should be skipped entirely
    fieldTypes.put("raw_text", OpenSearchTextType.of());
    fieldTypes.put("@timestamp", OpenSearchDataType.of(MappingType.Date));
    fieldTypes.put("active", OpenSearchDataType.of(MappingType.Boolean));
    fieldTypes.put("obj", OpenSearchDataType.of(MappingType.Object));

    SearchRequest request = collector.buildAggregationRequest("my-idx", fieldTypes);

    // Target index
    assertEquals(1, request.indices().length);
    assertEquals("my-idx", request.indices()[0]);

    SearchSourceBuilder source = request.source();
    assertEquals(0, source.size(), "expected size=0");
    assertTrue(source.trackTotalHitsUpTo() != null, "expected trackTotalHits to be set");

    // Collect top-level aggregations by name, locate the sampler among them
    Map<String, AggregationBuilder> topLevel = new LinkedHashMap<>();
    SamplerAggregationBuilder sampler = null;
    for (AggregationBuilder agg : source.aggregations().getAggregatorFactories()) {
      topLevel.put(agg.getName(), agg);
      if (agg instanceof SamplerAggregationBuilder sa
          && TableStatisticCollector.SAMPLER_AGG.equals(sa.getName())) {
        sampler = sa;
      }
    }
    assertNotNull(sampler, "sampler should be present for cardinality sub-aggs");
    assertEquals(100_000, sampler.shardSize());

    Map<String, AggregationBuilder> samplerSubs = new LinkedHashMap<>();
    for (AggregationBuilder sub : sampler.getSubAggregations()) {
      samplerSubs.put(sub.getName(), sub);
    }

    // status (keyword): cardinality only
    assertTrue(
        samplerSubs.containsKey(TableStatisticCollector.CARDINALITY_PREFIX + "status"),
        "cardinality for status (sampler sub-agg)");
    assertTrue(
        !topLevel.containsKey(TableStatisticCollector.MIN_PREFIX + "status"),
        "no min for keyword status");

    // latency (long): cardinality in sampler, min/max top-level
    assertTrue(
        samplerSubs.containsKey(TableStatisticCollector.CARDINALITY_PREFIX + "latency"),
        "cardinality for latency (sampler sub-agg)");
    assertTrue(
        topLevel.containsKey(TableStatisticCollector.MIN_PREFIX + "latency"),
        "min for latency (top-level)");
    assertTrue(
        topLevel.containsKey(TableStatisticCollector.MAX_PREFIX + "latency"),
        "max for latency (top-level)");
    assertTrue(
        !samplerSubs.containsKey(TableStatisticCollector.MIN_PREFIX + "latency"),
        "min must NOT be under sampler");

    // message (text w/ .keyword): cardinality only (on .keyword sub-field)
    assertTrue(
        samplerSubs.containsKey(TableStatisticCollector.CARDINALITY_PREFIX + "message"),
        "cardinality for message");
    assertTrue(
        !topLevel.containsKey(TableStatisticCollector.MIN_PREFIX + "message"), "no min for text");

    // raw_text (text w/o keyword sub-field): skipped
    assertTrue(
        !samplerSubs.containsKey(TableStatisticCollector.CARDINALITY_PREFIX + "raw_text"),
        "raw_text text without keyword should be skipped");

    // @timestamp (date): min + max top-level, no cardinality
    assertTrue(
        !samplerSubs.containsKey(TableStatisticCollector.CARDINALITY_PREFIX + "@timestamp"),
        "no cardinality for date");
    assertTrue(
        topLevel.containsKey(TableStatisticCollector.MIN_PREFIX + "@timestamp"),
        "min for date (top-level)");
    assertTrue(
        topLevel.containsKey(TableStatisticCollector.MAX_PREFIX + "@timestamp"),
        "max for date (top-level)");

    // active (boolean): skipped
    assertTrue(
        !samplerSubs.containsKey(TableStatisticCollector.CARDINALITY_PREFIX + "active"),
        "boolean should be skipped");
    assertTrue(
        !topLevel.containsKey(TableStatisticCollector.MIN_PREFIX + "active"),
        "boolean should be skipped");

    // obj (object): skipped
    assertTrue(
        !samplerSubs.containsKey(TableStatisticCollector.CARDINALITY_PREFIX + "obj"),
        "object should be skipped");
  }

  // ---- refreshAsync: race protection --------------------------------------

  @Test
  void refreshAsync_whenGeneratingMarkerFresh_isNoop() {
    // status=GENERATING, fresh (just now) — should skip
    Map<String, Object> generating = new LinkedHashMap<>();
    generating.put("status", TableStatistic.STATUS_GENERATING);
    generating.put("last_updated_time", Instant.now().toString());
    mockGetRaw(Optional.of(generating));

    collector.refreshAsync("idx", Map.of("status", OpenSearchDataType.of(MappingType.Keyword)));

    verify(nodeClient, never()).search(any(SearchRequest.class), any());
    verify(storage, never()).putStatus(any(), any(), any());
    verify(storage, never()).put(any(), any(), any());
  }

  @Test
  void refreshAsync_whenGeneratingMarkerStale_proceeds() {
    // status=GENERATING, but last_updated_time is >10 minutes ago → abandoned, proceed
    Map<String, Object> generating = new LinkedHashMap<>();
    generating.put("status", TableStatistic.STATUS_GENERATING);
    generating.put("last_updated_time", Instant.now().minusSeconds(20 * 60).toString());
    mockGetRaw(Optional.of(generating));
    mockPutStatusSuccess();
    mockSearchSuccess(buildResponse(42L, null));

    collector.refreshAsync("idx", Map.of("status", OpenSearchDataType.of(MappingType.Keyword)));

    verify(nodeClient).search(any(SearchRequest.class), any());
  }

  @Test
  void refreshAsync_whenNoMarker_proceeds() {
    mockGetRaw(Optional.empty());
    mockPutStatusSuccess();
    SearchResponse response = buildResponse(137L, null);
    mockSearchSuccess(response);
    mockPutSuccess();

    Map<String, OpenSearchDataType> fieldTypes =
        Map.of("status", OpenSearchDataType.of(MappingType.Keyword));

    collector.refreshAsync("my-idx", fieldTypes);

    // marker written
    verify(storage).putStatus(eq("my-idx"), eq(TableStatistic.STATUS_GENERATING), any());

    // search called with correct index
    ArgumentCaptor<SearchRequest> reqCaptor = ArgumentCaptor.forClass(SearchRequest.class);
    verify(nodeClient).search(reqCaptor.capture(), any());
    assertEquals("my-idx", reqCaptor.getValue().indices()[0]);

    // stat written
    ArgumentCaptor<TableStatistic> statCaptor = ArgumentCaptor.forClass(TableStatistic.class);
    verify(storage).put(eq("my-idx"), statCaptor.capture(), any());
    assertEquals(137L, statCaptor.getValue().getDocCount());
  }

  @Test
  void refreshAsync_whenCompletedMarkerPresent_stillProceeds() {
    Map<String, Object> completed = new LinkedHashMap<>();
    completed.put("status", TableStatistic.STATUS_COMPLETED);
    completed.put("last_updated_time", Instant.now().toString());
    mockGetRaw(Optional.of(completed));
    mockPutStatusSuccess();
    mockSearchSuccess(buildResponse(10L, null));
    mockPutSuccess();

    collector.refreshAsync("idx", Map.of("status", OpenSearchDataType.of(MappingType.Keyword)));

    verify(nodeClient).search(any(SearchRequest.class), any());
    verify(storage).put(eq("idx"), any(), any());
  }

  @Test
  void refreshAsync_whenSearchFails_writesFailedStatus() {
    mockGetRaw(Optional.empty());
    mockPutStatusSuccess();
    mockSearchFailure(new RuntimeException("boom"));

    collector.refreshAsync("idx", Map.of("status", OpenSearchDataType.of(MappingType.Keyword)));

    verify(storage).putStatus(eq("idx"), eq(TableStatistic.STATUS_GENERATING), any());
    verify(storage).putStatus(eq("idx"), eq(TableStatistic.STATUS_FAILED), any());
    verify(storage, never()).put(any(), any(), any());
  }

  // ---- parseSearchResponse -----------------------------------------------

  @Test
  void parseSearchResponse_populatesFieldStatistics() {
    InternalCardinality statusCard = mock(InternalCardinality.class);
    when(statusCard.getValue()).thenReturn(5L);

    InternalCardinality latencyCard = mock(InternalCardinality.class);
    when(latencyCard.getValue()).thenReturn(800L);

    InternalMin latencyMin = mock(InternalMin.class);
    when(latencyMin.getValue()).thenReturn(1.0);

    InternalMax latencyMax = mock(InternalMax.class);
    when(latencyMax.getValue()).thenReturn(999.0);

    // Cardinality lives inside the sampler
    InternalAggregations samplerSubs = mock(InternalAggregations.class);
    lenient()
        .when(samplerSubs.get(TableStatisticCollector.CARDINALITY_PREFIX + "status"))
        .thenReturn(statusCard);
    lenient()
        .when(samplerSubs.get(TableStatisticCollector.CARDINALITY_PREFIX + "latency"))
        .thenReturn(latencyCard);

    InternalSampler sampler = mock(InternalSampler.class);
    when(sampler.getAggregations()).thenReturn(samplerSubs);

    // min/max live at the top level. Use lenient() because the parser also calls
    // topLevel.get("min_status")/"max_status" for the keyword field (returns null → omitted),
    // and Mockito's default strictness would flag those as unexpected stubs.
    Aggregations topLevel = mock(Aggregations.class);
    lenient().when(topLevel.get(TableStatisticCollector.SAMPLER_AGG)).thenReturn(sampler);
    lenient()
        .when(topLevel.get(TableStatisticCollector.MIN_PREFIX + "latency"))
        .thenReturn(latencyMin);
    lenient()
        .when(topLevel.get(TableStatisticCollector.MAX_PREFIX + "latency"))
        .thenReturn(latencyMax);

    SearchResponse response = buildResponseWithTopAggs(123L, topLevel);

    Map<String, OpenSearchDataType> fieldTypes = new LinkedHashMap<>();
    fieldTypes.put("status", OpenSearchDataType.of(MappingType.Keyword));
    fieldTypes.put("latency", OpenSearchDataType.of(MappingType.Long));

    TableStatistic stat = collector.parseSearchResponse(response, fieldTypes);

    assertEquals(123L, stat.getDocCount());
    FieldStatistic statusStat = stat.getFieldStatistic("status");
    assertNotNull(statusStat);
    assertEquals(5L, statusStat.cardinality());

    FieldStatistic latencyStat = stat.getFieldStatistic("latency");
    assertNotNull(latencyStat);
    assertEquals(800L, latencyStat.cardinality());
    assertEquals(1.0, (Double) latencyStat.minValue());
    assertEquals(999.0, (Double) latencyStat.maxValue());
  }

  @Test
  void parseSearchResponse_handlesMissingAggregations() {
    // Sampler not present in response
    SearchResponse response = buildResponse(42L, null);
    Map<String, OpenSearchDataType> fieldTypes =
        Map.of("status", OpenSearchDataType.of(MappingType.Keyword));

    TableStatistic stat = collector.parseSearchResponse(response, fieldTypes);

    assertEquals(42L, stat.getDocCount());
    assertTrue(stat.getFields().isEmpty(), "fields should be empty when sampler missing");
  }

  @Test
  void parseSearchResponse_infiniteMinMax_returnsNull() {
    InternalCardinality card = mock(InternalCardinality.class);
    when(card.getValue()).thenReturn(0L);
    InternalMin min = mock(InternalMin.class);
    when(min.getValue()).thenReturn(Double.POSITIVE_INFINITY);
    InternalMax max = mock(InternalMax.class);
    when(max.getValue()).thenReturn(Double.NEGATIVE_INFINITY);

    InternalAggregations samplerSubs = mock(InternalAggregations.class);
    when(samplerSubs.get(TableStatisticCollector.CARDINALITY_PREFIX + "latency")).thenReturn(card);

    InternalSampler sampler = mock(InternalSampler.class);
    when(sampler.getAggregations()).thenReturn(samplerSubs);

    Aggregations topLevel = mock(Aggregations.class);
    when(topLevel.get(TableStatisticCollector.SAMPLER_AGG)).thenReturn(sampler);
    when(topLevel.get(TableStatisticCollector.MIN_PREFIX + "latency")).thenReturn(min);
    when(topLevel.get(TableStatisticCollector.MAX_PREFIX + "latency")).thenReturn(max);

    SearchResponse response = buildResponseWithTopAggs(10L, topLevel);
    Map<String, OpenSearchDataType> fieldTypes =
        Map.of("latency", OpenSearchDataType.of(MappingType.Long));

    TableStatistic stat = collector.parseSearchResponse(response, fieldTypes);

    FieldStatistic latency = stat.getFieldStatistic("latency");
    assertNotNull(latency);
    assertNull(latency.minValue(), "POSITIVE_INFINITY should map to null");
    assertNull(latency.maxValue(), "NEGATIVE_INFINITY should map to null");
  }

  @Test
  void parseSearchResponsePopulatesNullRatio() {
    // foo: value_count=80 against totalHits=100 → null_ratio = 0.2
    InternalCardinality fooCard = mock(InternalCardinality.class);
    lenient().when(fooCard.getValue()).thenReturn(10L);
    InternalMin fooMin = mock(InternalMin.class);
    lenient().when(fooMin.getValue()).thenReturn(0.0);
    InternalMax fooMax = mock(InternalMax.class);
    lenient().when(fooMax.getValue()).thenReturn(50.0);
    InternalValueCount fooCount = mock(InternalValueCount.class);
    lenient().when(fooCount.getValue()).thenReturn(80L);

    // bar: multi-valued, value_count=150 > docCount=100 → clamp to 0.0
    InternalCardinality barCard = mock(InternalCardinality.class);
    lenient().when(barCard.getValue()).thenReturn(7L);
    InternalMin barMin = mock(InternalMin.class);
    lenient().when(barMin.getValue()).thenReturn(1.0);
    InternalMax barMax = mock(InternalMax.class);
    lenient().when(barMax.getValue()).thenReturn(9.0);
    InternalValueCount barCount = mock(InternalValueCount.class);
    lenient().when(barCount.getValue()).thenReturn(150L);

    InternalAggregations samplerSubs = mock(InternalAggregations.class);
    lenient()
        .when(samplerSubs.get(TableStatisticCollector.CARDINALITY_PREFIX + "foo"))
        .thenReturn(fooCard);
    lenient()
        .when(samplerSubs.get(TableStatisticCollector.CARDINALITY_PREFIX + "bar"))
        .thenReturn(barCard);

    InternalSampler sampler = mock(InternalSampler.class);
    when(sampler.getAggregations()).thenReturn(samplerSubs);

    Aggregations topLevel = mock(Aggregations.class);
    lenient().when(topLevel.get(TableStatisticCollector.SAMPLER_AGG)).thenReturn(sampler);
    lenient().when(topLevel.get(TableStatisticCollector.MIN_PREFIX + "foo")).thenReturn(fooMin);
    lenient().when(topLevel.get(TableStatisticCollector.MAX_PREFIX + "foo")).thenReturn(fooMax);
    lenient().when(topLevel.get(TableStatisticCollector.COUNT_PREFIX + "foo")).thenReturn(fooCount);
    lenient().when(topLevel.get(TableStatisticCollector.MIN_PREFIX + "bar")).thenReturn(barMin);
    lenient().when(topLevel.get(TableStatisticCollector.MAX_PREFIX + "bar")).thenReturn(barMax);
    lenient().when(topLevel.get(TableStatisticCollector.COUNT_PREFIX + "bar")).thenReturn(barCount);

    SearchResponse response = buildResponseWithTopAggs(100L, topLevel);

    Map<String, OpenSearchDataType> fieldTypes = new LinkedHashMap<>();
    fieldTypes.put("foo", OpenSearchDataType.of(MappingType.Long));
    fieldTypes.put("bar", OpenSearchDataType.of(MappingType.Long));

    TableStatistic stat = collector.parseSearchResponse(response, fieldTypes);

    assertEquals(0.2, stat.getFieldStatistic("foo").nullRatio(), 1e-9);
    assertEquals(0.0, stat.getFieldStatistic("bar").nullRatio(), 1e-9);
  }

  @Test
  void buildAggregationRequest_dottedFieldName_subAggNameContainsDot() {
    // Flattened nested fields have dot-separated names ("user.name", "user.age"). We embed
    // the flat name directly into the sub-aggregation name ("cardinality_user.name"), which
    // OpenSearch's AggregationBuilders.cardinality/min/max accept verbatim. This test locks
    // in that behaviour — regression-protection against someone later "sanitizing" names by
    // replacing dots, which would silently break stats collection on nested-object indices.
    Map<String, OpenSearchDataType> fieldTypes = new LinkedHashMap<>();
    fieldTypes.put("user.name", OpenSearchDataType.of(MappingType.Keyword));
    fieldTypes.put("user.age", OpenSearchDataType.of(MappingType.Integer));

    SearchRequest request = collector.buildAggregationRequest("nested-idx", fieldTypes);

    Map<String, AggregationBuilder> topLevel = new LinkedHashMap<>();
    SamplerAggregationBuilder sampler = null;
    for (AggregationBuilder agg : request.source().aggregations().getAggregatorFactories()) {
      topLevel.put(agg.getName(), agg);
      if (agg instanceof SamplerAggregationBuilder sa
          && TableStatisticCollector.SAMPLER_AGG.equals(sa.getName())) {
        sampler = sa;
      }
    }
    assertNotNull(sampler, "sampler must exist for cardinality sub-aggs");
    Map<String, AggregationBuilder> samplerSubs = new LinkedHashMap<>();
    for (AggregationBuilder b : sampler.getSubAggregations()) {
      samplerSubs.put(b.getName(), b);
    }

    assertTrue(
        samplerSubs.containsKey(TableStatisticCollector.CARDINALITY_PREFIX + "user.name"),
        "cardinality sub-agg name must include the dotted path");
    assertTrue(
        samplerSubs.containsKey(TableStatisticCollector.CARDINALITY_PREFIX + "user.age"),
        "cardinality sub-agg name must include the dotted path");
    assertTrue(
        topLevel.containsKey(TableStatisticCollector.MIN_PREFIX + "user.age"),
        "top-level min name must include the dotted path");
    assertTrue(
        topLevel.containsKey(TableStatisticCollector.MAX_PREFIX + "user.age"),
        "top-level max name must include the dotted path");
  }

  @Test
  void refreshAsync_emptyFieldTypes_producesDocCountOnlyStat() {
    // Degenerate but realistic: the caller supplies no field types (e.g. the REST
    // /analyze endpoint on an index with only skipped-type fields, or a mapping fetch
    // that returned empty). refreshAsync must still issue a valid search and persist
    // a TableStatistic with just the docCount populated.
    mockGetRaw(Optional.empty());
    mockPutStatusSuccess();
    mockSearchSuccess(buildResponse(50L, null));
    mockPutSuccess();

    collector.refreshAsync("empty-idx", Map.of());

    ArgumentCaptor<TableStatistic> captor = ArgumentCaptor.forClass(TableStatistic.class);
    verify(storage).put(eq("empty-idx"), captor.capture(), any());
    TableStatistic stat = captor.getValue();
    assertEquals(50L, stat.getDocCount());
    assertTrue(stat.getFields().isEmpty(), "no fields supplied → no field statistics");
  }

  // ---- refreshAsync: 3-arg completion listener ----------------------------

  @Test
  public void refreshAsyncThreeArgInvokesCompletionOnSuccess() {
    // Arrange: getRaw returns empty → proceed to search; search returns valid
    // response; storage.put succeeds.
    mockGetRaw(Optional.empty());
    mockPutStatusSuccess();
    mockSearchSuccess(buildResponse(10L, null));
    mockPutSuccess();

    AtomicInteger successCount = new AtomicInteger();
    collector.refreshAsync(
        "logs-a",
        Map.of(),
        ActionListener.wrap(v -> successCount.incrementAndGet(), e -> fail(e.getMessage())));

    assertEquals(1, successCount.get());
  }

  @Test
  public void refreshAsyncDoesNotWriteFailedOnRejection() {
    mockGetRaw(Optional.empty());
    doAnswer(
            inv -> {
              ActionListener<SearchResponse> l = inv.getArgument(1);
              l.onFailure(new OpenSearchRejectedExecutionException("queue is full"));
              return null;
            })
        .when(nodeClient)
        .search(any(), any());

    ArgumentCaptor<String> statusCaptor = ArgumentCaptor.forClass(String.class);
    AtomicInteger completions = new AtomicInteger();

    collector.refreshAsync(
        "logs-a",
        Map.of(),
        ActionListener.wrap(v -> completions.incrementAndGet(), e -> fail(e.getMessage())));

    verify(storage, atLeastOnce()).putStatus(anyString(), statusCaptor.capture(), any());
    assertFalse(
        statusCaptor.getAllValues().contains(TableStatistic.STATUS_FAILED),
        "FAILED must not be written on rejection");
    assertEquals(1, completions.get());
  }

  // ---- helpers ------------------------------------------------------------

  private SearchResponse buildResponse(long totalHits, InternalSampler sampler) {
    SearchResponse response = mock(SearchResponse.class);
    SearchHits hits =
        new SearchHits(
            new org.opensearch.search.SearchHit[0],
            new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO),
            1.0F);
    when(response.getHits()).thenReturn(hits);
    Aggregations topLevel = mock(Aggregations.class);
    when(topLevel.get(TableStatisticCollector.SAMPLER_AGG)).thenReturn(sampler);
    when(response.getAggregations()).thenReturn(topLevel);
    return response;
  }

  /** Variant that lets the caller supply a pre-configured top-level {@link Aggregations}. */
  private SearchResponse buildResponseWithTopAggs(long totalHits, Aggregations topLevel) {
    SearchResponse response = mock(SearchResponse.class);
    SearchHits hits =
        new SearchHits(
            new org.opensearch.search.SearchHit[0],
            new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO),
            1.0F);
    when(response.getHits()).thenReturn(hits);
    when(response.getAggregations()).thenReturn(topLevel);
    return response;
  }

  private void mockGetRaw(Optional<Map<String, Object>> result) {
    doAnswer(
            invocation -> {
              ActionListener<Optional<Map<String, Object>>> listener = invocation.getArgument(1);
              listener.onResponse(result);
              return null;
            })
        .when(storage)
        .getRaw(any(), any());
  }

  private void mockPutStatusSuccess() {
    doAnswer(
            invocation -> {
              ActionListener<Void> listener = invocation.getArgument(2);
              listener.onResponse(null);
              return null;
            })
        .when(storage)
        .putStatus(any(), any(), any());
  }

  private void mockPutSuccess() {
    doAnswer(
            invocation -> {
              ActionListener<Void> listener = invocation.getArgument(2);
              listener.onResponse(null);
              return null;
            })
        .when(storage)
        .put(any(), any(), any());
  }

  private void mockSearchSuccess(SearchResponse response) {
    doAnswer(
            invocation -> {
              ActionListener<SearchResponse> listener = invocation.getArgument(1);
              listener.onResponse(response);
              return null;
            })
        .when(nodeClient)
        .search(any(SearchRequest.class), any());
  }

  private void mockSearchFailure(Exception e) {
    doAnswer(
            invocation -> {
              ActionListener<SearchResponse> listener = invocation.getArgument(1);
              listener.onFailure(e);
              return null;
            })
        .when(nodeClient)
        .search(any(SearchRequest.class), any());
  }
}
