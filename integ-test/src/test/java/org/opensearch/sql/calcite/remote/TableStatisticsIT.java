/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration tests covering the end-to-end table-statistics stack:
 *
 * <ul>
 *   <li>flag off &mdash; optimizer receives no per-column stats; filter rowcount uses Calcite's
 *       default {@code guessSelectivity} (EQUALS = 0.15).
 *   <li>flag on, index not yet analyzed &mdash; {@code getStatistic()} returns UNKNOWN; rowcount
 *       still uses {@code maxResultWindow} baseline.
 *   <li>flag on, after {@code POST /_statistics/{index}/analyze} &mdash; aggregate rowcount uses
 *       stored per-field cardinality; filter rowcount uses 1/cardinality.
 *   <li>REST read &mdash; {@code GET /_statistics/{index}} returns a well-formed COMPLETED
 *       document.
 * </ul>
 *
 * <p>Uses the {@code explain cost} output shape: {@code rowcount = <double>, cumulative cost ...}.
 * Rowcounts are extracted via regex because Calcite produces double-precision values that don't
 * round-trip cleanly through any existing assertion helper.
 */
public class TableStatisticsIT extends PPLIntegTestCase {

  /** Test index with 20 docs / 5 distinct status values — enough separation from defaults. */
  private static final String INDEX = "table_stats_it";

  // The explain-cost output embeds the plan as a JSON-encoded string, so line breaks and
  // parens inside conditions (e.g. `[=($1, 'OK')]`) appear literally. Using a non-greedy wildcard
  // past the opening paren is safer than trying to enumerate allowed characters.
  private static final Pattern LOGICAL_FILTER_ROWCOUNT =
      Pattern.compile("LogicalFilter\\(.*?: rowcount = ([0-9.E+\\-]+)");

  private static final Pattern LOGICAL_AGGREGATE_ROWCOUNT =
      Pattern.compile("LogicalAggregate\\(.*?: rowcount = ([0-9.E+\\-]+)");

  private static final Pattern LOGICAL_SCAN_ROWCOUNT =
      Pattern.compile("CalciteLogicalIndexScan\\(.*?: rowcount = ([0-9.E+\\-]+)");

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    seedIndex();
  }

  @Override
  public void tearDown() throws Exception {
    disableTableStatistics();
    try {
      client().performRequest(new Request("DELETE", "/" + INDEX));
    } catch (Exception ignored) {
      // idempotent
    }
    try {
      // cleaning the statistics system index avoids leakage between runs
      client().performRequest(new Request("DELETE", "/.opensearch-statistics"));
    } catch (Exception ignored) {
      // may not exist
    }
    super.tearDown();
  }

  @Test
  public void flagOff_filterUsesDefaultGuessSelectivity() throws IOException {
    disableTableStatistics();

    String plan = explainCost("source=" + INDEX + " | where status = 'OK'");

    // No stats → CalciteLogicalIndexScan reports maxResultWindow baseline (10 000).
    double scanRows = extractRowcount(plan, LOGICAL_SCAN_ROWCOUNT);
    assertEquals(10_000.0, scanRows, 0.5);

    // LogicalFilter: 10000 * 0.15 = 1500 (Calcite's default EQUALS selectivity).
    double filterRows = extractRowcount(plan, LOGICAL_FILTER_ROWCOUNT);
    assertEquals(1500.0, filterRows, 1.0);
  }

  @Test
  public void flagOn_noStatsYet_stillFallsBackToDefaults() throws IOException {
    enableTableStatistics();

    String plan = explainCost("source=" + INDEX + " | where status = 'OK'");

    // getStatistic() times out / misses → UNKNOWN → same fallback as flag-off.
    double scanRows = extractRowcount(plan, LOGICAL_SCAN_ROWCOUNT);
    assertEquals(10_000.0, scanRows, 0.5);
    double filterRows = extractRowcount(plan, LOGICAL_FILTER_ROWCOUNT);
    assertEquals(1500.0, filterRows, 1.0);
  }

  @Test
  public void flagOn_afterAnalyze_usesRealCardinality() throws IOException {
    enableTableStatistics();
    analyzeAndWaitForCompleted(INDEX);

    // Filter: WHERE status = 'OK' — 20 docs / 5 distinct status ⇒ 1/5 selectivity.
    String filterPlan = explainCost("source=" + INDEX + " | where status = 'OK'");
    double scanRows = extractRowcount(filterPlan, LOGICAL_SCAN_ROWCOUNT);
    assertEquals(20.0, scanRows, 0.5);
    double filterRows = extractRowcount(filterPlan, LOGICAL_FILTER_ROWCOUNT);
    // 20 * 1/5 = 4.0
    assertEquals(4.0, filterRows, 0.1);

    // Aggregate: stats count() by status — rowcount = cardinality(status) = 5.
    String aggPlan = explainCost("source=" + INDEX + " | stats count() by status");
    double aggRows = extractRowcount(aggPlan, LOGICAL_AGGREGATE_ROWCOUNT);
    assertEquals(5.0, aggRows, 0.1);
  }

  /**
   * Smoke-test the scheduler settings lifecycle. End-to-end "cron actually rewrites a stale doc" is
   * covered by the unit tests (scheduler + task) and validated manually on a live cluster (see spec
   * §1). Reproducing real 5 s-interval cron in-process here fights the shared test cluster: any
   * in-flight refresh surviving past tearDown poisons the next test's analyze via the
   * GENERATING-marker dedup. So we validate the contract we can observe cheaply: settings take
   * effect and can be reset.
   */
  @Test
  public void cronSettingsAreLive() throws IOException {
    updateCronSetting("refresh_interval", "30s");
    try {
      Response r = client().performRequest(new Request("GET", "/_cluster/settings"));
      JSONObject body = new JSONObject(getResponseBody(r, true));
      String got =
          body.getJSONObject("persistent")
              .getJSONObject("plugins")
              .getJSONObject("calcite")
              .getJSONObject("table_statistics")
              .getString("refresh_interval");
      assertEquals("30s", got);
    } finally {
      resetCronSetting("refresh_interval");
    }
  }

  @Test
  public void restApi_getReturnsCompletedDocument() throws IOException {
    enableTableStatistics();
    analyzeAndWaitForCompleted(INDEX);

    Response resp =
        client().performRequest(new Request("GET", "/_plugins/_sql/_statistics/" + INDEX));
    assertEquals(200, resp.getStatusLine().getStatusCode());
    JSONObject body = new JSONObject(getResponseBody(resp, true));

    assertEquals("COMPLETED", body.getString("status"));
    assertTrue("doc_count missing", body.has("doc_count"));
    assertEquals(20, body.getInt("doc_count"));
    assertTrue("last_updated_time missing", body.has("last_updated_time"));

    JSONObject fields = body.getJSONObject("fields");
    assertTrue("status field missing", fields.has("status"));
    JSONObject statusField = fields.getJSONObject("status");
    assertEquals(5, statusField.getInt("unique_count"));
    assertEquals(0.0, statusField.getDouble("null_ratio"), 1e-9);
  }

  // ---- helpers ----

  private void seedIndex() throws IOException {
    // Create with an explicit mapping so `status` has a keyword sub-field — the collector's
    // text→keyword resolution depends on it.
    Request create = new Request("PUT", "/" + INDEX);
    create.setJsonEntity(
        "{\"mappings\":{\"properties\":"
            + "{\"status\":{\"type\":\"keyword\"},\"latency\":{\"type\":\"long\"}}}}");
    client().performRequest(create);

    // 20 docs, 5 distinct status values (4 each), latency 0-19 (effectively unique).
    StringBuilder bulk = new StringBuilder();
    String[] statuses = {"OK", "WARN", "ERROR", "INFO", "DEBUG"};
    for (int i = 0; i < 20; i++) {
      bulk.append("{\"index\":{\"_id\":\"").append(i).append("\"}}\n");
      bulk.append("{\"status\":\"")
          .append(statuses[i % statuses.length])
          .append("\",\"latency\":")
          .append(i)
          .append("}\n");
    }
    Request bulkReq = new Request("POST", "/" + INDEX + "/_bulk?refresh=true");
    bulkReq.setJsonEntity(bulk.toString());
    client().performRequest(bulkReq);
  }

  private void enableTableStatistics() throws IOException {
    SQLIntegTestCase.updateClusterSettings(
        new SQLIntegTestCase.ClusterSetting(
            "persistent", Settings.Key.TABLE_STATISTICS_ENABLED.getKeyValue(), "true"));
  }

  private void disableTableStatistics() throws IOException {
    SQLIntegTestCase.updateClusterSettings(
        new SQLIntegTestCase.ClusterSetting(
            "persistent", Settings.Key.TABLE_STATISTICS_ENABLED.getKeyValue(), "false"));
  }

  private void updateCronSetting(String suffix, String value) throws IOException {
    SQLIntegTestCase.updateClusterSettings(
        new SQLIntegTestCase.ClusterSetting(
            "persistent", "plugins.calcite.table_statistics." + suffix, value));
  }

  private void resetCronSetting(String suffix) {
    try {
      SQLIntegTestCase.updateClusterSettings(
          new SQLIntegTestCase.ClusterSetting(
              "persistent", "plugins.calcite.table_statistics." + suffix, null));
    } catch (IOException ignored) {
      // best-effort cleanup
    }
  }

  /**
   * Kick off async collection via the REST API, then poll the GET endpoint until it reports {@code
   * COMPLETED}. The collector is fire-and-forget; in a healthy cluster it finishes well under a
   * second on 20 documents.
   */
  private void analyzeAndWaitForCompleted(String index) throws IOException {
    client()
        .performRequest(new Request("POST", "/_plugins/_sql/_statistics/" + index + "/analyze"));

    // Generous 30 s budget — first analyze in a fresh cluster has to lazily create the
    // .opensearch-statistics system index before the collector's GENERATING/put path even starts.
    awaitTrue(
        () -> {
          try {
            Response r =
                client().performRequest(new Request("GET", "/_plugins/_sql/_statistics/" + index));
            JSONObject body = new JSONObject(getResponseBody(r, true));
            return "COMPLETED".equals(body.optString("status"));
          } catch (IOException e) {
            return false;
          }
        },
        30,
        "statistics for " + index + " never reached COMPLETED");
  }

  private static void awaitTrue(BooleanSupplier cond, long seconds, String msg) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
    while (System.nanoTime() < deadline) {
      if (cond.getAsBoolean()) return;
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError("interrupted while waiting");
      }
    }
    throw new AssertionError(msg);
  }

  private String explainCost(String query) throws IOException {
    return explainQueryToString(query, ExplainMode.COST);
  }

  private static double extractRowcount(String plan, Pattern pattern) {
    Matcher m = pattern.matcher(plan);
    if (!m.find()) {
      throw new AssertionError(
          String.format(Locale.ROOT, "pattern %s did not match. plan=%s", pattern.pattern(), plan));
    }
    return Double.parseDouble(m.group(1));
  }
}
