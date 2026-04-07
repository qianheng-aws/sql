/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteEventstatsRankingIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.STATE_COUNTRY);
  }

  @Test
  public void testEventstatsRowNumber() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats row_number() by country", TEST_INDEX_STATE_COUNTRY));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("row_number()", "bigint"));
    verifyNumOfRows(actual, 4);
  }

  @Test
  public void testEventstatsRank() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | eventstats rank() by country", TEST_INDEX_STATE_COUNTRY));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("rank()", "bigint"));
    verifyNumOfRows(actual, 4);
  }

  @Test
  public void testEventstatsDenseRank() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats dense_rank() by country", TEST_INDEX_STATE_COUNTRY));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("dense_rank()", "bigint"));
    verifyNumOfRows(actual, 4);
  }

  @Test
  public void testStreamstatsRowNumber() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats row_number() by country", TEST_INDEX_STATE_COUNTRY));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("row_number()", "bigint"));
    verifyNumOfRows(actual, 4);
  }

  @Test
  public void testStreamstatsRank() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | streamstats rank() by country", TEST_INDEX_STATE_COUNTRY));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("rank()", "bigint"));
    verifyNumOfRows(actual, 4);
  }

  @Test
  public void testStreamstatsDenseRank() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats dense_rank() by country", TEST_INDEX_STATE_COUNTRY));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("dense_rank()", "bigint"));
    verifyNumOfRows(actual, 4);
  }
}
