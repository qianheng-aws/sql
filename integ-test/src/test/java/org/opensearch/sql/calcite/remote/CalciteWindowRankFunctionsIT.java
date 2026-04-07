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

/**
 * Integration test for issue #5168: Window functions row_number, rank, dense_rank in
 * eventstats/streamstats.
 */
public class CalciteWindowRankFunctionsIT extends PPLIntegTestCase {
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
                "source=%s | eventstats row_number() as rn by state", TEST_INDEX_STATE_COUNTRY));
    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("rn", "bigint"));
  }

  @Test
  public void testEventstatsRank() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats rank() as rnk by country", TEST_INDEX_STATE_COUNTRY));
    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("rnk", "bigint"));
  }

  @Test
  public void testEventstatsDenseRank() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats dense_rank() as dr by country", TEST_INDEX_STATE_COUNTRY));
    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("dr", "bigint"));
  }

  @Test
  public void testStreamstatsRowNumber() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats row_number() as rn by state", TEST_INDEX_STATE_COUNTRY));
    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("rn", "bigint"));
  }

  @Test
  public void testStreamstatsRank() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats rank() as rnk by country", TEST_INDEX_STATE_COUNTRY));
    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("rnk", "bigint"));
  }

  @Test
  public void testStreamstatsDenseRank() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats dense_rank() as dr by country", TEST_INDEX_STATE_COUNTRY));
    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("dr", "bigint"));
  }
}
