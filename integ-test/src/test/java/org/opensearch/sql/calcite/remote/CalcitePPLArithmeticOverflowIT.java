/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLArithmeticOverflowIT extends PPLIntegTestCase {

  private static final String TEST_INDEX = "test_arithmetic_overflow";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    // Delete and recreate test index
    try {
      client().performRequest(new Request("DELETE", "/" + TEST_INDEX));
    } catch (ResponseException e) {
      // Index may not exist yet - ignore
    }

    // Create test index
    Request createIndex = new Request("PUT", "/" + TEST_INDEX);
    createIndex.setJsonEntity(
        "{"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"int_field\": { \"type\": \"integer\" },"
            + "    \"long_field\": { \"type\": \"long\" }"
            + "  }"
            + "}"
            + "}");
    client().performRequest(createIndex);

    // Insert max value record
    Request insert = new Request("PUT", "/" + TEST_INDEX + "/_doc/1?refresh=true");
    insert.setJsonEntity(
        "{" + "\"int_field\": 2147483647," + "\"long_field\": 9223372036854775807" + "}");
    client().performRequest(insert);

    // Insert normal record
    Request insert2 = new Request("PUT", "/" + TEST_INDEX + "/_doc/2?refresh=true");
    insert2.setJsonEntity("{" + "\"int_field\": 100," + "\"long_field\": 200" + "}");
    client().performRequest(insert2);
  }

  @Test
  public void testIntegerAdditionOverflowThrowsError() {
    assertThrows(
        ResponseException.class,
        () ->
            executeQuery(
                String.format(
                    "source=%s | where int_field = 2147483647 | eval overflow = int_field + 1"
                        + " | fields int_field, overflow",
                    TEST_INDEX)));
  }

  @Test
  public void testLongAdditionOverflowThrowsError() {
    assertThrows(
        ResponseException.class,
        () ->
            executeQuery(
                String.format(
                    "source=%s | where long_field = 9223372036854775807 | eval overflow ="
                        + " long_field + 1 | fields long_field, overflow",
                    TEST_INDEX)));
  }

  @Test
  public void testIntegerMultiplicationOverflowThrowsError() {
    assertThrows(
        ResponseException.class,
        () ->
            executeQuery(
                String.format(
                    "source=%s | where int_field = 2147483647 | eval overflow = int_field * 2"
                        + " | fields int_field, overflow",
                    TEST_INDEX)));
  }

  @Test
  public void testNormalArithmeticStillWorks() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where int_field = 100 | eval sum = int_field + 50, diff ="
                    + " int_field - 50, prod = int_field * 2 | fields int_field, sum, diff, prod",
                TEST_INDEX));
    verifyDataRows(result, rows(100, 150, 50, 200));
  }

  @Test
  public void testNormalLongArithmeticStillWorks() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where long_field = 200 | eval sum = long_field + 100, diff ="
                    + " long_field - 100, prod = long_field * 2 | fields long_field, sum, diff,"
                    + " prod",
                TEST_INDEX));
    verifyDataRows(result, rows(200, 300, 100, 400));
  }
}
