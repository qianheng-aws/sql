/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.hamcrest.Matchers.containsString;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration tests verifying that integer and long overflow in arithmetic operations throws an
 * error instead of silently wrapping.
 */
public class CalciteArithmeticOverflowIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    // Create index with int_field and long_field
    Request createIndex = new Request("PUT", "/test_overflow");
    createIndex.setJsonEntity(
        "{"
            + "\"settings\": {\"number_of_shards\": 1, \"number_of_replicas\": 0},"
            + "\"mappings\": {\"properties\": {"
            + "\"int_field\": {\"type\": \"integer\"},"
            + "\"long_field\": {\"type\": \"long\"}"
            + "}}"
            + "}");
    client().performRequest(createIndex);

    // Insert max-value records
    Request doc1 = new Request("PUT", "/test_overflow/_doc/1?refresh=true");
    doc1.setJsonEntity(
        "{\"int_field\": 2147483647, \"long_field\": 9223372036854775807}");
    client().performRequest(doc1);

    // Insert normal records for non-overflow tests
    Request doc2 = new Request("PUT", "/test_overflow/_doc/2?refresh=true");
    doc2.setJsonEntity("{\"int_field\": 100, \"long_field\": 200}");
    client().performRequest(doc2);
  }

  @Test
  public void testIntegerAdditionOverflow() {
    ResponseException exception =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "source=test_overflow | where int_field = 2147483647"
                        + " | eval overflow = int_field + 1 | fields int_field, overflow"));
    assertThat(exception.getMessage(), containsString("integer overflow"));
  }

  @Test
  public void testLongAdditionOverflow() {
    ResponseException exception =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "source=test_overflow | where long_field = 9223372036854775807"
                        + " | eval overflow = long_field + 1 | fields long_field, overflow"));
    assertThat(exception.getMessage(), containsString("long overflow"));
  }

  @Test
  public void testNormalIntegerArithmetic() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_overflow | where int_field = 100"
                + " | eval result = int_field + 50 | fields int_field, result");
    verifySchema(result, schema("int_field", "integer"), schema("result", "integer"));
    verifyDataRows(result, rows(100, 150));
  }

  @Test
  public void testNormalLongArithmetic() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_overflow | where long_field = 200"
                + " | eval result = long_field + 100 | fields long_field, result");
    verifySchema(result, schema("long_field", "long"), schema("result", "long"));
    verifyDataRows(result, rows(200, 300));
  }

  @Test
  public void testIntegerMultiplicationOverflow() {
    ResponseException exception =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "source=test_overflow | where int_field = 2147483647"
                        + " | eval overflow = int_field * 2 | fields int_field, overflow"));
    assertThat(exception.getMessage(), containsString("integer overflow"));
  }
}
