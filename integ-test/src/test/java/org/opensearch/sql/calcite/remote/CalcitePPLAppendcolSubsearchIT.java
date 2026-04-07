/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration test for https://github.com/opensearch-project/sql/issues/5186 appendcol subsearch
 * should inherit schema-expanding transformations (eval, spath) from the main pipeline.
 */
public class CalcitePPLAppendcolSubsearchIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void testAppendColSubsearchUsesMainPipelineEvalField() throws IOException {
    // The subsearch references 'g' which is created by eval in the main pipeline.
    // Before the fix, this would fail with a field-not-found error because the
    // subsearch only had access to the raw table columns.
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval g = gender | appendcol [ stats count() as cnt by g ]"
                    + " | fields firstname, g, cnt | head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(
        actual, schema("firstname", "string"), schema("g", "string"), schema("cnt", "bigint"));
    // cnt should not be null for the first rows -- the subsearch successfully resolved 'g'
    JSONObject row1 = actual.getJSONArray("datarows").getJSONArray(0);
    org.junit.Assert.assertFalse(
        "cnt should not be null when subsearch can resolve eval field", row1.isNull(2));
  }
}
