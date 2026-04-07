/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DUPLICATION_NULLABLE;
import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration test for GitHub issue #5150: Dedup aggregation pushdown nullifies renamed fields.
 *
 * <p>When a field is renamed via the rename command and a subsequent dedup command operates on a
 * different field, the renamed field should retain its original values.
 */
public class CalciteDedupRenameIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.DUPLICATION_NULLABLE);
  }

  @Test
  public void testRenameThenDedupRetainsRenamedFieldValues() throws IOException {
    // Rename 'name' to 'n', then dedup by 'category' -- 'n' should not be null
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | rename name as n | dedup category | fields category, n",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(actual, rows("X", "A"), rows("Y", "A"), rows("Z", "B"));
  }

  @Test
  public void testRenameMultipleFieldsThenDedupRetainsAllValues() throws IOException {
    // Rename both 'name' and 'category', then dedup by renamed 'category'
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | rename name as n, category as cat | dedup cat | fields cat, n",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(actual, rows("X", "A"), rows("Y", "A"), rows("Z", "B"));
  }

  @Test
  public void testRenameDedupFieldItselfWorks() throws IOException {
    // Rename the dedup field itself -- this should already work
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | rename category as cat | dedup cat | fields cat, name",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(actual, rows("X", "A"), rows("Y", "A"), rows("Z", "B"));
  }
}
