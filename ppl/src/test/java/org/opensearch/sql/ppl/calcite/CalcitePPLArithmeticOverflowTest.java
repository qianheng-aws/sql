/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertThrows;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

/** Tests that integer and long arithmetic overflow throws an error instead of wrapping silently. */
public class CalcitePPLArithmeticOverflowTest extends CalcitePPLAbstractTest {

  public CalcitePPLArithmeticOverflowTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testIntegerAdditionOverflow() {
    // 2147483647 + 1 should throw ArithmeticException, not wrap to -2147483648
    String ppl = "source=EMP | eval overflow = 2147483647 + 1 | fields EMPNO, overflow";
    RelNode root = getRelNode(ppl);
    assertThrows(
        RuntimeException.class,
        () -> {
          try (PreparedStatement ps =
              org.apache.calcite.tools.RelRunners.run(root)) {
            CalciteAssert.toString(ps.executeQuery());
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Test
  public void testLongAdditionOverflow() {
    // 9223372036854775807 + 1 should throw ArithmeticException
    String ppl = "source=EMP | eval overflow = 9223372036854775807 + 1 | fields EMPNO, overflow";
    RelNode root = getRelNode(ppl);
    assertThrows(
        RuntimeException.class,
        () -> {
          try (PreparedStatement ps =
              org.apache.calcite.tools.RelRunners.run(root)) {
            CalciteAssert.toString(ps.executeQuery());
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Test
  public void testIntegerSubtractionOverflow() {
    // -2147483648 - 1 should throw ArithmeticException
    String ppl = "source=EMP | eval overflow = -2147483648 - 1 | fields EMPNO, overflow";
    RelNode root = getRelNode(ppl);
    assertThrows(
        RuntimeException.class,
        () -> {
          try (PreparedStatement ps =
              org.apache.calcite.tools.RelRunners.run(root)) {
            CalciteAssert.toString(ps.executeQuery());
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Test
  public void testIntegerMultiplicationOverflow() {
    // 2147483647 * 2 should throw ArithmeticException
    String ppl = "source=EMP | eval overflow = 2147483647 * 2 | fields EMPNO, overflow";
    RelNode root = getRelNode(ppl);
    assertThrows(
        RuntimeException.class,
        () -> {
          try (PreparedStatement ps =
              org.apache.calcite.tools.RelRunners.run(root)) {
            CalciteAssert.toString(ps.executeQuery());
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Test
  public void testNormalIntegerArithmeticStillWorks() {
    // Normal arithmetic should not be affected
    String ppl = "source=EMP | eval result = 100 + 200 | fields EMPNO, result";
    RelNode root = getRelNode(ppl);
    String expectedResult =
        "EMPNO=7369; result=300\n"
            + "EMPNO=7499; result=300\n"
            + "EMPNO=7521; result=300\n"
            + "EMPNO=7566; result=300\n"
            + "EMPNO=7654; result=300\n"
            + "EMPNO=7698; result=300\n"
            + "EMPNO=7782; result=300\n"
            + "EMPNO=7788; result=300\n"
            + "EMPNO=7839; result=300\n"
            + "EMPNO=7844; result=300\n"
            + "EMPNO=7876; result=300\n"
            + "EMPNO=7900; result=300\n"
            + "EMPNO=7902; result=300\n"
            + "EMPNO=7934; result=300\n";
    verifyResult(root, expectedResult);
  }

  @Test
  public void testNormalIntegerSubtractionStillWorks() {
    String ppl = "source=EMP | eval result = 500 - 200 | fields EMPNO, result";
    RelNode root = getRelNode(ppl);
    String expectedResult =
        "EMPNO=7369; result=300\n"
            + "EMPNO=7499; result=300\n"
            + "EMPNO=7521; result=300\n"
            + "EMPNO=7566; result=300\n"
            + "EMPNO=7654; result=300\n"
            + "EMPNO=7698; result=300\n"
            + "EMPNO=7782; result=300\n"
            + "EMPNO=7788; result=300\n"
            + "EMPNO=7839; result=300\n"
            + "EMPNO=7844; result=300\n"
            + "EMPNO=7876; result=300\n"
            + "EMPNO=7900; result=300\n"
            + "EMPNO=7902; result=300\n"
            + "EMPNO=7934; result=300\n";
    verifyResult(root, expectedResult);
  }

  @Test
  public void testNormalIntegerMultiplicationStillWorks() {
    String ppl = "source=EMP | eval result = 10 * 30 | fields EMPNO, result";
    RelNode root = getRelNode(ppl);
    String expectedResult =
        "EMPNO=7369; result=300\n"
            + "EMPNO=7499; result=300\n"
            + "EMPNO=7521; result=300\n"
            + "EMPNO=7566; result=300\n"
            + "EMPNO=7654; result=300\n"
            + "EMPNO=7698; result=300\n"
            + "EMPNO=7782; result=300\n"
            + "EMPNO=7788; result=300\n"
            + "EMPNO=7839; result=300\n"
            + "EMPNO=7844; result=300\n"
            + "EMPNO=7876; result=300\n"
            + "EMPNO=7900; result=300\n"
            + "EMPNO=7902; result=300\n"
            + "EMPNO=7934; result=300\n";
    verifyResult(root, expectedResult);
  }

  @Test
  public void testDoubleArithmeticNoOverflowCheck() {
    // Double arithmetic should not throw on large values (it uses IEEE 754)
    String ppl = "source=EMP | eval result = 1.0E308 + 1.0E308 | fields EMPNO, result";
    RelNode root = getRelNode(ppl);
    // Should not throw - doubles handle overflow via Infinity
    try (PreparedStatement ps =
        org.apache.calcite.tools.RelRunners.run(root)) {
      CalciteAssert.toString(ps.executeQuery());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
