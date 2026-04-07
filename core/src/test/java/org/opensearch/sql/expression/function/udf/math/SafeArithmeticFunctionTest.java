/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.math;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.expression.function.udf.math.SafeArithmeticFunction.SafeArithmeticImplementor;

/** Unit tests for {@link SafeArithmeticFunction}'s safe arithmetic methods. */
class SafeArithmeticFunctionTest {

  // ---- Addition ----

  @Test
  void safeIntegralAddNormal() {
    Number result = SafeArithmeticImplementor.safeIntegralAdd(10, 20);
    assertEquals(30, result.intValue());
    assertInstanceOf(Integer.class, result);
  }

  @Test
  void safeIntegralAddIntOverflow() {
    assertThrows(
        ArithmeticException.class,
        () -> SafeArithmeticImplementor.safeIntegralAdd(Integer.MAX_VALUE, 1));
  }

  @Test
  void safeIntegralAddIntUnderflow() {
    assertThrows(
        ArithmeticException.class,
        () -> SafeArithmeticImplementor.safeIntegralAdd(Integer.MIN_VALUE, -1));
  }

  @Test
  void safeIntegralAddLongOverflow() {
    assertThrows(
        ArithmeticException.class,
        () -> SafeArithmeticImplementor.safeIntegralAdd(Long.MAX_VALUE, 1L));
  }

  @Test
  void safeIntegralAddLongUnderflow() {
    assertThrows(
        ArithmeticException.class,
        () -> SafeArithmeticImplementor.safeIntegralAdd(Long.MIN_VALUE, -1L));
  }

  @Test
  void safeIntegralAddReturnsLongWhenEitherOperandIsLong() {
    Number result = SafeArithmeticImplementor.safeIntegralAdd(10, 20L);
    assertEquals(30L, result.longValue());
    assertInstanceOf(Long.class, result);
  }

  // ---- Subtraction ----

  @Test
  void safeIntegralSubtractNormal() {
    Number result = SafeArithmeticImplementor.safeIntegralSubtract(30, 10);
    assertEquals(20, result.intValue());
    assertInstanceOf(Integer.class, result);
  }

  @Test
  void safeIntegralSubtractIntOverflow() {
    assertThrows(
        ArithmeticException.class,
        () -> SafeArithmeticImplementor.safeIntegralSubtract(Integer.MIN_VALUE, 1));
  }

  @Test
  void safeIntegralSubtractLongOverflow() {
    assertThrows(
        ArithmeticException.class,
        () -> SafeArithmeticImplementor.safeIntegralSubtract(Long.MIN_VALUE, 1L));
  }

  // ---- Multiplication ----

  @Test
  void safeIntegralMultiplyNormal() {
    Number result = SafeArithmeticImplementor.safeIntegralMultiply(10, 20);
    assertEquals(200, result.intValue());
    assertInstanceOf(Integer.class, result);
  }

  @Test
  void safeIntegralMultiplyIntOverflow() {
    assertThrows(
        ArithmeticException.class,
        () -> SafeArithmeticImplementor.safeIntegralMultiply(Integer.MAX_VALUE, 2));
  }

  @Test
  void safeIntegralMultiplyLongOverflow() {
    assertThrows(
        ArithmeticException.class,
        () -> SafeArithmeticImplementor.safeIntegralMultiply(Long.MAX_VALUE, 2L));
  }

  // ---- Floating-point (no overflow check) ----

  @Test
  void floatingAddNormal() {
    Number result = SafeArithmeticImplementor.floatingAdd(1.5, 2.5);
    assertEquals(4.0, result.doubleValue(), 0.001);
  }

  @Test
  void floatingAddLargeValues() {
    // Double handles large values via Infinity, no exception
    Number result = SafeArithmeticImplementor.floatingAdd(Double.MAX_VALUE, Double.MAX_VALUE);
    assertEquals(Double.POSITIVE_INFINITY, result.doubleValue());
  }

  @Test
  void floatingSubtractNormal() {
    Number result = SafeArithmeticImplementor.floatingSubtract(5.0, 2.0);
    assertEquals(3.0, result.doubleValue(), 0.001);
  }

  @Test
  void floatingMultiplyNormal() {
    Number result = SafeArithmeticImplementor.floatingMultiply(3.0, 4.0);
    assertEquals(12.0, result.doubleValue(), 0.001);
  }
}
