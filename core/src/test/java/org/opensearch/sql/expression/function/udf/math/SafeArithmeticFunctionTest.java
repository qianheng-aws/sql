/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.math;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.expression.function.udf.math.SafeArithmeticFunction.SafeAddImplementor;
import org.opensearch.sql.expression.function.udf.math.SafeArithmeticFunction.SafeMultiplyImplementor;
import org.opensearch.sql.expression.function.udf.math.SafeArithmeticFunction.SafeSubtractImplementor;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class SafeArithmeticFunctionTest {

  // ==================== Addition Tests ====================

  @Test
  void safeAdd_integer_normal() {
    Number result = SafeAddImplementor.safeAdd(10, 20);
    assertEquals(30, result.intValue());
  }

  @Test
  void safeAdd_integer_max_plus_one_overflows() {
    assertThrows(ArithmeticException.class, () -> SafeAddImplementor.safeAdd(Integer.MAX_VALUE, 1));
  }

  @Test
  void safeAdd_long_normal() {
    Number result = SafeAddImplementor.safeAdd(10L, 20L);
    assertEquals(30L, result.longValue());
  }

  @Test
  void safeAdd_long_max_plus_one_overflows() {
    assertThrows(ArithmeticException.class, () -> SafeAddImplementor.safeAdd(Long.MAX_VALUE, 1L));
  }

  @Test
  void safeAdd_double_no_overflow() {
    Number result = SafeAddImplementor.safeAdd(1.5, 2.5);
    assertEquals(4.0, result.doubleValue(), 0.001);
  }

  @Test
  void safeAdd_float_no_overflow() {
    Number result = SafeAddImplementor.safeAdd(1.5f, 2.5f);
    assertEquals(4.0f, result.floatValue(), 0.001);
  }

  @Test
  void safeAdd_mixed_int_long_promotes_to_long() {
    // int MAX + long 1 => should succeed as long (result fits in long)
    Number result = SafeAddImplementor.safeAdd(Integer.MAX_VALUE, 1L);
    assertEquals(2147483648L, result.longValue());
    assertEquals(Long.class, result.getClass());
  }

  // ==================== Subtraction Tests ====================

  @Test
  void safeSubtract_integer_normal() {
    Number result = SafeSubtractImplementor.safeSubtract(20, 10);
    assertEquals(10, result.intValue());
  }

  @Test
  void safeSubtract_integer_min_minus_one_overflows() {
    assertThrows(
        ArithmeticException.class,
        () -> SafeSubtractImplementor.safeSubtract(Integer.MIN_VALUE, 1));
  }

  @Test
  void safeSubtract_long_normal() {
    Number result = SafeSubtractImplementor.safeSubtract(20L, 10L);
    assertEquals(10L, result.longValue());
  }

  @Test
  void safeSubtract_long_min_minus_one_overflows() {
    assertThrows(
        ArithmeticException.class, () -> SafeSubtractImplementor.safeSubtract(Long.MIN_VALUE, 1L));
  }

  @Test
  void safeSubtract_double_no_overflow() {
    Number result = SafeSubtractImplementor.safeSubtract(5.5, 2.5);
    assertEquals(3.0, result.doubleValue(), 0.001);
  }

  // ==================== Multiplication Tests ====================

  @Test
  void safeMultiply_integer_normal() {
    Number result = SafeMultiplyImplementor.safeMultiply(10, 20);
    assertEquals(200, result.intValue());
  }

  @Test
  void safeMultiply_integer_overflow_throws() {
    assertThrows(
        ArithmeticException.class,
        () -> SafeMultiplyImplementor.safeMultiply(Integer.MAX_VALUE, 2));
  }

  @Test
  void safeMultiply_long_normal() {
    Number result = SafeMultiplyImplementor.safeMultiply(10L, 20L);
    assertEquals(200L, result.longValue());
  }

  @Test
  void safeMultiply_long_overflow_throws() {
    assertThrows(
        ArithmeticException.class, () -> SafeMultiplyImplementor.safeMultiply(Long.MAX_VALUE, 2L));
  }

  @Test
  void safeMultiply_double_no_overflow() {
    Number result = SafeMultiplyImplementor.safeMultiply(2.5, 3.0);
    assertEquals(7.5, result.doubleValue(), 0.001);
  }

  // ==================== Type Coercion Tests ====================

  @Test
  void safeAdd_preserves_integer_type() {
    Number result = SafeAddImplementor.safeAdd(1, 2);
    assertEquals(Integer.class, result.getClass());
  }

  @Test
  void safeAdd_preserves_long_type() {
    Number result = SafeAddImplementor.safeAdd(1L, 2L);
    assertEquals(Long.class, result.getClass());
  }

  @Test
  void safeAdd_widens_int_to_long() {
    Number result = SafeAddImplementor.safeAdd(1, 2L);
    assertEquals(Long.class, result.getClass());
  }

  @Test
  void safeAdd_preserves_double_type() {
    Number result = SafeAddImplementor.safeAdd(1.0, 2.0);
    assertEquals(Double.class, result.getClass());
  }

  @Test
  void safeAdd_preserves_float_type() {
    Number result = SafeAddImplementor.safeAdd(1.0f, 2.0f);
    assertEquals(Float.class, result.getClass());
  }
}
