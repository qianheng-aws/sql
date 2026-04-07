/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.math;

import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.opensearch.sql.calcite.utils.MathUtils;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * Safe arithmetic functions that throw ArithmeticException on integer/long overflow instead of
 * silently wrapping. This applies to addition, subtraction, and multiplication of integral types.
 */
public class SafeArithmeticFunction {

  /** Safe addition: uses Math.addExact for integral types to detect overflow. */
  public static class SafeAddFunction extends ImplementorUDF {
    public SafeAddFunction() {
      super(new SafeAddImplementor(), NullPolicy.ANY);
    }

    @Override
    public SqlReturnTypeInference getReturnTypeInference() {
      return ReturnTypes.LEAST_RESTRICTIVE.andThen(SqlTypeTransforms.FORCE_NULLABLE);
    }

    @Override
    public UDFOperandMetadata getOperandMetadata() {
      return PPLOperandTypes.NUMERIC_NUMERIC;
    }
  }

  /** Safe subtraction: uses Math.subtractExact for integral types to detect overflow. */
  public static class SafeSubtractFunction extends ImplementorUDF {
    public SafeSubtractFunction() {
      super(new SafeSubtractImplementor(), NullPolicy.ANY);
    }

    @Override
    public SqlReturnTypeInference getReturnTypeInference() {
      return ReturnTypes.LEAST_RESTRICTIVE.andThen(SqlTypeTransforms.FORCE_NULLABLE);
    }

    @Override
    public UDFOperandMetadata getOperandMetadata() {
      return PPLOperandTypes.NUMERIC_NUMERIC;
    }
  }

  /** Safe multiplication: uses Math.multiplyExact for integral types to detect overflow. */
  public static class SafeMultiplyFunction extends ImplementorUDF {
    public SafeMultiplyFunction() {
      super(new SafeMultiplyImplementor(), NullPolicy.ANY);
    }

    @Override
    public SqlReturnTypeInference getReturnTypeInference() {
      return ReturnTypes.LEAST_RESTRICTIVE.andThen(SqlTypeTransforms.FORCE_NULLABLE);
    }

    @Override
    public UDFOperandMetadata getOperandMetadata() {
      return PPLOperandTypes.NUMERIC_NUMERIC;
    }
  }

  /**
   * Helper to perform overflow-safe coercion back to the widest integral type. If both operands are
   * int-width or narrower, the result must fit in an int.
   */
  static Number safeCoerceToWidestIntegralType(Number a, Number b, long value) {
    if (a instanceof Long || b instanceof Long) {
      return value;
    } else if (a instanceof Integer || b instanceof Integer) {
      // Verify result fits in int range
      if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
        throw new ArithmeticException("integer overflow");
      }
      return (int) value;
    } else if (a instanceof Short || b instanceof Short) {
      if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
        throw new ArithmeticException("short overflow");
      }
      return (short) value;
    } else {
      if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
        throw new ArithmeticException("byte overflow");
      }
      return (byte) value;
    }
  }

  /** Implementor for safe addition. */
  public static class SafeAddImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(
          SafeAddImplementor.class,
          "safeAdd",
          Expressions.convert_(Expressions.box(translatedOperands.get(0)), Number.class),
          Expressions.convert_(Expressions.box(translatedOperands.get(1)), Number.class));
    }

    /** Performs overflow-safe addition. */
    public static Number safeAdd(Number a, Number b) {
      if (MathUtils.isIntegral(a) && MathUtils.isIntegral(b)) {
        long result = Math.addExact(a.longValue(), b.longValue());
        return safeCoerceToWidestIntegralType(a, b, result);
      } else if (MathUtils.isDecimal(a) || MathUtils.isDecimal(b)) {
        BigDecimal bd1 =
            a instanceof BigDecimal ? (BigDecimal) a : BigDecimal.valueOf(a.doubleValue());
        BigDecimal bd2 =
            b instanceof BigDecimal ? (BigDecimal) b : BigDecimal.valueOf(b.doubleValue());
        return bd1.add(bd2);
      }
      double result = a.doubleValue() + b.doubleValue();
      return MathUtils.coerceToWidestFloatingType(a, b, result);
    }
  }

  /** Implementor for safe subtraction. */
  public static class SafeSubtractImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(
          SafeSubtractImplementor.class,
          "safeSubtract",
          Expressions.convert_(Expressions.box(translatedOperands.get(0)), Number.class),
          Expressions.convert_(Expressions.box(translatedOperands.get(1)), Number.class));
    }

    /** Performs overflow-safe subtraction. */
    public static Number safeSubtract(Number a, Number b) {
      if (MathUtils.isIntegral(a) && MathUtils.isIntegral(b)) {
        long result = Math.subtractExact(a.longValue(), b.longValue());
        return safeCoerceToWidestIntegralType(a, b, result);
      } else if (MathUtils.isDecimal(a) || MathUtils.isDecimal(b)) {
        BigDecimal bd1 =
            a instanceof BigDecimal ? (BigDecimal) a : BigDecimal.valueOf(a.doubleValue());
        BigDecimal bd2 =
            b instanceof BigDecimal ? (BigDecimal) b : BigDecimal.valueOf(b.doubleValue());
        return bd1.subtract(bd2);
      }
      double result = a.doubleValue() - b.doubleValue();
      return MathUtils.coerceToWidestFloatingType(a, b, result);
    }
  }

  /** Implementor for safe multiplication. */
  public static class SafeMultiplyImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(
          SafeMultiplyImplementor.class,
          "safeMultiply",
          Expressions.convert_(Expressions.box(translatedOperands.get(0)), Number.class),
          Expressions.convert_(Expressions.box(translatedOperands.get(1)), Number.class));
    }

    /** Performs overflow-safe multiplication. */
    public static Number safeMultiply(Number a, Number b) {
      if (MathUtils.isIntegral(a) && MathUtils.isIntegral(b)) {
        long result = Math.multiplyExact(a.longValue(), b.longValue());
        return safeCoerceToWidestIntegralType(a, b, result);
      } else if (MathUtils.isDecimal(a) || MathUtils.isDecimal(b)) {
        BigDecimal bd1 =
            a instanceof BigDecimal ? (BigDecimal) a : BigDecimal.valueOf(a.doubleValue());
        BigDecimal bd2 =
            b instanceof BigDecimal ? (BigDecimal) b : BigDecimal.valueOf(b.doubleValue());
        return bd1.multiply(bd2);
      }
      double result = a.doubleValue() * b.doubleValue();
      return MathUtils.coerceToWidestFloatingType(a, b, result);
    }
  }
}
