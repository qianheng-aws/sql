/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.math;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.ImplementableFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.opensearch.sql.calcite.utils.MathUtils;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * Overflow-safe arithmetic operations for integer and long types.
 *
 * <p>Uses {@link Math#addExact}, {@link Math#subtractExact}, and {@link Math#multiplyExact} to
 * detect integer/long overflow and throw {@link ArithmeticException} instead of silently wrapping.
 * Floating-point and decimal arithmetic is unchanged (IEEE 754 semantics).
 *
 * <p>The operators use the same names ({@code +}, {@code -}, {@code *}) as the standard Calcite
 * operators so that logical plan output is unchanged.
 */
public final class SafeArithmeticFunction {

  private SafeArithmeticFunction() {}

  /** The arithmetic operation to perform. */
  public enum Op {
    ADD("+"),
    SUBTRACT("-"),
    MULTIPLY("*");

    final String symbol;

    Op(String symbol) {
      this.symbol = symbol;
    }
  }

  /** Creates a UDF operator for the given arithmetic operation. */
  public static SqlUserDefinedFunction create(Op op) {
    UDFOperandMetadata operandMetadata = PPLOperandTypes.NUMERIC_NUMERIC;
    NotNullImplementor implementor = new SafeArithmeticImplementor(op);
    CallImplementor callImplementor = RexImpTable.createImplementor(implementor, NullPolicy.ANY, false);
    ImplementableFunction function =
        new ImplementableFunction() {
          @Override
          public List<FunctionParameter> getParameters() {
            return List.of();
          }

          @Override
          public CallImplementor getImplementor() {
            return callImplementor;
          }
        };
    SqlIdentifier identifier =
        new SqlIdentifier(Collections.singletonList(op.symbol), null, SqlParserPos.ZERO, null);
    return new SqlUserDefinedFunction(
        identifier,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.LEAST_RESTRICTIVE,
        InferTypes.ANY_NULLABLE,
        operandMetadata,
        function) {
      @Override
      public boolean isDeterministic() {
        return true;
      }

      @Override
      public SqlIdentifier getSqlIdentifier() {
        return null;
      }

      @Override
      public SqlSyntax getSyntax() {
        // Use BINARY syntax so that RelToSqlConverter produces infix notation (a + b)
        // instead of function-call notation +(a, b).
        return SqlSyntax.BINARY;
      }
    };
  }

  /** Implementor that generates code delegating to the static safe-arithmetic methods. */
  public static class SafeArithmeticImplementor implements NotNullImplementor {
    private final Op op;

    public SafeArithmeticImplementor(Op op) {
      this.op = op;
    }

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression left = translatedOperands.get(0);
      Expression right = translatedOperands.get(1);
      RelDataType leftType = call.getOperands().get(0).getType();
      RelDataType rightType = call.getOperands().get(1).getType();

      String methodName;
      if (SqlTypeFamily.INTEGER.contains(leftType) && SqlTypeFamily.INTEGER.contains(rightType)) {
        methodName =
            switch (op) {
              case ADD -> "safeIntegralAdd";
              case SUBTRACT -> "safeIntegralSubtract";
              case MULTIPLY -> "safeIntegralMultiply";
            };
      } else {
        methodName =
            switch (op) {
              case ADD -> "floatingAdd";
              case SUBTRACT -> "floatingSubtract";
              case MULTIPLY -> "floatingMultiply";
            };
      }

      return Expressions.call(
          SafeArithmeticImplementor.class,
          methodName,
          Expressions.convert_(Expressions.box(left), Number.class),
          Expressions.convert_(Expressions.box(right), Number.class));
    }

    // ---- Integral (overflow-checked) methods ----

    /**
     * Overflow-safe integer/long addition. Throws ArithmeticException on overflow. Uses int-level
     * addExact when both operands are Integer (or narrower); uses long-level addExact when either
     * is Long.
     */
    public static Number safeIntegralAdd(Number a, Number b) {
      if (a instanceof Long || b instanceof Long) {
        return Math.addExact(a.longValue(), b.longValue());
      }
      return Math.addExact(a.intValue(), b.intValue());
    }

    /**
     * Overflow-safe integer/long subtraction. Throws ArithmeticException on overflow. Uses
     * int-level subtractExact when both operands are Integer (or narrower); uses long-level
     * subtractExact when either is Long.
     */
    public static Number safeIntegralSubtract(Number a, Number b) {
      if (a instanceof Long || b instanceof Long) {
        return Math.subtractExact(a.longValue(), b.longValue());
      }
      return Math.subtractExact(a.intValue(), b.intValue());
    }

    /**
     * Overflow-safe integer/long multiplication. Throws ArithmeticException on overflow. Uses
     * int-level multiplyExact when both operands are Integer (or narrower); uses long-level
     * multiplyExact when either is Long.
     */
    public static Number safeIntegralMultiply(Number a, Number b) {
      if (a instanceof Long || b instanceof Long) {
        return Math.multiplyExact(a.longValue(), b.longValue());
      }
      return Math.multiplyExact(a.intValue(), b.intValue());
    }

    // ---- Floating-point / decimal methods (no overflow check) ----

    /** Floating-point addition (IEEE 754 semantics, no overflow check). */
    public static Number floatingAdd(Number a, Number b) {
      if (MathUtils.isDecimal(a) || MathUtils.isDecimal(b)) {
        BigDecimal da =
            MathUtils.isDecimal(a) ? (BigDecimal) a : BigDecimal.valueOf(a.doubleValue());
        BigDecimal db =
            MathUtils.isDecimal(b) ? (BigDecimal) b : BigDecimal.valueOf(b.doubleValue());
        return da.add(db);
      }
      double result = a.doubleValue() + b.doubleValue();
      return MathUtils.coerceToWidestFloatingType(a, b, result);
    }

    /** Floating-point subtraction (IEEE 754 semantics, no overflow check). */
    public static Number floatingSubtract(Number a, Number b) {
      if (MathUtils.isDecimal(a) || MathUtils.isDecimal(b)) {
        BigDecimal da =
            MathUtils.isDecimal(a) ? (BigDecimal) a : BigDecimal.valueOf(a.doubleValue());
        BigDecimal db =
            MathUtils.isDecimal(b) ? (BigDecimal) b : BigDecimal.valueOf(b.doubleValue());
        return da.subtract(db);
      }
      double result = a.doubleValue() - b.doubleValue();
      return MathUtils.coerceToWidestFloatingType(a, b, result);
    }

    /** Floating-point multiplication (IEEE 754 semantics, no overflow check). */
    public static Number floatingMultiply(Number a, Number b) {
      if (MathUtils.isDecimal(a) || MathUtils.isDecimal(b)) {
        BigDecimal da =
            MathUtils.isDecimal(a) ? (BigDecimal) a : BigDecimal.valueOf(a.doubleValue());
        BigDecimal db =
            MathUtils.isDecimal(b) ? (BigDecimal) b : BigDecimal.valueOf(b.doubleValue());
        return da.multiply(db);
      }
      double result = a.doubleValue() * b.doubleValue();
      return MathUtils.coerceToWidestFloatingType(a, b, result);
    }
  }
}
