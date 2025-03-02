/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.rewriter.nestedfield;

import static org.opensearch.sql.legacy.utils.Util.NESTED_JOIN_TYPE;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 *
 *
 * <pre>
 * Visitor to rewrite AST (abstract syntax tree) for nested type fields to support implicit nested() function call.
 * Intuitively, the approach is to implement SQLIdentifier.visit() and wrap nested() function for nested field.
 * The parsing result of FROM clause will be used to determine if an identifier is nested field.
 * <p>
 * State transition table (state here means Scope + Query AST):
 * _________________________________________________________________________________________________________________
 * |    Rewrite   |            Scope              |                        Sample Query                            |
 * |---------------------------------------------------------------------------------------------------------------|
 * |   (Start)    |             ()                | SELECT e.lastname, COUNT(*) FROM team t, employees e           |
 * |              |                               |   WHERE region = 'US' and e.firstname = 'John'                 |
 * |              |                               |     GROUP BY e.lastname                                        |
 * |---------------------------------------------------------------------------------------------------------------|
 * |     FROM     | (parentAlias='t'              | SELECT e.lastname, COUNT(*) FROM team                          |
 * |              |  aliasFullPaths={e: employees}|   WHERE region = 'US' and e.firstname = 'John'                 |
 * |              |  conditionTags={})            |     GROUP BY e.lastname                                        |
 * |---------------------------------------------------------------------------------------------------------------|
 * |   Identifier | (parentAlias='t'              | SELECT nested(employees.lastname), COUNT(*) FROM team          |
 * |              |  aliasFullPaths={e: employees}|   WHERE region = 'US' and employees.firstname = 'John'         |
 * |              |  conditionTags={c: employees})|     GROUP BY nested(employees.lastname)                        |
 * |---------------------------------------------------------------------------------------------------------------|
 * |    WHERE     | (parentAlias='t'              | SELECT nested(employees.lastname), COUNT(*) FROM team          |
 * |              |  aliasFullPaths={e: employees}|   WHERE region = 'US' and nested(employees.firstname) = 'John' |
 * |              |  conditionTags={c: employees})|     GROUP BY nested(employees.lastname)                        |
 * |---------------------------------------------------------------------------------------------------------------|
 * <p>
 * Note 'c' in conditionTag refer to the reference to SQLBinaryOpExpr object of condition 'employees.firstname = 'John'
 * <p>
 * More details:
 * 1) Manage environment in the case of subquery
 * 2) Add nested field to select for SELECT *
 * 3) Merge conditions of same nested field to single nested() call
 * </pre>
 */
public class NestedFieldRewriter extends MySqlASTVisitorAdapter {

  /**
   * Scope stack to record the state (nested field names etc) for current query. In the case of
   * subquery, the active scope of current query is the top element of the stack.
   */
  private final Deque<Scope> environment = new ArrayDeque<>();

  /**
   * Rewrite FROM here to make sure FROM statement always be visited before other statement in
   * query. Note that return true anyway to continue visiting FROM in subquery if any.
   */
  @Override
  public boolean visit(MySqlSelectQueryBlock query) {
    environment.push(new Scope());
    if (query.getFrom() == null) {
      return false;
    }

    query.getFrom().setParent(query);
    new From(query.getFrom()).rewrite(curScope());

    if (curScope().isAnyNestedField() && isNotGroupBy(query)) {
      new Select(query.getSelectList()).rewrite(curScope());
    }

    query.putAttribute(NESTED_JOIN_TYPE, curScope().getActualJoinType());
    return true;
  }

  @Override
  public boolean visit(SQLIdentifierExpr expr) {
    if (curScope().isAnyNestedField()) {
      new Identifier(expr).rewrite(curScope());
    }
    return true;
  }

  @Override
  public void endVisit(SQLBinaryOpExpr expr) {
    if (curScope().isAnyNestedField()) {
      new Where(expr).rewrite(curScope());
    }
  }

  @Override
  public void endVisit(MySqlSelectQueryBlock query) {
    environment.pop();
  }

  /** Current scope which is top of the stack */
  private Scope curScope() {
    return environment.peek();
  }

  private boolean isNotGroupBy(MySqlSelectQueryBlock query) {
    return query.getGroupBy() == null;
  }
}
