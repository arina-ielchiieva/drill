package org.apache.drill.common.expression;

import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.common.types.TypeProtos.MajorType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SchemaExpression extends LogicalExpressionBase {

  private List<String> columns = new ArrayList<>();
  private LogicalExpression column;
  private MajorType type;
  private boolean nullability;

  public SchemaExpression(LogicalExpression column, MajorType type, boolean nullability) {
    super(null);
    this.column = column;
    this.type = type;
    this.nullability = nullability;
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return null;
  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    return null;
  }


  @Override
  public String toString() {
    return "SchemaExpression{" + "columns=" + columns + ", column=" + column + ", type=" + type + ", nullability='" + nullability + '\'' + '}';
  }
}
