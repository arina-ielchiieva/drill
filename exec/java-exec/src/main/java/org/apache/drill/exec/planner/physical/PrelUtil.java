/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.physical;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.PathSegment.ArraySegment;
import org.apache.drill.common.expression.PathSegment.NameSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class PrelUtil {

  public static List<Ordering> getOrdering(RelCollation collation, RelDataType rowType) {
    List<Ordering> orderExpr = Lists.newArrayList();

    final List<String> childFields = rowType.getFieldNames();

    for (RelFieldCollation fc: collation.getFieldCollations() ) {
      FieldReference fr = new FieldReference(childFields.get(fc.getFieldIndex()), ExpressionPosition.UNKNOWN, false);
      orderExpr.add(new Ordering(fc.getDirection(), fr, fc.nullDirection));
    }

    return orderExpr;
  }


  public static Iterator<Prel> iter(RelNode... nodes) {
    return (Iterator<Prel>) (Object) Arrays.asList(nodes).iterator();
  }

  public static Iterator<Prel> iter(List<RelNode> nodes) {
    return (Iterator<Prel>) (Object) nodes.iterator();
  }

  public static PlannerSettings getSettings(final RelOptCluster cluster) {
    return getPlannerSettings(cluster);
  }

  public static PlannerSettings getPlannerSettings(final RelOptCluster cluster) {
    return cluster.getPlanner().getContext().unwrap(PlannerSettings.class);
  }

  public static PlannerSettings getPlannerSettings(RelOptPlanner planner) {
    return planner.getContext().unwrap(PlannerSettings.class);
  }

  public static Prel removeSvIfRequired(Prel prel, SelectionVectorMode... allowed) {
    SelectionVectorMode current = prel.getEncoding();
    for (SelectionVectorMode m : allowed) {
      if (current == m) {
        return prel;
      }
    }
    return new SelectionVectorRemoverPrel(prel);
  }

  public static int getLastUsedColumnReference(List<RexNode> projects) {
    LastUsedRefVisitor lastUsed = new LastUsedRefVisitor();
    for (RexNode rex : projects) {
      rex.accept(lastUsed);
    }
    return lastUsed.getLastUsedReference();
  }

  public static ProjectPushInfo getColumns(RelDataType rowType, List<RexNode> projects) {
    final List<String> fieldNames = rowType.getFieldNames();
    if (fieldNames.isEmpty()) {
      return null;
    }

    RefFieldsVisitor v = new RefFieldsVisitor(rowType);
    for (RexNode exp : projects) {
      PathSegment segment = exp.accept(v);
      v.addColumn(segment);
    }

    return v.getInfo();

  }

  private static class DesiredField {
    private final int originalIndex;
    private final String name;
    private final RelDataType type;
    private final RexNode originalNode;

    DesiredField(int origIndex, String name, RelDataType type, RexNode originalNode) {
      this.originalIndex = origIndex;
      this.name = name;
      this.type = type;
      this.originalNode = originalNode;
    }

    DesiredField(String name, RelDataType type, RexNode originalNode) {
      this(-1, name, type, originalNode);
    }

    boolean hasOriginalIndex() {
      return originalIndex != -1;
    }

    int getOriginalIndex() {
      return originalIndex;
    }

    String getName() {
      return name;
    }

    RelDataType getType() {
      return type;
    }

    RexNode getOriginalNode() {
      return originalNode;
    }

    @Override
    public int hashCode() {
      return Objects.hash(originalIndex, name, type, originalNode);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DesiredField field = (DesiredField) o;
      return originalIndex == field.originalIndex &&
          Objects.equals(name, field.name) &&
          Objects.equals(type, field.type) &&
          Objects.equals(originalNode, field.originalNode);
    }
  }

  private static class FieldMapper {

    private final Map<RexNode, Integer> mapByNode = new HashMap<>();
    private final Map<Integer, Integer> mapByIndex = new HashMap<>();

    FieldMapper(List<DesiredField> fields) {
      int index = 0;
      for (DesiredField field : fields) {
/*        if (field.hasOriginalIndex()) {
          mapByIndex.put(field.getOriginalIndex(), index);
        } else {
          mapByName.put(field.getName(), index);
        }*/
        mapByNode.put(field.getOriginalNode(), index);
        index++;
      }
    }

    Integer getNewId(RexNode node) {
      return mapByNode.get(node);
    }

/*    Integer getNewId(int originalIndex) {
      return mapByIndex.get(originalIndex);
    }*/
  }

  public static class ProjectPushInfo {
    private final List<SchemaPath> columns;
    private final InputReWriter reWriter;
    private final List<String> fieldNames;
    private final List<RelDataType> types;

    public ProjectPushInfo(List<SchemaPath> columns, ImmutableList<DesiredField> desiredFields) {
      this.columns = columns;
      this.fieldNames = Lists.newArrayListWithCapacity(desiredFields.size());
      this.types = Lists.newArrayListWithCapacity(desiredFields.size());

      Map<RexNode, Integer> mapper = new HashMap<>();

      int index = 0;
      for (DesiredField f : desiredFields) {
        fieldNames.add(f.getName());
        types.add(f.getType());
        mapper.put(f.getOriginalNode(), index++);
      }

      this.reWriter = new InputReWriter(mapper);
    }

    public List<SchemaPath> getColumns() {
      return columns;
    }

    public InputReWriter getInputReWriter() {
      return reWriter;
    }

    public boolean isStarQuery() {
      for (SchemaPath column : columns) {
        if (column.getRootSegment().getPath().startsWith("*")) {
          return true;
        }
      }
      return false;
    }

    public RelDataType createNewRowType(RelDataTypeFactory factory) {
      return factory.createStructType(types, fieldNames);
    }
  }

  // Simple visitor class to determine the last used reference in the expression
  private static class LastUsedRefVisitor extends RexVisitorImpl<Void> {

    int lastUsedRef = -1;

    protected LastUsedRefVisitor() {
      super(true);
    }

    @Override
    public Void visitInputRef(RexInputRef inputRef) {
      lastUsedRef = Math.max(lastUsedRef, inputRef.getIndex());
      return null;
    }

    @Override
    public Void visitCall(RexCall call) {
      for (RexNode operand : call.operands) {
        operand.accept(this);
      }
      return null;
    }

    public int getLastUsedReference() {
      return lastUsedRef;
    }
  }

  /** Visitor that finds the set of inputs that are used. */
  private static class RefFieldsVisitor extends RexVisitorImpl<PathSegment> {
    private final Set<SchemaPath> columns = Sets.newLinkedHashSet();
    private final List<String> fieldNames;
    private final List<RelDataTypeField> fields;
    private final Set<DesiredField> desiredFields = Sets.newLinkedHashSet();

    RefFieldsVisitor(RelDataType rowType) {
      super(true);
      this.fieldNames = rowType.getFieldNames();
      this.fields = rowType.getFieldList();
    }

    void addColumn(PathSegment segment) {
      if (segment != null && segment instanceof NameSegment) {
        columns.add(new SchemaPath((NameSegment)segment));
      }
    }

    ProjectPushInfo getInfo() {
      return new ProjectPushInfo(ImmutableList.copyOf(columns), ImmutableList.copyOf(desiredFields));
    }

    @Override
    public PathSegment visitInputRef(RexInputRef inputRef) {
      int index = inputRef.getIndex();
      String name = fieldNames.get(index);
      RelDataTypeField field = fields.get(index);
      DesiredField f = new DesiredField(index, name, field.getType(), inputRef);
      desiredFields.add(f);
      return new NameSegment(name);
    }

    @Override
    public PathSegment visitCall(RexCall call) {
      if (SqlStdOperatorTable.ITEM.equals(call.getOperator()) && call.getOperands().size() == 2) {

        if (hasStar(call.operands.get(0))) {
          if (call.operands.get(1) instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) call.operands.get(1);
            if (SqlTypeName.CHAR.equals(literal.getType().getSqlTypeName())) {
              String name = RexLiteral.stringValue(literal);
              addColumn(new NameSegment(name));
              desiredFields.add(new DesiredField(name, call.getType(), call));
            }
          }
          return null;
        }

        PathSegment mapOrArray = call.operands.get(0).accept(this);
        if (mapOrArray != null) {
          if (call.operands.get(1) instanceof RexLiteral) {
            return mapOrArray.cloneWithNewChild(convertLiteral((RexLiteral) call.operands.get(1)));
          }
          return mapOrArray;
        }
      } else {
        for (RexNode operand : call.operands) {
          addColumn(operand.accept(this));
        }
      }
      return null;
    }

    private boolean hasStar(RexNode ref) {
      if (ref instanceof RexInputRef) {
        RexInputRef inputRef = (RexInputRef) ref;
        int index = inputRef.getIndex();
        String name = fieldNames.get(index);
        return "**".equals(name); // dynamic star //todo think about startsWith method usage
      }
      return false;
    }

    private PathSegment convertLiteral(RexLiteral literal) {
      switch (literal.getType().getSqlTypeName()) {
      case CHAR:
        return new NameSegment(RexLiteral.stringValue(literal));
      case INTEGER:
        return new ArraySegment(RexLiteral.intValue(literal));
      default:
        return null;
      }
    }

  }

  public static RelTraitSet fixTraits(RelOptRuleCall call, RelTraitSet set) {
    return fixTraits(call.getPlanner(), set);
  }

  public static RelTraitSet fixTraits(RelOptPlanner cluster, RelTraitSet set) {
    if (getPlannerSettings(cluster).isSingleMode()) {
      return set.replace(DrillDistributionTrait.ANY);
    } else {
      return set;
    }
  }

  private static class InputReWriter extends RexShuttle {

    private final Map<RexNode, Integer> mapper;

    InputReWriter(Map<RexNode, Integer> mapper) {
      this.mapper = mapper;
    }

    @Override
    public RexNode visitCall(final RexCall call) {
/*      if (SqlStdOperatorTable.ITEM.equals(call.getOperator()) && call.operands.get(1) instanceof RexLiteral) {
        RexLiteral literal = (RexLiteral) call.operands.get(1);
        if (SqlTypeName.CHAR.equals(literal.getType().getSqlTypeName())) {
          String fieldName = RexLiteral.stringValue(literal);
          if (mapper.getNewId(fieldName) != null) {
            return new RexInputRef(mapper.getNewId(fieldName), call.operands.get(0).getType());
          }
        }
      }*/

      Integer index = mapper.get(call);

      if (index != null) {
        return new RexInputRef(index, call.getType());
      }
      return super.visitCall(call);
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
      Integer index = mapper.get(inputRef);
      if (index != null) {
        return new RexInputRef(index, inputRef.getType());
      }
      return super.visitInputRef(inputRef);
    }

/*    @Override
    public RexNode visitLocalRef(RexLocalRef localRef) {
      return new RexInputRef(mapper.getNewId(localRef.getIndex()), localRef.getType());
    }*/

  }

}
