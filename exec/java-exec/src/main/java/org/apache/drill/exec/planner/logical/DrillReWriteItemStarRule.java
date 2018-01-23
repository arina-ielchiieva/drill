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
package org.apache.drill.exec.planner.logical;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.exec.planner.types.RelDataTypeDrillImpl;
import org.apache.drill.exec.planner.types.RelDataTypeHolder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DrillReWriteItemStarRule extends RelOptRule {

  public static final DrillReWriteItemStarRule INSTANCE_FPT =
      new DrillReWriteItemStarRule(RelOptHelper.some(Filter.class, RelOptHelper.some(Project.class, RelOptHelper.any(TableScan.class))),
        "DrillReWriteItemStarRule:FPT");

  public static final DrillReWriteItemStarRule INSTANCE_PPT =
    new DrillReWriteItemStarRule(RelOptHelper.some(Project.class, RelOptHelper.some(Project.class, RelOptHelper.any(TableScan.class))),
      "DrillReWriteItemStarRule:PPT");

  public DrillReWriteItemStarRule(RelOptRuleOperand operand, String id) {
    super(operand, id);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    System.out.println("ARINA");

/*    if (true) {
      return;
    }*/


    //todo we'll have two rules to cover this case
    Filter filterRel = call.rel(0);
    Project projectRel;
    TableScan scanRel;
    if (call.getRelList().size() == 2) {
      projectRel = null;
      scanRel = call.rel(1);
    } else {
      projectRel = call.rel(1);
      scanRel = call.rel(2);
    }

    final Map<RexCall, ItemFieldsHolder> starItemFields = new HashMap<>();

    RexNode condition = filterRel.getCondition();
    final RelDataType rowType = filterRel.getRowType();

    final RexVisitorImpl<RexNode> rexVisitor = new RexVisitorImpl<RexNode>(true) {
      @Override
      public RexNode visitCall(RexCall call) {
        // need to figure out field name and index
        ItemFieldsHolder itemFieldsHolder = ItemFieldsHolder.create(call);
        if (itemFieldsHolder != null) {
          String parentFieldName = rowType.getFieldNames().get(itemFieldsHolder.getRefIndex());
          if ("**".equals(parentFieldName)) { // dynamic star
            starItemFields.put(call, itemFieldsHolder);
          }
        }
        return super.visitCall(call);
      }
    };

    condition.accept(rexVisitor);

    // there is no item fields, no need to proceed further
    if (starItemFields.isEmpty()) {
      return;
    }

    // replace scan row type

    // create new row type
    RelDataType scanRowType = scanRel.getRowType();

    List<RelDataTypeField> fieldList = scanRowType.getFieldList();

    RelDataTypeHolder relDataTypeHolder = new RelDataTypeHolder();

    // add original fields
    for (RelDataTypeField field : fieldList) {
      relDataTypeHolder.getField(scanRel.getCluster().getTypeFactory(), field.getName());
    }

    // add new fields
    for (ItemFieldsHolder itemFieldsHolder : starItemFields.values()) {
      relDataTypeHolder.getField(scanRel.getCluster().getTypeFactory(), itemFieldsHolder.getFieldName());
    }

    RelDataTypeDrillImpl newRelDataType = new RelDataTypeDrillImpl(relDataTypeHolder, scanRel.getCluster().getTypeFactory());


    // create new scan

    RelOptTable table = scanRel.getTable();

    final Table tbl = table.unwrap(Table.class);
    Class elementType = EnumerableTableScan.deduceElementType(tbl);

    DrillTable unwrap;
    unwrap = table.unwrap(DrillTable.class);
    if (unwrap == null) {
      unwrap = table.unwrap(DrillTranslatableTable.class).getDrillTable();
    }

    final DrillTranslatableTable newTable = new DrillTranslatableTable(
        new DynamicDrillTable(unwrap.getPlugin(), unwrap.getStorageEngineName(),
            unwrap.getUserName(),
            unwrap.getSelection())); // need to wrap into translatable table due to assertion error

    final RelOptTableImpl newOptTableImpl = RelOptTableImpl.create(table.getRelOptSchema(), newRelDataType, newTable, ImmutableList.<String>of());

    //todo can we create this differently
    RelNode newScan =
        new EnumerableTableScan(scanRel.getCluster(), scanRel.getTraitSet(), newOptTableImpl, elementType);

    //todo this works when we have project, need to check what should be modified for the second rule
    assert projectRel != null;

    // create new project
    RelDataType newScanRowType = newScan.getRowType();
    List<RelDataTypeField> newScanFields = newScanRowType.getFieldList();

    // get expressions
    final List<RexNode> expressions = new ArrayList<>(projectRel.getProjects());

    // add references to item star fields
    Iterator<ItemFieldsHolder> iterator = starItemFields.values().iterator();
    for (int i = expressions.size(); i < newScanFields.size(); i++) {
      RexInputRef inputRef = new RexInputRef(i, newScanFields.get(i).getType());
      expressions.add(inputRef);
      ItemFieldsHolder itemFieldsHolder = iterator.next();
      itemFieldsHolder.setNewInputRex(inputRef); //todo we need cleaner solution
    }

    RelNode newProject = new LogicalProject(projectRel.getCluster(), projectRel.getTraitSet(), newScan, expressions, newScanRowType);

    // create new filter

    final RexShuttle filterTransformer = new RexShuttle() {

      @Override
      public RexNode visitCall(RexCall call) {
        ItemFieldsHolder itemFieldsHolder = starItemFields.get(call);
        if (itemFieldsHolder != null) {
          return itemFieldsHolder.getNewInputRex();
        }

        return super.visitCall(call);
      }
    };

    // transform filter condition
    RexNode newCondition = filterRel.getCondition().accept(filterTransformer);

    RelNode newFilter = new LogicalFilter(filterRel.getCluster(), filterRel.getTraitSet(), newProject, newCondition, ImmutableSet.<CorrelationId>of());
    //without project
    //RelNode newFilter = new LogicalFilter(filterRel.getCluster(), filterRel.getTraitSet(), newScan, newFilterCondition);

    // create new project to have the same row type as before
    // can not use old project
    Project wrapper = projectRel.copy(projectRel.getTraitSet(), newFilter, projectRel.getProjects(), projectRel.getRowType());

    call.transformTo(wrapper);
  }

  private static class ItemFieldsHolder {

    private final String fieldName;
    private final int refIndex;
    private RexInputRef newInputRex;


    ItemFieldsHolder(String fieldName, int refIndex) {
      this.fieldName = fieldName;
      this.refIndex = refIndex;
    }

    String getFieldName() {
      return fieldName;
    }

    int getRefIndex() {
      return refIndex;
    }

    RexInputRef getNewInputRex() {
      return newInputRex;
    }

    void setNewInputRex(RexInputRef newInputRex) {
      this.newInputRex = newInputRex;
    }

    /**
     * Creates {@link ItemFieldsHolder} instance by retrieving information from call to an ITEM operator.
     * If it is not ITEM operator or operands types do not match expected returns null.
     *
     * @param rexCall call to an operator
     * @return {@link ItemFieldsHolder} instance, null otherwise
     */
    static ItemFieldsHolder create(RexCall rexCall) {
      if (!SqlStdOperatorTable.ITEM.equals(rexCall.getOperator())) {
        return null;
      }

      if (rexCall.getOperands().size() != 2) {
        return null;
      }

      if (!(rexCall.getOperands().get(0) instanceof RexInputRef &&
          rexCall.getOperands().get(1) instanceof RexLiteral)) {
        return null;
      }

      RexInputRef rexInputRef = (RexInputRef) rexCall.getOperands().get(0);
      RexLiteral rexLiteral = (RexLiteral) rexCall.getOperands().get(1);
      if (SqlTypeName.CHAR.equals(rexLiteral.getType().getSqlTypeName())) {
        String fieldName = RexLiteral.stringValue(rexLiteral);
        return new ItemFieldsHolder(fieldName, rexInputRef.getIndex());
      }

      return null;
    }

  }

}
