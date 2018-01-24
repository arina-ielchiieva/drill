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
import org.apache.calcite.rel.type.RelDataTypeFactory;
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
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.planner.types.RelDataTypeDrillImpl;
import org.apache.drill.exec.planner.types.RelDataTypeHolder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DrillReWriteItemStarRule extends RelOptRule {

  public static final DrillReWriteItemStarRule INSTANCE = new DrillReWriteItemStarRule(RelOptHelper.some(Filter.class, RelOptHelper.some(Project.class, RelOptHelper.any
      (TableScan.class))), "DrillReWriteItemStarRule");

  public DrillReWriteItemStarRule(RelOptRuleOperand operand, String id) {
    super(operand, id);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    System.out.println("ARINA: DrillReWriteItemStarRule"); //todo entered two times...

    Filter filterRel = call.rel(0);
    Project projectRel = call.rel(1);
    TableScan scanRel = call.rel(2);
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
    RelDataTypeFactory typeFactory = scanRel.getCluster().getTypeFactory();

    // add original fields
    for (RelDataTypeField field : fieldList) {
      relDataTypeHolder.getField(typeFactory, field.getName());
    }

    List<SchemaPath> newColumns = new ArrayList<>();

    // add new fields
    for (ItemFieldsHolder itemFieldsHolder : starItemFields.values()) {
      itemFieldsHolder.initNewField(relDataTypeHolder, typeFactory);
      newColumns.add(new SchemaPath(new PathSegment.NameSegment(itemFieldsHolder.getFieldName())));
    }

    RelDataTypeDrillImpl newRelDataType = new RelDataTypeDrillImpl(relDataTypeHolder, typeFactory);


    // create new scan

    RelOptTable table = scanRel.getTable();

    final Table tbl = table.unwrap(Table.class);
    Class elementType = EnumerableTableScan.deduceElementType(tbl);

    DrillTable unwrap;
    unwrap = table.unwrap(DrillTable.class);
    if (unwrap == null) {
      unwrap = table.unwrap(DrillTranslatableTable.class).getDrillTable();
    }

    final DrillTranslatableTable newTable = new DrillTranslatableTable(new DynamicDrillTable(unwrap.getPlugin(), unwrap.getStorageEngineName(), unwrap.getUserName(), unwrap.getSelection())); // need to wrap into translatable table due to assertion error

    final RelOptTableImpl newOptTableImpl = RelOptTableImpl.create(table.getRelOptSchema(), newRelDataType, newTable, ImmutableList.<String>of());

    RelNode newScan = new EnumerableTableScan(scanRel.getCluster(), scanRel.getTraitSet(), newOptTableImpl, elementType);
    //todo we use this method instead of DrillScanRel to include previous columns... consider if can be re-written

    // new SchemaPath(new NameSegment(name))

/*    final DrillScanRel newScan =
        new DrillScanRel(scanRel.getCluster(),
            scanRel.getTraitSet().plus(DrillRel.DRILL_LOGICAL),
            scanRel.getTable(),
            newRelDataType,
            newColumns); //////todo only new columns....., we need old as well*/

    // create new project
    RelDataType newScanRowType = newScan.getRowType();

    // get expressions
    final List<RexNode> expressions = new ArrayList<>(projectRel.getProjects());

    // add references to item star fields
    int newRexIndex = scanRowType.getFieldCount();
    for (ItemFieldsHolder itemFieldsHolder : starItemFields.values()) {
      itemFieldsHolder.initNewInputRef(newRexIndex++);
      expressions.add(itemFieldsHolder.getNewInputRef());
    }

    RelNode newProject = new LogicalProject(projectRel.getCluster(), projectRel.getTraitSet(), newScan, expressions, newScanRowType);

    // create new filter

    final RexShuttle filterTransformer = new RexShuttle() {

      @Override
      public RexNode visitCall(RexCall call) {
        ItemFieldsHolder itemFieldsHolder = starItemFields.get(call);
        if (itemFieldsHolder != null) {
          return itemFieldsHolder.getNewInputRef();
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

    private RexInputRef newInputRef;

    private RelDataTypeField newField;


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

    RexInputRef getNewInputRef() {
      return newInputRef;
    }

    void initNewInputRef(int index) {
      assert newField != null;
      newInputRef = new RexInputRef(index, newField.getType());
    }

    void initNewField(RelDataTypeHolder relDataTypeHolder, RelDataTypeFactory factory) {
      newField = relDataTypeHolder.getField(factory, fieldName);
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

      if (!(rexCall.getOperands().get(0) instanceof RexInputRef && rexCall.getOperands().get(1) instanceof RexLiteral)) {
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
