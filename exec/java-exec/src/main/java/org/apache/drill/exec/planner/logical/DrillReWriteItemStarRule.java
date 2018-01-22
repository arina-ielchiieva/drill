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

import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.exec.planner.types.RelDataTypeDrillImpl;
import org.apache.drill.exec.planner.types.RelDataTypeHolder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
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

    final Map<String, Integer> itemColumns = new LinkedHashMap<>();

    RexNode condition = filterRel.getCondition();

    final RexVisitorImpl<RexNode> rexVisitor = new RexVisitorImpl<RexNode>(true) {
      @Override
      public RexNode visitCall(RexCall call) {
        if (SqlStdOperatorTable.ITEM.equals(call.getOperator())) {
          System.out.println("It's an item");
          // need to figure out column name and index
          RexInputRef rexInputRef = (RexInputRef) call.getOperands().get(0);
          System.out.println("Field index -> " + rexInputRef.getIndex());
          RexLiteral rexLiteral = (RexLiteral) call.getOperands().get(1);
          if (SqlTypeName.CHAR.equals(rexLiteral.getType().getSqlTypeName())) {
            String fieldName = RexLiteral.stringValue(rexLiteral);
            itemColumns.put(fieldName, rexInputRef.getIndex());
          }
        }
        return super.visitCall(call);
      }
    };

    condition.accept(rexVisitor);

    // there is no item columns, no need to proceed further
    if (itemColumns.isEmpty()) {
      return;
    }

    // we can only process item columns that contain star
    List<String> starItemColumns = new ArrayList<>();

    // todo move star item check to rexVisitor
    RelDataType rowType = filterRel.getRowType();
    for (Map.Entry<String, Integer> entry : itemColumns.entrySet()) {
      int index = entry.getValue();
      String fieldName = rowType.getFieldNames().get(index);
      if (fieldName.startsWith("*")) {
        starItemColumns.add(entry.getKey());
      }
    }

    // there are no item star columns, no need to proceed
    if (starItemColumns.isEmpty()) {
      return;
    }

    // replace scan row type

    // create new row type
    RelDataType scanRowType = scanRel.getRowType();

    List<RelDataTypeField> fieldList = scanRowType.getFieldList();

    RelDataTypeHolder holder = new RelDataTypeHolder();

    //List<RelDataTypeField> newFields = new ArrayList<>();
    for (RelDataTypeField field : fieldList) {
      holder.getField(scanRel.getCluster().getTypeFactory(), field.getName());
    }

    // add new columns
    for (String fieldName : starItemColumns) {
      holder.getField(scanRel.getCluster().getTypeFactory(), fieldName);
    }

    RelDataTypeDrillImpl newRelDataType = new RelDataTypeDrillImpl(holder, scanRel.getCluster().getTypeFactory());


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

    final RelOptTableImpl newOptTableImpl = RelOptTableImpl.create(table.getRelOptSchema(), newRelDataType, newTable);

    RelNode newScan =
        new EnumerableTableScan(scanRel.getCluster(), scanRel.getTraitSet(), newOptTableImpl, elementType);

    //todo this works when we have project, need to check what should be modified for the second rule
    assert projectRel != null;

    // create new project
    RelDataType newScanRowType = newScan.getRowType();
    List<RelDataTypeField> newScanFields = newScanRowType.getFieldList();
    //RelRecordType projectRecordType = new RelRecordType(newScanFields);

    // get expressions
    final List<RexNode> expressions = new ArrayList<>(projectRel.getProjects());

    final Map<String, RexNode> mapToItemExpressions = new HashMap<>();

    // add references to item star fields
    for (int i = expressions.size(); i < newScanFields.size(); i++) {
      RexInputRef inputRef = new RexInputRef(i, newScanFields.get(i).getType());
      expressions.add(inputRef);
      mapToItemExpressions.put(newScanFields.get(i).getName(), inputRef);
    }

    RelNode newProject = new LogicalProject(projectRel.getCluster(), projectRel.getTraitSet(), newScan, expressions, newScanRowType);

    // create new filter

    final RexShuttle filterTransformer = new RexShuttle() {

      @Override
      public RexNode visitCall(RexCall call) {
        if (SqlStdOperatorTable.ITEM.equals(call.getOperator())) {
          System.out.println("It's an item");
          // need to figure out column name and index
          RexInputRef rexInputRef = (RexInputRef) call.getOperands().get(0);
          System.out.println("Field index -> " + rexInputRef.getIndex());
          RexLiteral rexLiteral = (RexLiteral) call.getOperands().get(1);
          if (SqlTypeName.CHAR.equals(rexLiteral.getType().getSqlTypeName())) {
            String fieldName = RexLiteral.stringValue(rexLiteral);
            RexNode rexNode = mapToItemExpressions.get(fieldName);
            if (rexNode != null) {
              //todo
              return rexNode;
            }
          }
        }
        return super.visitCall(call);
      }
    };

    RexNode newCondition = filterRel.getCondition().accept(filterTransformer);

    RelNode newFilter = new LogicalFilter(filterRel.getCluster(), filterRel.getTraitSet(), newProject, newCondition);
    //without project
    //RelNode newFilter = new LogicalFilter(filterRel.getCluster(), filterRel.getTraitSet(), newScan, newFilterCondition);

    // create new project to have the same row type as before
    // can use old project
    Project wrapper = projectRel.copy(projectRel.getTraitSet(), newFilter, projectRel.getProjects(), projectRel.getRowType());

    call.transformTo(wrapper);

    //RelDataType rowType = filterRel.getRowType();
/*    int index = inputRefs.get(0);
    String s = rowType.getFieldNames().get(index);
    if ("*".equals(s)) {
      System.out.println("It's star item");*/




      //newFields.addAll(fieldList);

      // create dir0 field
      //RelDataTypeField dir0 = new RelDataTypeFieldImpl("dir0", fieldList.size(),
      //scanRel.getCluster().getTypeFactory().createSqlType(SqlTypeName.ANY));

      //holder.getField(scanRel.getCluster().getTypeFactory(), "dir0");
      //holder.getField(scanRel.getCluster().getTypeFactory(), "o_orderdate");

      //newFields.add(dir0);

      //RelDataTypeDrillImpl newRelDataType = new RelDataTypeDrillImpl(holder, scanRel.getCluster().getTypeFactory());
      //RelDataType newRelDataType = new RelRecordType(newFields);



      //call.transformTo(newScan);



    //}

    //todo since we know that we have star item, we need to replace it with field reference...
    // rel#23:LogicalFilter.NONE.ANY([]).[](input=HepRelVertex#22,condition==(ITEM($0, 'dir0'), '1992'))
    // rel#21:LogicalProject.NONE.ANY([]).[](input=HepRelVertex#20,*=$0)
    // rel#4:EnumerableTableScan.ENUMERABLE.ANY([]).[](table=[dfs.root, *])


  }
}


/*
  private RelDataType constructDataType(DrillAggregateRel aggregateRel, Collection<String> fieldNames) {
    List<RelDataTypeField> fields = new ArrayList<>();
    Iterator<String> filedNamesIterator = fieldNames.iterator();
    int fieldIndex = 0;
    while (filedNamesIterator.hasNext()) {
      RelDataTypeField field = new RelDataTypeFieldImpl(
          filedNamesIterator.next(),
          fieldIndex++,
          aggregateRel.getCluster().getTypeFactory().createSqlType(SqlTypeName.BIGINT));
      fields.add(field);
    }
    return new RelRecordType(fields);
  }
 */