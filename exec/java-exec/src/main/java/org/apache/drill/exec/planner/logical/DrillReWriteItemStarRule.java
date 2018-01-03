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
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.exec.planner.types.RelDataTypeDrillImpl;
import org.apache.drill.exec.planner.types.RelDataTypeHolder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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


    final List<Integer> inputRefs = new ArrayList<>();

    RexNode condition = filterRel.getCondition();
    condition.accept(new RexVisitorImpl<Object>(true) {
      @Override
      public Object visitCall(RexCall call) {
        RexNode rexNode = call.getOperands().get(0);
        if (rexNode instanceof RexCall) {
          RexCall call2 = (RexCall) rexNode;
          if (SqlStdOperatorTable.ITEM.equals(call2.getOperator())) {
            System.out.println("It's an item");
            RexInputRef rexInputRef = (RexInputRef) call2.getOperands().get(0);
            System.out.println("Field index -> " + rexInputRef.getIndex());
            inputRefs.add(rexInputRef.getIndex());
          }
        }
        return null;
      }
    });

    if (inputRefs.isEmpty()) {
      return;
    }

    RelDataType rowType = filterRel.getRowType();
    int index = inputRefs.get(0);
    String s = rowType.getFieldNames().get(index);
    if ("*".equals(s)) {
      System.out.println("It's star item");

      // replace scan row type

      // create new row type
      RelDataType scanRowType = scanRel.getRowType();

      List<RelDataTypeField> fieldList = scanRowType.getFieldList();

      RelDataTypeHolder holder = new RelDataTypeHolder();

      //List<RelDataTypeField> newFields = new ArrayList<>();
      for (RelDataTypeField field : fieldList) {
        holder.getField(scanRel.getCluster().getTypeFactory(), field.getName());
      }


      //newFields.addAll(fieldList);

      // create dir0 field
      //RelDataTypeField dir0 = new RelDataTypeFieldImpl("dir0", fieldList.size(),
      //scanRel.getCluster().getTypeFactory().createSqlType(SqlTypeName.ANY));

      //holder.getField(scanRel.getCluster().getTypeFactory(), "dir0");
      holder.getField(scanRel.getCluster().getTypeFactory(), "o_orderdate");

      //newFields.add(dir0);

      RelDataTypeDrillImpl newRelDataType = new RelDataTypeDrillImpl(holder, scanRel.getCluster().getTypeFactory());
      //RelDataType newRelDataType = new RelRecordType(newFields);

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

      //call.transformTo(newScan);

      //todo
      assert projectRel != null;

      // create new project
      RelDataType newScanRowType = newScan.getRowType();
      List<RelDataTypeField> newScanFields = newScanRowType.getFieldList();
      RelRecordType projectRecordType = new RelRecordType(newScanFields);

      // get expressions
      List<RexNode> expressions = new ArrayList<>();
      expressions.addAll(projectRel.getProjects());

      // create dir0 input ref
      RexInputRef dir0InputRef = new RexInputRef(expressions.size(), newScanFields.get(1).getType());
      expressions.add(dir0InputRef);

      RelNode newProject = new LogicalProject(projectRel.getCluster(), projectRel.getTraitSet(), newScan, expressions, newScanRowType);
      //call.transformTo(newProject);

      // create new filter

      //create new condition
      RexCall oldFilterCondition = (RexCall) filterRel.getCondition();
      List<RexNode> operands = oldFilterCondition.getOperands();
      List<RexNode> newOperands = new ArrayList<>();
      newOperands.add(dir0InputRef);
      newOperands.add(operands.get(1));
      RexCall newFilterCondition = oldFilterCondition.clone(oldFilterCondition.getType(), newOperands);

      RelNode newFilter = new LogicalFilter(filterRel.getCluster(), filterRel.getTraitSet(), newProject, newFilterCondition);
      //without project
      //RelNode newFilter = new LogicalFilter(filterRel.getCluster(), filterRel.getTraitSet(), newScan, newFilterCondition);

      // create new project to have the same row type as before
      // can use old project
      Project wrapper = projectRel.copy(projectRel.getTraitSet(), newFilter, projectRel.getProjects(), projectRel.getRowType());

      call.transformTo(wrapper);

    }

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