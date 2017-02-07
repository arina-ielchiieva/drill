/*
 * ****************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ****************************************************************************
 */
package org.apache.drill.exec.planner.logical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.Validator;
import org.apache.drill.exec.planner.common.DrillValidatorRelBase;

import java.util.List;

public class DrillValidatorRel extends DrillValidatorRelBase implements DrillRel {

  public DrillValidatorRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, List<String> columns) {
    super(cluster, traitSet, left, right, columns);
  }

  @Override
  public double getRows() {
    return getRight().getRows(); //todo
  }

  @Override
  public RelDataType deriveRowType() {
    return getLeft().getRowType(); //todo
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DrillValidatorRel(getCluster(), traitSet, getLeft(), getRight(), getColumns());
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    Validator.Builder builder = Validator.builder();
    return builder
        .left(implementor.visitChild(this, 0, getLeft()))
        .right(implementor.visitChild(this, 1, getRight()))
        .columns(getColumns())
        .build();
  }
}
