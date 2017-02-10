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
package org.apache.drill.exec.planner.physical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.Validator;
import org.apache.drill.exec.planner.common.DrillValidatorRelBase;
import org.apache.drill.exec.planner.physical.visitor.PrelVisitor;
import org.apache.drill.exec.record.BatchSchema;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class ValidatorPrel extends DrillValidatorRelBase implements Prel {

  public ValidatorPrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, List<String> columns) {
    super(cluster, traitSet, left, right, columns);
  }

  @Override
  public Prel copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ValidatorPrel(getCluster(), traitSet, inputs.get(0), inputs.get(1), getColumns()); //todo inputs or getLeft
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    PhysicalOperator leftPop = ((Prel)getLeft()).getPhysicalOperator(creator);
    PhysicalOperator rightPop = ((Prel)getRight()).getPhysicalOperator(creator);
    return new Validator(leftPop, rightPop, getColumns());
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitPrel(this, value);
  }

  @Override
  public BatchSchema.SelectionVectorMode[] getSupportedEncodings() {
    return BatchSchema.SelectionVectorMode.DEFAULT;
  }

  @Override
  public BatchSchema.SelectionVectorMode getEncoding() {
    return BatchSchema.SelectionVectorMode.NONE;
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return false;
  }

  @Override
  public Iterator<Prel> iterator() {
    return PrelUtil.iter(getLeft(), getRight());
  }
}

