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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillUnionRel;
import org.apache.drill.exec.planner.logical.DrillValidatorRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;

import java.util.List;

public class ValidatorPrule extends Prule {
  public static final RelOptRule INSTANCE = new ValidatorPrule();

  private ValidatorPrule() {
    super(RelOptHelper.some(DrillValidatorRel.class, DrillRel.DRILL_LOGICAL, RelOptHelper.any(RelNode.class)),"Prel.ValidatorPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final DrillValidatorRel rel = call.rel(0);
    RelTraitSet traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL);

    RelNode convertedLeft = convert(rel.getLeft(), PrelUtil.fixTraits(call, traits));
    RelNode convertedRight = convert(rel.getRight(), PrelUtil.fixTraits(call, traits));

    //traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(DrillDistributionTrait.SINGLETON);
    traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(DrillDistributionTrait.ANY);

    ValidatorPrel validatorPrel = new ValidatorPrel(rel.getCluster(), traits,
        convertedLeft, convertedRight, rel.getColumns());
    call.transformTo(validatorPrel);
  }

/*  private class ValidatorTraitPull extends SubsetTransformer<DrillValidatorRel, RuntimeException> {

    public ValidatorTraitPull(RelOptRuleCall call) {
      super(call);
    }

    @Override
    public RelNode convertChild(DrillValidatorRel current, RelNode child) throws RuntimeException {
      DrillDistributionTrait childDist = current.getTraitSet().getTrait(DrillDistributionTraitDef.INSTANCE);
      return new ValidatorPrel(current.getCluster(),
          current.getTraitSet().plus(childDist).plus(Prel.DRILL_PHYSICAL),
          child, );
    }
  }*/

}

/*
  private class WriteTraitPull extends SubsetTransformer<DrillWriterRelBase, RuntimeException> {

    public WriteTraitPull(RelOptRuleCall call) {
      super(call);
    }

    @Override
    public RelNode convertChild(DrillWriterRelBase writer, RelNode rel) throws RuntimeException {
      DrillDistributionTrait childDist = rel.getTraitSet().getTrait(DrillDistributionTraitDef.INSTANCE);

      // Create the Writer with the child's distribution because the degree of parallelism for the writer
      // should correspond to the number of child minor fragments. The Writer itself is not concerned with
      // the collation of the child.  Note that the Writer's output RowType consists of
      // {fragment_id varchar(255), number_of_records_written bigint} which are very different from the
      // child's output RowType.
      return new WriterPrel(writer.getCluster(),
          writer.getTraitSet().plus(childDist).plus(Prel.DRILL_PHYSICAL),
          rel, writer.getCreateTableEntry());
    }

  }

 */