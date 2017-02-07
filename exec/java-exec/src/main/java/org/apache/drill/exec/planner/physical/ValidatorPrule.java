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
import org.apache.drill.exec.planner.logical.DrillUnionRel;
import org.apache.drill.exec.planner.logical.DrillValidatorRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;

import java.util.List;

public class ValidatorPrule extends Prule {
  public static final RelOptRule INSTANCE = new ValidatorPrule();

  private ValidatorPrule() {
    super(RelOptHelper.any(DrillUnionRel.class), "Prel.ValidatorPrule");
  }


  @Override
  public void onMatch(RelOptRuleCall call) {
    //todo implement method

    final DrillValidatorRel rel = (DrillValidatorRel) call.rel(0);
    //final List<RelNode> inputs = rel.getInputs();
   // List<RelNode> convertedInputList = Lists.newArrayList();
    RelTraitSet traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL);

    RelNode convertedLeft = convert(rel.getLeft(), PrelUtil.fixTraits(call, traits));
    RelNode convertedRight = convert(rel.getRight(), PrelUtil.fixTraits(call, traits));

/*    for (int i = 0; i < inputs.size(); i++) {
      RelNode convertedInput = convert(inputs.get(i), PrelUtil.fixTraits(call, traits));
      convertedInputList.add(convertedInput);
    }*/
    traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(DrillDistributionTrait.SINGLETON);

    //todo where we should get inputs?
    ValidatorPrel validatorPrel = new ValidatorPrel(rel.getCluster(), traits,
        convertedLeft, convertedRight, rel.getColumns());
    call.transformTo(validatorPrel);
  }
}