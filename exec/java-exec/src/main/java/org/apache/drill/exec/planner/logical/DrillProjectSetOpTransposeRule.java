/**
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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectSetOpTransposeRule;
import org.apache.calcite.rel.rules.PushProjector;

import java.util.ArrayList;
import java.util.List;

public class DrillProjectSetOpTransposeRule extends ProjectSetOpTransposeRule {
  public final static RelOptRule INSTANCE = new DrillProjectSetOpTransposeRule(DrillConditions.PRESERVE_ITEM);

  private final PushProjector.ExprCondition preserveExprCondition;

  protected DrillProjectSetOpTransposeRule(PushProjector.ExprCondition preserveExprCondition) {
    super(preserveExprCondition);
    this.preserveExprCondition = preserveExprCondition;
  }


  // implement RelOptRule
  public boolean matches(RelOptRuleCall call) {
    final LogicalProject origProj = call.rel(0);
    final SetOp setOp = call.rel(1);

    // If the LogicalProject on top of SetOp is a trivial Project,
    // it is not useful to push this project downward
    // Besides, cannot push project past a distinct
    return !ProjectRemoveRule.isTrivial(origProj)
        && setOp.all;
  }

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    LogicalProject origProj = call.rel(0);
    SetOp setOp = call.rel(1);

    // locate all fields referenced in the projection
    PushProjector pushProject =
        new PushProjector(origProj, null, setOp, preserveExprCondition);
    pushProject.locateAllRefs();

    List<RelNode> newSetOpInputs = new ArrayList<RelNode>();
    int[] adjustments = pushProject.getAdjustments();

    // push the projects completely below the setop; this
    // is different from pushing below a join, where we decompose
    // to try to keep expensive expressions above the join,
    // because UNION ALL does not have any filtering effect,
    // and it is the only operator this rule currently acts on
    for (RelNode input : setOp.getInputs()) {
      // be lazy:  produce two ProjectRels, and let another rule
      // merge them (could probably just clone origProj instead?)
      LogicalProject p =
          pushProject.createProjectRefsAndExprs(
              input, true, false);
      newSetOpInputs.add(
          pushProject.createNewProject(p, adjustments));
    }

    // create a new setop whose children are the ProjectRels created above
    SetOp newSetOp =
        setOp.copy(setOp.getTraitSet(), newSetOpInputs);

    call.transformTo(newSetOp);
  }

}