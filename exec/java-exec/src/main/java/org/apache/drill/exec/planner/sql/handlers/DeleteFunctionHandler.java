package org.apache.drill.exec.planner.sql.handlers;/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.parser.SqlDeleteFunction;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.rpc.control.ControlTunnel;
import org.apache.drill.exec.work.foreman.ForemanSetupException;

import java.io.IOException;
import java.util.Collection;

public class DeleteFunctionHandler extends DefaultSqlHandler {

  private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DeleteFunctionHandler.class);

  public DeleteFunctionHandler(SqlHandlerConfig config) {
    super(config);
  }

  /**
   * Deletes udf-s dynamically.
   * @return - Single row indicating list of removed udf-s, raise exception otherwise
   */
  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ForemanSetupException, IOException {
    SqlDeleteFunction node = unwrap(sqlNode, SqlDeleteFunction.class);

    // get jar name
    String jarName = ((SqlCharStringLiteral) node.getJarName()).toValue();
    Collection<CoordinationProtos.DrillbitEndpoint> activeEndpoints = context.getActiveEndpoints();
    boolean anyErrors = false;
    for (CoordinationProtos.DrillbitEndpoint endpoint : activeEndpoints) {
      try {
        final ControlTunnel controlTunnel = context.getController().getTunnel(endpoint);
        final ControlTunnel.CustomTunnel<String, String> tunnel = controlTunnel.getCustomTunnel(3, CreateFunctionHandler.SIMPLE_SERDE, CreateFunctionHandler.SIMPLE_SERDE);
        tunnel.send(jarName).get();
      } catch (Exception e) {
        anyErrors = true;
        logger.warn("There were problems during clean up on endpoint " + endpoint, e);
      }
    }

    return DirectPlan.createDirectPlan(context, !anyErrors, anyErrors ?
        "There were problems during function deletion. Look in log for more information" :
        "Function(-s) has(-ve) been deleted successfully");
  }
}
