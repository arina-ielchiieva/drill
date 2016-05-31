/*
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
package org.apache.drill.exec.planner.sql.handlers;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.parser.SqlDeleteUDF;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.work.foreman.ForemanSetupException;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class DeleteUDFHandler extends DefaultSqlHandler {

  private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DeleteUDFHandler.class);

  public DeleteUDFHandler(SqlHandlerConfig config) {
    super(config);
  }

  /**
   * Deletes UDFs dynamically.
   * @return - Single row indicating list of removed UDFs, raise exception otherwise
   */
  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ForemanSetupException, IOException {
    SqlDeleteUDF node = unwrap(sqlNode, SqlDeleteUDF.class);

    // get binaries jar name
    String jarName = ((SqlCharStringLiteral) node.getJarName()).toValue();
    UserProtos.JarHolder jarHolder = UserProtos.JarHolder.newBuilder().setName(jarName).build();

    ExecutorService executor = Executors.newFixedThreadPool(10); //todo remove hardcode
    List<Pair<CoordinationProtos.DrillbitEndpoint, Future<UserProtos.StringList>>> futureList = Lists.newArrayList();
    Set<String> deletedUDFs = Sets.newHashSet();
    boolean anyErrors = false;
    try {
      for (CoordinationProtos.DrillbitEndpoint endpoint : context.getActiveEndpoints()) {
        final DrillRpcFuture<UserProtos.StringList> deleteUdf = context.getController().getTunnel(endpoint).deleteUDF(jarHolder);
        futureList.add(new ImmutablePair<>(endpoint, executor.submit(new Callable<UserProtos.StringList>() {
          @Override
          public UserProtos.StringList call() throws Exception {
            return deleteUdf.get();
          }
        })));
      }

      for (Pair<CoordinationProtos.DrillbitEndpoint, Future<UserProtos.StringList>> endpointPair : futureList) {
        try {
          deletedUDFs.addAll(endpointPair.getValue().get().getListList());
        } catch (Exception e) {
          anyErrors = true;
          logger.error("Error during UDFs deletion on remote drillbit {}", endpointPair.getKey().getAddress(), e);
        }
      }
    } finally {
      executor.shutdown();
    }

    return DirectPlan.createDirectPlan(context, !anyErrors, anyErrors ?
        "There were problems during UDFs deletion. Look in log for more information" :
        "UDFs have been deleted successfully - " + deletedUDFs);
  }
}