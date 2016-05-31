/**
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
 */
package org.apache.drill.exec.planner.sql.handlers;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.UDFHandler;
import org.apache.drill.exec.planner.sql.parser.SqlCreateUDF;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.work.foreman.ForemanSetupException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.*;

public class CreateUDFHandler extends DefaultSqlHandler {

  private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CreateUDFHandler.class);

  public CreateUDFHandler(SqlHandlerConfig config) {
    super(config);
  }

  /**
   * Creates UDFs dynamically.
   * @return - Single row indicating list of created UDFs, raise exception otherwise
   */
  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ForemanSetupException, IOException {
    SqlCreateUDF node = unwrap(sqlNode, SqlCreateUDF.class);

    UDFHandler udfHandler = new UDFHandler(context.getFunctionRegistry());

    // get path to binary and source jars
    String pathToBinaryJar = ((SqlCharStringLiteral) node.getPathToJar()).toValue();

    // we assume that source jar is in the same directory with jar and both jars have standard naming convention
    String pathToSourceJar = pathToBinaryJar.replace(".jar", "-sources.jar");

    // create file using path to jar and sources
    File initialBinary = new File(pathToBinaryJar);
    File initialSource = new File(pathToSourceJar);

    // check binary and source jars exist
    checkFileExistence(initialBinary, initialSource);

    // send to current endpoint
    // prepare UDFHolder to pass to remote endpoints
    UserProtos.JarHolder binaryHolder = UserProtos.JarHolder.newBuilder()
        .setName(initialBinary.getName())
        .addContent(ByteString.copyFrom(Files.readAllBytes(initialBinary.toPath())))
        .build();

    UserProtos.JarHolder sourceHolder = UserProtos.JarHolder.newBuilder()
        .setName(initialSource.getName())
        .addContent(ByteString.copyFrom(Files.readAllBytes(initialSource.toPath())))
        .build();

    UserProtos.UDFHolder udfHolder = UserProtos.UDFHolder.newBuilder()
        .setBinary(binaryHolder)
        .setSource(sourceHolder)
        .build();

    List<String> registeredUDFs;
    try {
      registeredUDFs = udfHandler.createUDF(udfHolder).getListList();
    } catch (Exception e) {
      throw UserException.validationError()
          .message("UDFs registration error on current drillbit " + context.getCurrentEndpoint().getAddress())
          .build(logger);
    }

    // create list of drillbits where we need to perform cleanup in case of failure during udf registration
    List<CoordinationProtos.DrillbitEndpoint> endpointsToCleanup = Lists.newArrayList();
    boolean needCleanup = false;

    // add current endpoint since registration on current endpoint was successful
    endpointsToCleanup.add(context.getCurrentEndpoint());

    // start registering UDFs on remote endpoints
    if (context.getActiveEndpoints().size() > 1) {

      // prepare executor to register UDFs on remote endpoints in parallel
      ExecutorService executor = Executors.newFixedThreadPool(10); //todo remove hardcode
      List<Pair<CoordinationProtos.DrillbitEndpoint, Future<UserProtos.StringList>>> futureList = Lists.newArrayList();
      try {
        for (CoordinationProtos.DrillbitEndpoint endpoint : context.getActiveEndpoints()) {
          // exclude current endpoint since UDFs have been registered there already
          if (endpoint.equals(context.getCurrentEndpoint())) {
            continue;
          }
          final DrillRpcFuture<UserProtos.StringList> udf = context.getController().getTunnel(endpoint).createUDF(udfHolder);
          futureList.add(new ImmutablePair<>(endpoint, executor.submit(new Callable<UserProtos.StringList>() {
            @Override
            public UserProtos.StringList call() throws Exception {
              return udf.get();
            }
          })));
        }

        for (Pair<CoordinationProtos.DrillbitEndpoint, Future<UserProtos.StringList>> endpointPair : futureList) {
          try {
            endpointPair.getValue().get();
            endpointsToCleanup.add(endpointPair.getKey());
          } catch (Exception e) {
            needCleanup = true;
            logger.error("Error during UDFs registration on remote drillbit {}", endpointPair.getKey().getAddress(), e);
          }
        }
      } catch (Exception e) {
        needCleanup = true;
        logger.error("Error during remote UDFs registration", e);
      } finally {
        if (needCleanup) {
          futureList = Lists.newArrayList();
          UserProtos.JarHolder jarHolder = UserProtos.JarHolder.newBuilder().setName(initialBinary.getName()).build();
          for (CoordinationProtos.DrillbitEndpoint endpoint : endpointsToCleanup) {
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
              endpointPair.getValue().get();
            } catch (Exception e) {
              logger.error("Error during UDFs deletion on remote drillbit {}", endpointPair.getKey().getAddress(), e);
            }
          }

        }
        executor.shutdown();
      }
    }

    return DirectPlan.createDirectPlan(context, !needCleanup,
        needCleanup ? "There were problem during creating UDFs. Look in logs for more information" :
            "The following UDFs have been created " + registeredUDFs);
  }

  private void checkFileExistence(File file, File... files) {
    List<File> fileList = Lists.newArrayList(files);
    fileList.add(file);
    for (File f : fileList) {
      if (!f.exists()) {
        throw UserException.validationError()
            .message(String.format("File %s doesn't exist", f))
            .build(logger);
      }
    }
  }

}