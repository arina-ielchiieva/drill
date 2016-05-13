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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.SchemaUtilites;
import org.apache.drill.exec.planner.sql.parser.SqlDescribeSchema;
import org.apache.drill.exec.store.StoragePlugin;

import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;

public class DescribeSchemaHandler extends DefaultSqlHandler {

  public DescribeSchemaHandler(SqlHandlerConfig config) {
    super(config);
  }

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DescribeSchemaHandler.class);
  private static final ObjectMapper mapper = new ObjectMapper().enable(INDENT_OUTPUT);

  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) {
    SqlIdentifier schema = ((SqlDescribeSchema) sqlNode).getSchema();
    SchemaPlus drillSchema = SchemaUtilites.findSchema(config.getConverter().getDefaultSchema(), schema.names);

    if (drillSchema != null) {
      StoragePlugin storagePlugin;
      try {
        storagePlugin = context.getStorage().getPlugin(schema.names.get(0));
      } catch (ExecutionSetupException e) {
        throw UserException.validationError()
            .message("Failure while retrieving storage plugin", e)
            .build(logger);
      }
      String properties;
      try {
        properties = mapper.writeValueAsString(storagePlugin.getConfig());
      } catch (JsonProcessingException e) {
        throw UserException.parseError()
            .message("Error while trying to convert storage config to json string")
            .build(logger);
      }
      return DirectPlan.createDirectPlan(context, new DescribeSchemaCommandResult(properties));
    }

    throw UserException.validationError()
          .message(String.format("Invalid schema name [%s]", Joiner.on(".").join(schema.names)))
          .build(logger);
  }
}
