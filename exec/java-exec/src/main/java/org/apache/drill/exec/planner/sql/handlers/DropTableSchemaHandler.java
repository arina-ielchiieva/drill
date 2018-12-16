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
package org.apache.drill.exec.planner.sql.handlers;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.SchemaUtilites;
import org.apache.drill.exec.planner.sql.parser.SqlTableSchema;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.WorkspaceSchemaFactory;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

public class DropTableSchemaHandler extends DefaultSqlHandler  {

  private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DropTableSchemaHandler.class);

  public static final String DEFAULT_SCHEMA_NAME = ".drill.table_schema";

  public DropTableSchemaHandler(SqlHandlerConfig config) {
    super(config);
  }

  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException, ForemanSetupException {

    SqlTableSchema.Drop sqlCall = ((SqlTableSchema.Drop) sqlNode);

    String schemaName = sqlCall.getName() == null ? DEFAULT_SCHEMA_NAME : sqlCall.getName();

    if (sqlCall.getPath() != null) {
      Path schemaFilePath = new Path(sqlCall.getPath(), schemaName);
      // get default file system ...
      FileSystem fs = schemaFilePath.getFileSystem(new Configuration());
      //todo kind the same logic regarding drop behavior as in below if
    } else {
      String tableName = FileSelection.removeLeadingSlash(sqlCall.getTableName());
      SchemaPlus defaultSchema = config.getConverter().getDefaultSchema();
      List<String> tableSchema = sqlCall.getSchemaPath();
      DrillConfig drillConfig = context.getConfig();
      UserSession session = context.getSession();

      AbstractSchema temporarySchema = SchemaUtilites.resolveToTemporarySchema(tableSchema, defaultSchema, drillConfig);

      if (session.isTemporaryTable(temporarySchema, drillConfig, tableName)) {
        throw UserException.validationError()
          .message("Indicated table [%s] is temporary table.", tableName)
          .build(logger);
      }

      AbstractSchema drillSchema = SchemaUtilites.resolveToMutableDrillSchema(defaultSchema, tableSchema);
      Table table = SqlHandlerUtil.getTableFromSchema(drillSchema, tableName);
      if (table == null || table.getJdbcTableType() != Schema.TableType.TABLE) {
        if (sqlCall.ifExists()) {
          return DirectPlan.createDirectPlan(context, false,
            String.format("Schema was not dropped. Table [%s] was not found.", tableName));
        } else {
          throw UserException.validationError()
            .message("Table [%s] not found.", tableName)
            .build(logger);
        }
      }

      if (!(drillSchema instanceof WorkspaceSchemaFactory.WorkspaceSchema)) {
        throw UserException.validationError()
          //todo
          .message("Schema was not dropped. Table does exists in Drill file storage plugin.", tableName)
          .build(logger);
      }

      WorkspaceSchemaFactory.WorkspaceSchema wsSchema = (WorkspaceSchemaFactory.WorkspaceSchema) drillSchema;
      FileSystem fs = wsSchema.getFS();
      Path tablePath = new Path(wsSchema.getDefaultLocation(), tableName);
      Path schemaParentPath;
      if (fs.isFile(tablePath)) {
        schemaParentPath = tablePath.getParent();
      } else {
        schemaParentPath = tablePath;
      }

      Path schemaFilePath = new Path(schemaParentPath, schemaName);

      if (fs.exists(schemaFilePath)) {
        boolean isSuccess;
        try {
          //todo will fail if file is absent? when returns false?????
          isSuccess = fs.delete(schemaFilePath, false); // todo boolean return handling
        } catch (IOException e) {
          // re-check if file was deleted
          if (fs.exists(schemaFilePath)) {
            throw e; //todo proper handling
          }
          isSuccess = true;
        }

        if (isSuccess) {
          return DirectPlan.createDirectPlan(context, true,
            //todo better message
            String.format("Schema was dropped.", schemaName));
        } else {
          if (sqlCall.ifExists()) {
            return DirectPlan.createDirectPlan(context, false,
              //todo better error message - because file does not exist
              String.format("Schema was not dropped.", schemaName));
          } else {
            throw UserException.validationError()
              //todo better error message
              .message("Schema file was not dropped successfully.", tableName)
              .build(logger);
          }
        }

      } else {
        if (sqlCall.ifExists()) {
          return DirectPlan.createDirectPlan(context, false,
            //todo better error message - because file does not exist
            String.format("Schema file is absent.", schemaName));
        } else {
          throw UserException.validationError()
            //todo better error message
            .message("Schema file is absent.", tableName)
            .build(logger);
        }
      }
    }


    System.out.println(sqlNode.toString());
    return DirectPlan.createDirectPlan(context, true, "Dropped table schema");
  }

}
