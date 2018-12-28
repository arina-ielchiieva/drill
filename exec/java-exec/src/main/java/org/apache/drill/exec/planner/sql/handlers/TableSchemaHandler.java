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
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.SchemaUtilites;
import org.apache.drill.exec.planner.sql.parser.SqlTableSchema;
import org.apache.drill.exec.record.metadata.schema.FsMetastoreTableSchemaProvider;
import org.apache.drill.exec.record.metadata.schema.PathTableSchemaProvider;
import org.apache.drill.exec.record.metadata.schema.TableSchemaProvider;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.dfs.WorkspaceSchemaFactory;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

public abstract class TableSchemaHandler extends DefaultSqlHandler {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TableSchemaHandler.class);

  TableSchemaHandler(SqlHandlerConfig config) {
    super(config);
  }

  WorkspaceSchemaFactory.WorkspaceSchema getWorkspaceSchema(List<String> tableSchema, String tableName) {
    SchemaPlus defaultSchema = config.getConverter().getDefaultSchema();
    AbstractSchema temporarySchema = SchemaUtilites.resolveToTemporarySchema(tableSchema, defaultSchema, context.getConfig());

    if (context.getSession().isTemporaryTable(temporarySchema, context.getConfig(), tableName)) {
      throw UserException.validationError()
        .message("Indicated table [%s] is temporary table", tableName)
        .build(logger);
    }

    AbstractSchema drillSchema = SchemaUtilites.resolveToMutableDrillSchema(defaultSchema, tableSchema);
    Table table = SqlHandlerUtil.getTableFromSchema(drillSchema, tableName);
    if (table == null || table.getJdbcTableType() != Schema.TableType.TABLE) {
      throw UserException.validationError().message("Table [%s] was not found", tableName).build(logger);
    }

    if (!(drillSchema instanceof WorkspaceSchemaFactory.WorkspaceSchema)) {
      throw UserException.validationError()
        .message("Table [`%s`.`%s`] must belong to file storage plugin", drillSchema.getFullSchemaName(), tableName)
        .build(logger);
    }

    return (WorkspaceSchemaFactory.WorkspaceSchema) drillSchema;
  }

  PhysicalPlan produceErrorResult(String message, boolean doFail) {
    if (doFail) {
      throw UserException.validationError().message(message).build(logger);
    } else {
      return DirectPlan.createDirectPlan(context, false, message);
    }
  }

  public static class Create extends TableSchemaHandler {

    public Create(SqlHandlerConfig config) {
      super(config);
    }

    @Override
    public PhysicalPlan getPlan(SqlNode sqlNode) {

      SqlTableSchema.Create sqlCall = ((SqlTableSchema.Create) sqlNode);

      String schemaSource = sqlCall.getTable() == null ? sqlCall.getPath() : sqlCall.getTable().toString();
      try {

        TableSchemaProvider schemaProvider;

        if (sqlCall.getTable() == null) {
          schemaProvider = new PathTableSchemaProvider(new Path(sqlCall.getPath()));
        } else {
          String tableName = sqlCall.getTableName();
          WorkspaceSchemaFactory.WorkspaceSchema wsSchema = getWorkspaceSchema(sqlCall.getSchemaPath(), tableName);
          schemaProvider = new FsMetastoreTableSchemaProvider(wsSchema, tableName);
        }

        // validate creation possibility
        if (schemaProvider.exists()) {
          boolean doFail = false;
          switch (sqlCall.getSqlCreateType()) {
            case OR_REPLACE:
              schemaProvider.delete();
              break;
            case SIMPLE:
              doFail = true;
              // fall through
            case IF_NOT_EXISTS:
              return produceErrorResult(String.format("Schema file already exists for [%s]", schemaSource), doFail);
          }
        }

        schemaProvider.store(sqlCall.getSchema(), sqlCall.getProperties());
        return DirectPlan.createDirectPlan(context, true, String.format("Created table schema for [%s]", schemaSource));

      } catch (IOException e) {
        throw UserException.resourceError(e)
          .message("Error while preparing / creating schema file path for [%s]", schemaSource)
          .addContext(e.getMessage())
          .build(logger);
      }
    }
  }

  public static class Drop extends TableSchemaHandler {

    public Drop(SqlHandlerConfig config) {
      super(config);
    }

    @Override
    public PhysicalPlan getPlan(SqlNode sqlNode) {
      SqlTableSchema.Drop sqlCall = ((SqlTableSchema.Drop) sqlNode);

      String tableName = sqlCall.getTableName();
      WorkspaceSchemaFactory.WorkspaceSchema wsSchema = getWorkspaceSchema(sqlCall.getSchemaPath(), tableName);

      try {

        TableSchemaProvider schemaProvider = new FsMetastoreTableSchemaProvider(wsSchema, tableName);

        if (!schemaProvider.exists()) {
          return produceErrorResult(String.format("Schema file [%s] does not exist in table [%s] root directory",
            TableSchemaProvider.DEFAULT_SCHEMA_NAME, sqlCall.getTable()), !sqlCall.ifExists());
        }

        schemaProvider.delete();

        return DirectPlan.createDirectPlan(context, true,
          String.format("Dropped schema file for table [%s]", sqlCall.getTable()));

      } catch (IOException e) {
        throw UserException.resourceError(e)
          .message("Error while accessing table location or deleting schema file for [%s]", sqlCall.getTable())
          .build(logger);
      }
    }
  }

}
