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
package org.apache.drill.exec.planner.sql.handlers;

import java.io.IOException;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.dotdrill.View;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.SchemaUtilites;
import org.apache.drill.exec.planner.sql.parser.SqlCreateView;
import org.apache.drill.exec.planner.sql.parser.SqlDropView;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;

public abstract class ViewHandler extends DefaultSqlHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ViewHandler.class);

  protected QueryContext context;

  public ViewHandler(SqlHandlerConfig config) {
    super(config);
    this.context = config.getContext();
  }

  /**
   * If view to be dropped is in default temporary workspace, checks if it's a temporary table or not.
   *
   * @param schema view schema
   * @param viewName view name to be created
   * @return true is object to be created is temporary table, false otherwise
   */
  protected boolean isTemporaryTable(AbstractSchema schema, String viewName) {
    if (schema.getFullSchemaName().equals(context.getConfig().getString(ExecConstants.DEFAULT_TEMPORARY_WORKSPACE))) {
      String temporaryTableName = context.getSession().findTemporaryTable(viewName);
      if (temporaryTableName != null) {
        Table temporaryTable = SqlHandlerUtil.getTableFromSchema(schema, temporaryTableName);
        return temporaryTable != null && temporaryTable.getJdbcTableType() == Schema.TableType.TABLE;
      }
    }
    return false;
  }

  /** Handler for Create View DDL command */
  public static class CreateView extends ViewHandler {

    public CreateView(SqlHandlerConfig config) {
      super(config);
    }

    @Override
    public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException, ForemanSetupException {
      SqlCreateView createView = unwrap(sqlNode, SqlCreateView.class);

      final String newViewName = createView.getName();

      // Store the viewSql as view def SqlNode is modified as part of the resolving the new table definition below.
      final String viewSql = createView.getQuery().toString();

      final ConvertedRelNode convertedRelNode = validateAndConvert(createView.getQuery());
      final RelDataType validatedRowType = convertedRelNode.getValidatedRowType();
      final RelNode queryRelNode = convertedRelNode.getConvertedNode();

      final RelNode newViewRelNode = SqlHandlerUtil.resolveNewTableRel(true, createView.getFieldNames(), validatedRowType, queryRelNode);

      final SchemaPlus defaultSchema = context.getNewDefaultSchema();
      final AbstractSchema drillSchema = SchemaUtilites.resolveToMutableDrillSchema(defaultSchema, createView.getSchemaPath());

      final View view = new View(newViewName, viewSql, newViewRelNode.getRowType(),
          SchemaUtilites.getSchemaPathAsList(defaultSchema));

      validateViewCreationPossibility(drillSchema, newViewName, createView.getReplace());

      final boolean replaced = drillSchema.createView(view);
      final String summary = String.format("View '%s' %s successfully in '%s' schema",
          createView.getName(), replaced ? "replaced" : "created", drillSchema.getFullSchemaName());

      return DirectPlan.createDirectPlan(context, true, summary);
    }

    /**
     * Validates if view can be created in indicated schema:
     * checks if object (persistent / temporary table) with the same exists
     * in indicated schema, or if view exists but replace flag is not set.
     *
     * @param drillSchema schema where views will be created
     * @param viewName view name
     * @param replaceView replace view if exists
     * @throws UserException if views can be created in indicated schema
     */
    private void validateViewCreationPossibility(AbstractSchema drillSchema, String viewName, boolean replaceView) {
      final String schemaPath = drillSchema.getFullSchemaName();
      final Table existingTable = SqlHandlerUtil.getTableFromSchema(drillSchema, viewName);

      if ((existingTable != null && existingTable.getJdbcTableType() != Schema.TableType.VIEW) ||
          isTemporaryTable(drillSchema, viewName)) {
        // existing table is not a view
        throw UserException
            .validationError()
            .message("A non-view table with given name [%s] already exists in schema [%s]", viewName, schemaPath)
            .build(logger);
      }

      if ((existingTable != null && existingTable.getJdbcTableType() == Schema.TableType.VIEW) && !replaceView) {
          // existing table is a view and create view has no "REPLACE" clause
        throw UserException
            .validationError()
            .message("A view with given name [%s] already exists in schema [%s]", viewName, schemaPath)
            .build(logger);
      }
    }
  }

  /** Handler for Drop View [If Exists] DDL command. */
  public static class DropView extends ViewHandler {
    public DropView(SqlHandlerConfig config) {
      super(config);
    }

    @Override
    public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException, ForemanSetupException {
      SqlDropView dropView = unwrap(sqlNode, SqlDropView.class);
      final String viewName = dropView.getName();
      final AbstractSchema drillSchema =
          SchemaUtilites.resolveToMutableDrillSchema(context.getNewDefaultSchema(), dropView.getSchemaPath());

      final String schemaPath = drillSchema.getFullSchemaName();

      final Table viewToDrop = SqlHandlerUtil.getTableFromSchema(drillSchema, viewName);
      final boolean isTemporaryTable = isTemporaryTable(drillSchema, viewName);

      if (dropView.checkViewExistence()) {
        if (!isTemporaryTable && (viewToDrop == null || viewToDrop.getJdbcTableType() != Schema.TableType.VIEW)) {
          return DirectPlan.createDirectPlan(context, false,
              String.format("View [%s] not found in schema [%s].", viewName, schemaPath));
        }
      } else {
        if (isTemporaryTable || (viewToDrop != null && viewToDrop.getJdbcTableType() != Schema.TableType.VIEW)) {
          throw UserException
              .validationError()
              .message("[%s] is not a VIEW in schema [%s]", viewName, schemaPath)
              .build(logger);
        } else if (viewToDrop == null) {
          throw UserException
              .validationError()
              .message("Unknown view [%s] in schema [%s].", viewName, schemaPath)
              .build(logger);
        }
      }

      try {
        drillSchema.dropView(viewName);
      } catch (Exception e) {
        // concurrency check: we might have failed because somebody has already dropped the view
        if (SqlHandlerUtil.getTableFromSchema(drillSchema, viewName) != null) {
          throw e;
        }
      }

      return DirectPlan.createDirectPlan(context, true,
          String.format("View [%s] deleted successfully from schema [%s].", viewName, schemaPath));
    }
  }
}
