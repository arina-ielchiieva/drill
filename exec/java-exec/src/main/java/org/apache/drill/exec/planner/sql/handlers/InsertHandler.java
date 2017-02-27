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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.DrillScreenRel;
import org.apache.drill.exec.planner.logical.DrillValidatorRel;
import org.apache.drill.exec.planner.logical.DrillWriterRel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.sql.SchemaUtilites;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.StorageStrategy;
import org.apache.drill.exec.store.parquet.ParquetGroupScan;
import org.apache.drill.exec.util.Pointer;
import org.apache.drill.exec.work.foreman.ForemanSetupException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class InsertHandler extends DefaultSqlHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InsertHandler.class);

  public InsertHandler(SqlHandlerConfig config, Pointer<String> textPlan) {
    super(config, textPlan);
  }

  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException, ForemanSetupException {
    final SqlNode[] operands = ((SqlBasicCall) sqlNode).getOperands();

    // get target operands
    final SqlIdentifier target = (SqlIdentifier) operands[1];
    final String targetTableName = target.names.get(target.names.size() - 1);
    final List<String> targetSchema = target.names.subList(0, target.names.size() - 1);

    final List<String> targetColumnList = Lists.newArrayList();
    for (SqlNode node : ((SqlNodeList) operands[3]).getList()) {
      targetColumnList.add(node.toString());
    }

    // validate target schema and table
    SchemaPlus defaultSchema = config.getConverter().getDefaultSchema();
    DrillConfig drillConfig = context.getConfig();
    UserSession session = context.getSession();
    String resolvedTargetTableName = targetTableName;
    AbstractSchema resolvedTargetSchema;
    StorageStrategy storageStrategy = new StorageStrategy(context.getOption(ExecConstants.PERSISTENT_TABLE_UMASK).string_val, false);
    if (targetSchema.isEmpty()) {
      AbstractSchema temporarySchema = SchemaUtilites.getTemporaryWorkspace(defaultSchema, drillConfig);
      boolean temporaryTable = session.isTemporaryTable(temporarySchema, drillConfig, targetTableName);
      if (temporaryTable) {
        resolvedTargetTableName = session.resolveTemporaryTableName(targetTableName);
        resolvedTargetSchema = temporarySchema;
        storageStrategy = StorageStrategy.TEMPORARY;
      } else {
        resolvedTargetSchema = SchemaUtilites.resolveToMutableDrillSchema(defaultSchema, targetSchema);
      }
    } else {
      resolvedTargetSchema = SchemaUtilites.resolveToMutableDrillSchema(defaultSchema, targetSchema);
      boolean temporaryTable = session.isTemporaryTable(resolvedTargetSchema, drillConfig, targetTableName);
      if (temporaryTable) {
        resolvedTargetTableName = session.resolveTemporaryTableName(targetTableName);
        storageStrategy = StorageStrategy.TEMPORARY;
      }
    }

    // target table should exist and be of table table
    Table targetTable = SqlHandlerUtil.getTableFromSchema(resolvedTargetSchema, resolvedTargetTableName);
    if (targetTable == null) {
      throw UserException
          .validationError()
          .message("A table with given name [%s] is absent in schema [%s]",
              targetTableName, resolvedTargetSchema.getFullSchemaName())
          .build(logger);
    } else if (targetTable.getJdbcTableType() != Schema.TableType.TABLE) {
      throw UserException
          .validationError()
          .message("Object with given name [%s] in schema [%s] is not a table. Object type: [%s]",
              targetTableName, resolvedTargetSchema.getFullSchemaName(), targetTable.getJdbcTableType())
          .build(logger);
    }

    // validate store type
    //todo can we allow insert in any store type but it would be on customer
    String storeType = context.getOption(ExecConstants.OUTPUT_FORMAT_OPTION).string_val;
    if (!"parquet".equalsIgnoreCase(storeType)) {
      throw UserException
          .validationError()
          .message("Insert is only allowed when storage type is parquet")
          .build(logger);
    }

    // construct and convert target table select all limit 0 query
    SqlSelect targetSelectAll = new SqlSelect(SqlParserPos.ZERO, null,
        new SqlNodeList(Lists.<SqlNode>newArrayList(new SqlIdentifier("*", SqlParserPos.ZERO)), SqlParserPos.ZERO), //todo replace star to constant
        new SqlIdentifier(Lists.newArrayList(resolvedTargetSchema.getFullSchemaName(), resolvedTargetTableName), SqlParserPos.ZERO),
        null, null, null, null, null, null, null);

    SqlOrderBy targetLimitZero = new SqlOrderBy(SqlParserPos.ZERO, targetSelectAll,
        new SqlNodeList(Lists.<SqlNode>newArrayList(), SqlParserPos.ZERO), null, SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO));

    ConvertedRelNode convertedTarget = validateAndConvert(targetLimitZero);
    RelNode convertedTargetNode = convertedTarget.getConvertedNode();
    DrillRel targetRel = convertToDrel(convertedTargetNode);

    //todo can we allow insert in any store type but it would be on customer
    // validate target table type
    if (targetRel.getInputs().size() == 1 && targetRel.getInput(0) instanceof DrillScanRel) {
    if (!(((DrillScanRel) targetRel.getInput(0)).getGroupScan() instanceof ParquetGroupScan)) {
      throw UserException
          .validationError()
          .message("Insert is only allowed into parquet table")
          .build(logger);
      }
    } else {
      throw UserException
          .validationError()
          .message("Unable to determine target table storage type")
          .build(logger);
    }

    // check source
    final SqlNode source = operands[2];
    ConvertedRelNode convertedSource = validateAndConvert(source);
    RelNode convertedSourceNode = convertedSource.getConvertedNode();
    DrillRel sourceRel = convertToDrel(convertedSourceNode);
    RelDataType sourceRowType = convertedSource.getValidatedRowType();
    boolean checkColumnsCount = true;
    for (String fieldName : sourceRowType.getFieldNames()) {
      if ("*".equals(fieldName)) {
        checkColumnsCount = false;
        break;
      }
    }

    /*
    todo replace to predicate above foreach
        boolean isStarQuery = Iterables.tryFind(getRowType().getFieldNames(), new Predicate<String>() {
      @Override
      public boolean apply(String input) {
        return Preconditions.checkNotNull(input).equals("*");
      }
    }).isPresent();

    if (isStarQuery) {
      columnCount = STAR_COLUMN_COST;
    }
     */

    // check columns count only if source does not have *
    if (checkColumnsCount) {
      if (targetColumnList.size() != sourceRowType.getFieldCount()) {
        throw UserException
            .validationError()
            .message("Target and source column count should match.\n" + "Target (columns # %s): %s.\n Source (columns # %s): %s.",
                targetColumnList.size(), targetColumnList, sourceRowType.getFieldCount(), sourceRowType.getFieldNames())
            .build(logger);
      }
    }

    // convert to Drel
    // add validator, writer and screen rels
    RelTraitSet traitSet = sourceRel.getCluster().traitSet().plus(DrillRel.DRILL_LOGICAL);
    //todo add validator
    //todo may be validator rel can prepare itself select * from target
    DrillValidatorRel validatorRel = new DrillValidatorRel(sourceRel.getCluster(), traitSet, targetRel, sourceRel, targetColumnList);

    DrillWriterRel writerRel = new DrillWriterRel(validatorRel.getCluster(), traitSet, validatorRel,
        //todo createTableEntry is not logically correct (add new entry or rename existent -> can break anything?)
        resolvedTargetSchema.createNewTable(resolvedTargetTableName, Collections.<String>emptyList(), storageStrategy));
    DrillScreenRel screenRel = new DrillScreenRel(writerRel.getCluster(), writerRel.getTraitSet(), writerRel);

    // convert to plan
    Prel prel = convertToPrel(screenRel);
    logAndSetTextPlan("Drill Physical", prel, logger);
    PhysicalOperator pop = convertToPop(prel);
    PhysicalPlan plan = convertToPlan(pop);
    log("Drill Plan", plan, logger);
    return plan;
  }
}
