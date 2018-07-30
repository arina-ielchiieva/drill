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

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.planner.sql.SchemaUtilites;
import org.apache.drill.exec.planner.sql.parser.DrillParserUtil;
import org.apache.drill.exec.planner.sql.parser.SqlShowFiles;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.dfs.WorkspaceSchemaFactory.WorkspaceSchema;
import org.apache.drill.exec.store.ischema.InfoSchemaTableType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.FILES_COL_SCHEMA_NAME;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.IS_SCHEMA_NAME;


public class ShowFileHandler extends DefaultSqlHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SetOptionHandler.class);

  public ShowFileHandler(SqlHandlerConfig config) {
    super(config);
  }

  /** Rewrite the parse tree as SELECT ... FROM INFORMATION_SCHEMA.FILES ... */
  @Override
  public SqlNode rewrite(SqlNode sqlNode) {

    List<SqlNode> selectList = Collections.singletonList(SqlIdentifier.star(SqlParserPos.ZERO));

    SqlNode fromClause = new SqlIdentifier(
        Arrays.asList(IS_SCHEMA_NAME, InfoSchemaTableType.FILES.name()), null, SqlParserPos.ZERO, null);

    SchemaPlus defaultSchema = config.getConverter().getDefaultSchema();
    SchemaPlus drillSchema = defaultSchema;

    SqlIdentifier from = ((SqlShowFiles) sqlNode).getDb();

    // Show files can be used without from clause, in which case we display the files in the default schema
    if (from != null) {
      // We are not sure if the full from clause is just the schema or includes table name,
      // first try to see if the full path specified is a schema
      drillSchema = SchemaUtilites.findSchema(defaultSchema, from.names);
      if (drillSchema == null) {
        // Entire from clause is not a schema, try to obtain the schema without the last part of the specified clause.
        drillSchema = SchemaUtilites.findSchema(defaultSchema, from.names.subList(0, from.names.size() - 1));
      }

      if (drillSchema == null) {
        throw UserException.validationError()
            .message("Invalid FROM/IN clause [%s]", from.toString())
            .build(logger);
      }
    }

    WorkspaceSchema wsSchema;
    try {
      wsSchema = (WorkspaceSchema) drillSchema.unwrap(AbstractSchema.class).getDefaultSchema();
    } catch (ClassCastException e) {
      throw UserException.validationError()
          .message("SHOW FILES is supported in workspace type schema only. Schema [%s] is not a workspace schema.",
              SchemaUtilites.getSchemaPath(drillSchema))
          .build(logger);
    }

    SqlNode where = DrillParserUtil.createCondition(new SqlIdentifier(FILES_COL_SCHEMA_NAME, SqlParserPos.ZERO),
        SqlStdOperatorTable.EQUALS, SqlLiteral.createCharString(wsSchema.getFullSchemaName(), SqlParserPos.ZERO));

    return new SqlSelect(SqlParserPos.ZERO, null, new SqlNodeList(selectList, SqlParserPos.ZERO), fromClause, where,
        null, null, null, null, null, null);
  }
}
