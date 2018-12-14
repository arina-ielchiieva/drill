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
package org.apache.drill.exec.planner.sql.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.drill.exec.planner.sql.handlers.AbstractSqlHandler;
import org.apache.drill.exec.planner.sql.handlers.CreateSchemaHandler;
import org.apache.drill.exec.planner.sql.handlers.SqlHandlerConfig;

import java.util.Arrays;
import java.util.List;

public class SqlCreateSchema extends DrillSqlCall {

   private final SqlIdentifier table;
   private final SqlNode schemaName;
   private final SqlCharStringLiteral schemaString;
   private final SqlNode path;
   private final SqlNodeList properties;


  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE_SCHEMA", SqlKind.OTHER_DDL) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      return new SqlCreateSchema(pos, (SqlIdentifier) operands[0], operands[1],
        (SqlCharStringLiteral) operands[2], operands[3], (SqlNodeList) operands[4]);
    }
  };

  public SqlCreateSchema(SqlParserPos pos,
                         SqlIdentifier table,
                         SqlNode schemaName,
                         SqlCharStringLiteral schemaString,
                         SqlNode path,
                         SqlNodeList properties) {
    super(pos);
    this.table = table;
    this.schemaName = schemaName;
    this.schemaString = schemaString;
    this.path = path;
    this.properties = properties;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Arrays.asList(table, schemaName, schemaString, path, properties);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    writer.keyword("SCHEMA");
    if (table != null) {
      writer.keyword("FOR TABLE");
      table.unparse(writer, leftPrec, rightPrec);
    }
    if (schemaName != null) {
      writer.keyword("AS");
      schemaName.unparse(writer, leftPrec, rightPrec);
    }
    writer.literal(getSchemaString());
    if (path != null) {
      writer.keyword("PATH");
      path.unparse(writer, leftPrec, rightPrec);
    }
    if (properties != null) {
      writer.keyword("PROPERTIES");
      writer.keyword("(");

      for (int i = 0; i < properties.size(); i++) {
        if (i != 0) {
          writer.keyword(",");
        }
        properties.get(i).unparse(writer, leftPrec, rightPrec);
        writer.keyword("=");
        properties.get(++i).unparse(writer, leftPrec, rightPrec);
      }

      writer.keyword(")");
    }
  }

  @Override
  public AbstractSqlHandler getSqlHandler(SqlHandlerConfig config) {
    return new CreateSchemaHandler(config);
  }

  public String getSchemaString() {
    return schemaString.toValue();
  }
}
