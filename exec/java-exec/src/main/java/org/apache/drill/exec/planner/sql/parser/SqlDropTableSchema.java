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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.drill.exec.planner.sql.handlers.AbstractSqlHandler;
import org.apache.drill.exec.planner.sql.handlers.DropTableSchemaHandler;
import org.apache.drill.exec.planner.sql.handlers.SqlHandlerConfig;

import java.util.Arrays;
import java.util.List;

public class SqlDropTableSchema extends DrillSqlCall {

  private final SqlNode name;
  private final SqlIdentifier table;
  private final SqlNode path;
  private final SqlLiteral existenceCheck;

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("DROP_TABLE_SCHEMA", SqlKind.OTHER_DDL) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      return new SqlDropTableSchema(pos, operands[0], (SqlIdentifier) operands[1], operands[2], (SqlLiteral) operands[3]);
    }
  };

  public SqlDropTableSchema(SqlParserPos pos, SqlNode name, SqlIdentifier table, SqlNode path, SqlLiteral existenceCheck) {
    super(pos);
    this.name = name;
    this.table = table;
    this.path = path;
    this.existenceCheck = existenceCheck;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Arrays.asList(name, table, path, existenceCheck);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("DROP");
    writer.keyword("TABLE");
    writer.keyword("SCHEMA");

    if (ifExists()) {
      writer.keyword("IF");
      writer.keyword("EXISTS");
    }

    if (name != null) {
      writer.keyword("NAME");
      name.unparse(writer, leftPrec, rightPrec);
    }
    if (table != null) {
      writer.keyword("FOR");
      table.unparse(writer, leftPrec, rightPrec);
    }
    if (path != null) {
      writer.keyword("PATH");
      path.unparse(writer, leftPrec, rightPrec);
    }
  }

  @Override
  public AbstractSqlHandler getSqlHandler(SqlHandlerConfig config) {
    return new DropTableSchemaHandler(config);
  }

  public boolean ifExists() {
    return  existenceCheck.booleanValue();
  }

}
