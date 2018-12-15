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
import org.apache.drill.exec.planner.sql.handlers.CreateTableSchemaHandler;
import org.apache.drill.exec.planner.sql.handlers.SqlHandlerConfig;

import java.util.Arrays;
import java.util.List;

public class SqlCreateTableSchema extends DrillSqlCall {

  private final SqlCharStringLiteral schema;
  private final SqlNode name;
  private final SqlIdentifier table;
  private final SqlNode path;
  private final SqlNodeList properties;
  private final SqlLiteral createType;

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE_TABLE_SCHEMA", SqlKind.OTHER_DDL) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      return new SqlCreateTableSchema(pos, (SqlCharStringLiteral) operands[0], operands[1],
        (SqlIdentifier) operands[2], operands[3], (SqlNodeList) operands[4], (SqlLiteral) operands[5]);
    }
  };

  public SqlCreateTableSchema(SqlParserPos pos,
                              SqlCharStringLiteral schema,
                              SqlNode name,
                              SqlIdentifier table,
                              SqlNode path,
                              SqlNodeList properties,
                              SqlLiteral createType) {
    super(pos);
    this.schema = schema;
    this.name = name;
    this.table = table;
    this.path = path;
    this.properties = properties;
    this.createType = createType;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Arrays.asList(schema, name, table, path, properties, createType);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");

    switch (getSqlCreateType()) {
      case SIMPLE:
        writer.keyword("TABLE SCHEMA");
        break;
      case OR_REPLACE:
        writer.keyword("OR");
        writer.keyword("REPLACE");
        writer.keyword("TABLE SCHEMA");
        break;
      case IF_NOT_EXISTS:
        writer.keyword("TABLE SCHEMA");
        writer.keyword("IF");
        writer.keyword("NOT");
        writer.keyword("EXISTS");
    }

    writer.literal(getSchema());

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
    if (properties != null) {
      writer.keyword("PROPERTIES");
      writer.keyword("(");

      for (int i = 1; i < properties.size(); i += 2) {
        if (i != 1) {
          writer.keyword(",");
        }
        properties.get(i - 1).unparse(writer, leftPrec, rightPrec);
        writer.keyword("=");
        properties.get(i).unparse(writer, leftPrec, rightPrec);
      }

      writer.keyword(")");
    }
  }

  @Override
  public AbstractSqlHandler getSqlHandler(SqlHandlerConfig config) {
    return new CreateTableSchemaHandler(config);
  }

  public String getSchema() {
    return schema.toValue();
  }

  public SqlCreateType getSqlCreateType() {
    return SqlCreateType.valueOf(createType.toValue());
  }

}
