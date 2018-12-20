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
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.drill.exec.planner.sql.handlers.AbstractSqlHandler;
import org.apache.drill.exec.planner.sql.handlers.SqlHandlerConfig;
import org.apache.drill.exec.planner.sql.handlers.TableSchemaHandler;
import org.apache.drill.exec.store.dfs.FileSelection;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Parent class for CREATE and DROP TABLE SCHEMA commands.
 * Holds logic common command property: name.
 */
public abstract class SqlTableSchema extends DrillSqlCall {

  protected final SqlIdentifier table;

  protected SqlTableSchema(SqlParserPos pos, SqlIdentifier table) {
    super(pos);
    this.table = table;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (table != null) {
      writer.keyword("FOR");
      table.unparse(writer, leftPrec, rightPrec);
    }
  }

  public SqlIdentifier getTable() {
    return table;
  }

  public List<String> getSchemaPath() {
    if (table == null) {
      return null;
    }

    return table.isSimple() ? Collections.emptyList() : table.names.subList(0, table.names.size() - 1);
  }

  public String getTableName() {
    if (table == null) {
      return null;
    }

    String tableName = table.isSimple() ? table.getSimple() : table.names.get(table.names.size() - 1);
    return FileSelection.removeLeadingSlash(tableName);
  }

  /**
   * Visits literal and returns bare value (i.e. single quotes).
   */
  public static class LiteralVisitor extends SqlBasicVisitor<String> {

    public static final LiteralVisitor INSTANCE = new LiteralVisitor();

    @Override
    public String visit(SqlLiteral literal) {
      return literal.toValue();
    }

  }

  /**
   * CREATE TABLE SCHEMA sql call.
   */
  public static class Create extends SqlTableSchema {

    private final SqlCharStringLiteral schema;
    private final SqlNode path;
    private final SqlNodeList properties;
    private final SqlLiteral createType;

    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE_TABLE_SCHEMA", SqlKind.OTHER_DDL) {
      @Override
      public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
        return new Create(pos, (SqlCharStringLiteral) operands[0],
          (SqlIdentifier) operands[1], operands[2], (SqlNodeList) operands[3], (SqlLiteral) operands[4]);
      }
    };

    public Create(SqlParserPos pos,
                                SqlCharStringLiteral schema,
                                SqlIdentifier table,
                                SqlNode path,
                                SqlNodeList properties,
                                SqlLiteral createType) {
      super(pos, table);
      this.schema = schema;
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
      return Arrays.asList(schema, table, path, properties, createType);
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

      super.unparse(writer, leftPrec, rightPrec);

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
      return new TableSchemaHandler.Create(config);
    }

    public String getSchema() {
      return schema.toValue();
    }

    public String getPath() {
      return path == null ? null : path.accept(LiteralVisitor.INSTANCE);
    }

    public LinkedHashMap<String, String> getProperties() {
      if (properties == null) {
        return null;
      }

      // preserve properties order
      LinkedHashMap<String, String> map = new LinkedHashMap<>();
      for (int i = 1; i < properties.size(); i += 2) {
        map.put(properties.get(i - 1).accept(LiteralVisitor.INSTANCE),
          properties.get(i).accept(LiteralVisitor.INSTANCE));
      }
      return map;
    }

    public SqlCreateType getSqlCreateType() {
      return SqlCreateType.valueOf(createType.toValue());
    }

  }



  /**
   * DROP TABLE SCHEMA sql call.
   */
  public static class Drop extends SqlTableSchema {

    private final SqlLiteral existenceCheck;

    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("DROP_TABLE_SCHEMA", SqlKind.OTHER_DDL) {
      @Override
      public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
        return new Drop(pos, (SqlIdentifier) operands[0], (SqlLiteral) operands[1]);
      }
    };

    public Drop(SqlParserPos pos, SqlIdentifier table, SqlLiteral existenceCheck) {
      super(pos, table);
      this.existenceCheck = existenceCheck;
    }

    @Override
    public SqlOperator getOperator() {
      return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
      return Arrays.asList(table, existenceCheck);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
      writer.keyword("DROP");
      writer.keyword("TABLE SCHEMA");

      if (ifExists()) {
        writer.keyword("IF");
        writer.keyword("EXISTS");
      }

      super.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public AbstractSqlHandler getSqlHandler(SqlHandlerConfig config) {
      return new TableSchemaHandler.Drop(config);
    }

    public boolean ifExists() {
      return existenceCheck.booleanValue();
    }

  }

}
