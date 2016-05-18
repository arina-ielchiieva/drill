package org.apache.drill.exec.planner.sql.parser;/*
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

import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.drill.exec.planner.sql.handlers.AbstractSqlHandler;
import org.apache.drill.exec.planner.sql.handlers.DeleteFunctionHandler;
import org.apache.drill.exec.planner.sql.handlers.SqlHandlerConfig;

import java.util.List;

public class SqlDeleteFunction extends DrillSqlCall {

  private final SqlNode jarName;

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("DELETE_FUNCTION", SqlKind.OTHER) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      return new SqlDeleteFunction(pos, operands[0]);
    }
  };

  public SqlDeleteFunction(SqlParserPos pos, SqlNode jarName) {
    super(pos);
    this.jarName = jarName;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> opList = Lists.newArrayList();
    opList.add(jarName);
    return opList;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("DELETE");
    writer.keyword("FUNCTION");
    writer.keyword("BY");
    writer.keyword("JAR");
    jarName.unparse(writer, leftPrec, rightPrec);
  }

  @Override
  public AbstractSqlHandler getSqlHandler(SqlHandlerConfig config) {
    return new DeleteFunctionHandler(config);
  }

  public SqlNode getJarName() { return jarName; }
}
