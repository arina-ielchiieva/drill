/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.work.prepare;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Date;
import java.util.Collections;
import java.util.List;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType;
import org.apache.drill.exec.proto.UserProtos.ColumnSearchability;
import org.apache.drill.exec.proto.UserProtos.ColumnUpdatability;
import org.apache.drill.exec.proto.UserProtos.CreatePreparedStatementResp;
import org.apache.drill.exec.proto.UserProtos.PreparedStatement;
import org.apache.drill.exec.proto.UserProtos.RequestStatus;
import org.apache.drill.exec.proto.UserProtos.ResultColumnMetadata;
import org.apache.drill.exec.store.ischema.InfoSchemaConstants;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/**
 * Tests for creating and executing prepared statements.
 */
public class TestPreparedStatementProvider extends BaseTestQuery {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestPreparedStatementProvider.class);

  /**
   * Simple query.
   * @throws Exception
   */
  @Test
  public void simple() throws Exception {
    String query = "SELECT * FROM cp.`region.json` ORDER BY region_id LIMIT 1";
    PreparedStatement preparedStatement = createPrepareStmt(query, false, null);

    List<ExpectedColumnResult> expMetadata = ImmutableList.of(
        new ExpectedColumnResult("region_id", "BIGINT", true, 20, 0, 0, true, Long.class.getName()),
        new ExpectedColumnResult("sales_city", "CHARACTER VARYING", true, 65536, 65536, 0, false, String.class.getName()),
        new ExpectedColumnResult("sales_state_province", "CHARACTER VARYING", true, 65536, 65536, 0, false, String.class.getName()),
        new ExpectedColumnResult("sales_district", "CHARACTER VARYING", true, 65536, 65536, 0, false, String.class.getName()),
        new ExpectedColumnResult("sales_region", "CHARACTER VARYING", true, 65536, 65536, 0, false, String.class.getName()),
        new ExpectedColumnResult("sales_country", "CHARACTER VARYING", true, 65536, 65536, 0, false, String.class.getName()),
        new ExpectedColumnResult("sales_district_id", "BIGINT", true, 20, 0, 0, true, Long.class.getName())
    );

    verifyMetadata(expMetadata, preparedStatement.getColumnsList());

    testBuilder()
        .unOrdered()
        .preparedStatement(preparedStatement.getServerHandle())
        .baselineColumns("region_id", "sales_city", "sales_state_province", "sales_district",
            "sales_region", "sales_country", "sales_district_id")
        .baselineValues(0L, "None", "None", "No District", "No Region", "No Country", 0L)
        .go();
  }

  /**
   * Create a prepared statement for a query that has GROUP BY clause in it
   */
  @Test
  public void groupByQuery() throws Exception {
    String query = "SELECT sales_city, count(*) as cnt FROM cp.`region.json` " +
        "GROUP BY sales_city ORDER BY sales_city DESC LIMIT 1";
    PreparedStatement preparedStatement = createPrepareStmt(query, false, null);

    List<ExpectedColumnResult> expMetadata = ImmutableList.of(
        new ExpectedColumnResult("sales_city", "CHARACTER VARYING", true, 65536, 65536, 0, false, String.class.getName()),
        new ExpectedColumnResult("cnt", "BIGINT", false, 20, 0, 0, true, Long.class.getName())
    );

    verifyMetadata(expMetadata, preparedStatement.getColumnsList());

    testBuilder()
        .unOrdered()
        .preparedStatement(preparedStatement.getServerHandle())
        .baselineColumns("sales_city", "cnt")
        .baselineValues("Yakima", 1L)
        .go();
  }

  /**
   * Create a prepared statement for a query that joins two tables and has ORDER BY clause.
   */
  @Test
  public void joinOrderByQuery() throws Exception {
    String query = "SELECT l.l_quantity, l.l_shipdate, o.o_custkey FROM cp.`tpch/lineitem.parquet` l JOIN cp.`tpch/orders.parquet` o " +
        "ON l.l_orderkey = o.o_orderkey LIMIT 2";

    PreparedStatement preparedStatement = createPrepareStmt(query, false, null);

    List<ExpectedColumnResult> expMetadata = ImmutableList.of(
        new ExpectedColumnResult("l_quantity", "DOUBLE", false, 24, 0, 0, true, Double.class.getName()),
        new ExpectedColumnResult("l_shipdate", "DATE", false, 10, 0, 0, false, Date.class.getName()),
        new ExpectedColumnResult("o_custkey", "INTEGER", false, 11, 0, 0, true, Integer.class.getName())
    );

    verifyMetadata(expMetadata, preparedStatement.getColumnsList());
  }

  /**
   * Pass an invalid query to the create prepare statement request and expect a parser failure.
   * @throws Exception
   */
  @Test
  public void invalidQueryParserError() throws Exception {
    createPrepareStmt("BLAH BLAH", true, ErrorType.PARSE);
  }

  /**
   * Pass an invalid query to the create prepare statement request and expect a validation failure.
   * @throws Exception
   */
  @Test
  public void invalidQueryValidationError() throws Exception {
    createPrepareStmt("SELECT * sdflkgdh", true, ErrorType.PARSE /** Drill returns incorrect error for parse error*/);
  }

  @Test
  public void queryWithVarLenCasts() throws Exception {
    String query = "select\n" +
        "cast(col_int as varchar(30)) as col_int,\n" +
        "cast(col_vrchr as varchar(31)) as col_vrchr,\n" +
        "cast(col_dt as varchar(32)) as col_dt,\n" +
        "cast(col_tim as varchar(33)) as col_tim,\n" +
        "cast(col_tmstmp as varchar(34)) as col_tmstmp,\n" +
        "cast(col_flt as varchar(35)) as col_flt,\n" +
        "cast(col_intrvl_yr as varchar(36)) as col_intrvl_yr,\n" +
        "cast(col_bln as varchar(37)) as col_bln\n" +
        "from cp.`parquet/alltypes_optional.parquet` limit 0";

    PreparedStatement preparedStatement = createPrepareStmt(query, false, null);

    List<ExpectedColumnResult> expMetadata = ImmutableList.of(
        new ExpectedColumnResult("col_int", "CHARACTER VARYING", true, 30, 30, 0, false, String.class.getName()),
        new ExpectedColumnResult("col_vrchr", "CHARACTER VARYING", true, 31, 31, 0, false, String.class.getName()),
        new ExpectedColumnResult("col_dt", "CHARACTER VARYING", true, 32, 32, 0, false, String.class.getName()),
        new ExpectedColumnResult("col_tim", "CHARACTER VARYING", true, 33, 33, 0, false, String.class.getName()),
        new ExpectedColumnResult("col_tmstmp", "CHARACTER VARYING", true, 34, 34, 0, false, String.class.getName()),
        new ExpectedColumnResult("col_flt", "CHARACTER VARYING", true, 35, 35, 0, false, String.class.getName()),
        new ExpectedColumnResult("col_intrvl_yr", "CHARACTER VARYING", true, 36, 36, 0, false, String.class.getName()),
        new ExpectedColumnResult("col_bln", "CHARACTER VARYING", true, 37, 37, 0, false, String.class.getName())
    );

    verifyMetadata(expMetadata, preparedStatement.getColumnsList());
  }

  @Test
  public void queryWithConstant() throws Exception {
    String query = "select\n" +
        "'aaa' as col_a,\n" +
        "10 as col_i\n," +
        "cast('aaa' as varchar(5)) as col_a_short,\n" +
        "cast(10 as varchar(5)) as col_i_short,\n" +
        "cast('aaaaaaaaaaaaa' as varchar(5)) as col_a_long,\n" +
        "cast(1000000000 as varchar(5)) as col_i_long\n" +
        "from (values(1))";

    List<ExpectedColumnResult> expMetadata = ImmutableList.of(
        new ExpectedColumnResult("col_a", "CHARACTER VARYING", false, 3, 3, 0, false, String.class.getName()),
        new ExpectedColumnResult("col_i", "INTEGER", false, 11, 0, 0, true, Integer.class.getName()),
        new ExpectedColumnResult("col_a_short", "CHARACTER VARYING", false, 5, 5, 0, false, String.class.getName()),
        new ExpectedColumnResult("col_i_short", "CHARACTER VARYING", false, 5, 5, 0, false, String.class.getName()),
        new ExpectedColumnResult("col_a_long", "CHARACTER VARYING", false, 5, 5, 0, false, String.class.getName()),
        new ExpectedColumnResult("col_i_long", "CHARACTER VARYING", false, 5, 5, 0, false, String.class.getName())
    );

    PreparedStatement preparedStatement = createPrepareStmt(query, false, null);

    verifyMetadata(expMetadata, preparedStatement.getColumnsList());
  }

  @Test
  public void unknownPrecision() throws Exception {
    String query = "SELECT sales_city FROM cp.`region.json` ORDER BY region_id LIMIT 0";
    PreparedStatement preparedStatement = createPrepareStmt(query, false, null);
    System.out.println(preparedStatement.getColumnsList());
    test("alter session set `planner.enable_limit0_optimization` = true");

    preparedStatement = createPrepareStmt(query, false, null);
    System.out.println(preparedStatement.getColumnsList());
  }

  @Test
  public void queryWithVarLenCastForDecimal() throws Exception {
    try {
      test("alter session set `planner.enable_decimal_data_type` = true");
      String query = "select cast(commission_pct as varchar(50)) as commission_pct from cp.`parquet/fixedlenDecimal.parquet` limit 1";
      PreparedStatement preparedStatement = createPrepareStmt(query, false, null);
      List<ExpectedColumnResult> expMetadata = ImmutableList.of(
          new ExpectedColumnResult("commission_pct", "CHARACTER VARYING", true, 50, 50, 0, false, String.class.getName()));
      verifyMetadata(expMetadata, preparedStatement.getColumnsList());
    } finally {
      test("alter session reset `planner.enable_decimal_data_type`");
    }
  }

  /* Helper method which creates a prepared statement for given query. */
  private static PreparedStatement createPrepareStmt(String query, boolean expectFailure, ErrorType errorType) throws Exception {
    CreatePreparedStatementResp resp = client.createPreparedStatement(query).get();

    assertEquals(expectFailure ? RequestStatus.FAILED : RequestStatus.OK, resp.getStatus());

    if (expectFailure) {
      assertEquals(errorType, resp.getError().getErrorType());
    } else {
      logger.error("Prepared statement creation failed: {}", resp.getError().getMessage());
    }

    return resp.getPreparedStatement();
  }

  private static class ExpectedColumnResult {
    final String columnName;
    final String type;
    final boolean nullable;
    final int displaySize;
    final int precision;
    final int scale;
    final boolean signed;
    final String className;

    ExpectedColumnResult(String columnName, String type, boolean nullable, int displaySize, int precision, int scale,
        boolean signed, String className) {
      this.columnName = columnName;
      this.type = type;
      this.nullable = nullable;
      this.displaySize = displaySize;
      this.precision = precision;
      this.scale = scale;
      this.signed = signed;
      this.className = className;
    }

    boolean isEqualsTo(ResultColumnMetadata result) {
      return
          result.getCatalogName().equals(InfoSchemaConstants.IS_CATALOG_NAME) &&
          result.getSchemaName().isEmpty() &&
          result.getTableName().isEmpty() &&
          result.getColumnName().equals(columnName) &&
          result.getLabel().equals(columnName) &&
          result.getDataType().equals(type) &&
          result.getIsNullable() == nullable &&
          result.getPrecision() == precision &&
          result.getScale() == scale &&
          result.getSigned() == signed &&
          result.getDisplaySize() == displaySize &&
          result.getClassName().equals(className) &&
          result.getSearchability() == ColumnSearchability.ALL &&
          result.getAutoIncrement() == false &&
          result.getCaseSensitivity() == false &&
          result.getUpdatability() == ColumnUpdatability.READ_ONLY &&
          result.getIsAliased() == true &&
          result.getIsCurrency() == false;
    }

    @Override
    public String toString() {
      return "ExpectedColumnResult[" +
          "columnName='" + columnName + '\'' +
          ", type='" + type + '\'' +
          ", nullable=" + nullable +
          ", displaySize=" + displaySize +
          ", precision=" + precision +
          ", scale=" + scale +
          ", signed=" + signed +
          ", className='" + className + '\'' +
          ']';
    }
  }

  private static String toString(ResultColumnMetadata metadata) {
    return "ResultColumnMetadata[" +
        "columnName='" + metadata.getColumnName() + '\'' +
        ", type='" + metadata.getDataType() + '\'' +
        ", nullable=" + metadata.getIsNullable() +
        ", displaySize=" + metadata.getDisplaySize() +
        ", precision=" + metadata.getPrecision() +
        ", scale=" + metadata.getScale() +
        ", signed=" + metadata.getSigned() +
        ", className='" + metadata.getClassName() + '\'' +
        ']';
  }

  private static void verifyMetadata(List<ExpectedColumnResult> expMetadata,
      List<ResultColumnMetadata> actMetadata) {
    assertEquals(expMetadata.size(), actMetadata.size());

    int i = 0;
    for(ExpectedColumnResult exp : expMetadata) {
      ResultColumnMetadata act = actMetadata.get(i++);

      assertTrue("Failed to find the expected column metadata: " + exp + ". Was: " + toString(act), exp.isEqualsTo(act));
    }
  }
}
