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
import java.util.List;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.types.Types;
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
        new ExpectedColumnResult("sales_city", "CHARACTER VARYING", true, Types.MAX_VARCHAR_LENGTH, Types.MAX_VARCHAR_LENGTH, 0, false, String.class.getName()),
        new ExpectedColumnResult("sales_state_province", "CHARACTER VARYING", true, Types.MAX_VARCHAR_LENGTH, Types.MAX_VARCHAR_LENGTH, 0, false, String.class.getName()),
        new ExpectedColumnResult("sales_district", "CHARACTER VARYING", true, Types.MAX_VARCHAR_LENGTH, Types.MAX_VARCHAR_LENGTH, 0, false, String.class.getName()),
        new ExpectedColumnResult("sales_region", "CHARACTER VARYING", true, Types.MAX_VARCHAR_LENGTH, Types.MAX_VARCHAR_LENGTH, 0, false, String.class.getName()),
        new ExpectedColumnResult("sales_country", "CHARACTER VARYING", true, Types.MAX_VARCHAR_LENGTH, Types.MAX_VARCHAR_LENGTH, 0, false, String.class.getName()),
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
        new ExpectedColumnResult("sales_city", "CHARACTER VARYING", true, Types.MAX_VARCHAR_LENGTH, Types.MAX_VARCHAR_LENGTH, 0, false, String.class.getName()),
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
  public void queryWithScalarStringCasts() throws Exception {
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
  public void queryWithConstants() throws Exception {
    String query = "select\n" +
        "'aaa' as col_a,\n" +
        "10 as col_i\n," +
        "cast(null as varchar(5)) as col_n\n," +
        "cast('aaa' as varchar(5)) as col_a_short,\n" +
        "cast(10 as varchar(5)) as col_i_short,\n" +
        "cast('aaaaaaaaaaaaa' as varchar(5)) as col_a_long,\n" +
        "cast(1000000000 as varchar(5)) as col_i_long\n" +
        "from (values(1))";

    List<ExpectedColumnResult> expMetadata = ImmutableList.of(
        new ExpectedColumnResult("col_a", "CHARACTER VARYING", false, 3, 3, 0, false, String.class.getName()),
        new ExpectedColumnResult("col_i", "INTEGER", false, 11, 0, 0, true, Integer.class.getName()),
        new ExpectedColumnResult("col_n", "CHARACTER VARYING", true, 5, 5, 0, false, String.class.getName()),
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
    // both return 65536
    String query = "SELECT sales_city FROM cp.`region.json` ORDER BY region_id LIMIT 0";
    PreparedStatement preparedStatement = createPrepareStmt(query, false, null);
    System.out.println(preparedStatement.getColumnsList());
    test("alter session set `planner.enable_limit0_optimization` = true");

    preparedStatement = createPrepareStmt(query, false, null);
    System.out.println(preparedStatement.getColumnsList());
  }

  //@Test //todo case should be fixed
  public void differencesBetweenLimitAndRegular() throws Exception {
    test("use dfs_test.tmp");
    test("create view voter_csv_v as select " +
        "case when columns[0]='' then cast(null as integer) else cast(columns[0] as integer) end as voter_id, " +
        "case when columns[3]='' then cast(null as char(20)) else cast(columns[3] as char(20)) end as registration " +
        "from dfs.`F:\\git_repo\\drill-test-framework\\framework\\resources\\Datasources\\limit0\\p1tests\\voter.csv`");

    String query = "SELECT " +
        //"concat(registration, '_D'), " +
        "concat(voter_id, '_D') FROM voter_csv_v limit 0";
    //String query = "SELECT * FROM voter_csv_v where voter_id=10 limit 0";
    //PreparedStatement preparedStatement1 = createPrepareStmt(query, false, null);
    //System.out.println(preparedStatement1.getColumnsList());
    test("alter session set `planner.enable_limit0_optimization` = true");
    PreparedStatement preparedStatement2 = createPrepareStmt(query, false, null);
    System.out.println(preparedStatement2.getColumnsList());
    //assertEquals(preparedStatement1.getColumnsList(), preparedStatement2.getColumnsList());
  }

  //@Test //todo create test cover four functions: lead, lag, first_value, last_value
  public void leadAndLeadFunctions() throws Exception {
    test("use dfs_test.tmp");

    test("create or replace view tblWnulls_v as select cast(c1 as integer) c1, "
        + "cast(c2 as char(1)) c2 "
        + "from dfs.`F:\\git_repo\\drill-test-framework\\framework\\resources\\Datasources\\window_functions\\tblWnulls.parquet`");

    test("alter session set `planner.enable_decimal_data_type` = true");

    //String query = "SELECT LEAD(c2) OVER ( PARTITION BY c2 ORDER BY c1) LEAD_c2 FROM `tblWnulls_v` limit 0";
    String query = "select * from (SELECT * FROM (SELECT c1, c2, LEAD(c2) OVER ( PARTITION BY c2 ORDER BY c1) LEAD_c2, " +
        "NTILE(3) OVER ( PARTITION BY c2 ORDER BY c1) tile " +
        "FROM `tblWnulls_v`) sub_query WHERE LEAD_c2 ='e' ORDER BY tile, c1) t limit 0";

    PreparedStatement preparedStatement = createPrepareStmt(query, false, null);
    System.out.println(preparedStatement.getColumnsList());
  }

  @Test
  public void complexInput() throws Exception {
    // both give 65536
    // 10 is casted to bigint, thus we can't determine cast length
    String query = "SELECT castVarchar(sales_city, 10) as c FROM cp.`region.json` ORDER BY region_id LIMIT 0";
    PreparedStatement preparedStatement = createPrepareStmt(query, false, null);
    System.out.println(preparedStatement.getColumnsList());

    test("alter session set `planner.enable_limit0_optimization` = true");
    preparedStatement = createPrepareStmt(query, false, null);
    System.out.println(preparedStatement.getColumnsList());
  }

  @Test
  //todo the same cases for concat operator
  public void concat() throws Exception {
    String query = "select\n" +
        "concat(cast(sales_city as varchar(10)), cast(sales_city as varchar(10))) as two_casts,\n" +
        "concat(cast(sales_city as varchar(60000)), cast(sales_city as varchar(60000))) as max_length,\n" +
        "concat(cast(sales_city as varchar(10)), sales_city) as one_unknown,\n" +
        "concat(sales_city, sales_city) as two_unknown,\n" +
        "concat(cast(sales_city as varchar(10)), 'a') as one_constant,\n" +
        "concat('a', 'a') as two_constants," +
        "concat(cast(sales_city as varchar(10)), cast(null as varchar(10))) as right_null,\n" +
        "concat(cast(null as varchar(10)), cast(sales_city as varchar(10))) as left_null,\n" +
        "concat(cast(null as varchar(10)), cast(null as varchar(10))) as both_null\n" +
        "from cp.`region.json` limit 0";

    List<ExpectedColumnResult> expMetadata = ImmutableList.of(
        new ExpectedColumnResult("two_casts", "CHARACTER VARYING", false, 20, 20, 0, false, String.class.getName()),
        new ExpectedColumnResult("max_length", "CHARACTER VARYING", false, 120000, 120000, 0, false, String.class.getName()),
        new ExpectedColumnResult("one_unknown", "CHARACTER VARYING", false, Types.MAX_VARCHAR_LENGTH, Types.MAX_VARCHAR_LENGTH, 0, false, String.class.getName()),
        new ExpectedColumnResult("two_unknown", "CHARACTER VARYING", false, Types.MAX_VARCHAR_LENGTH, Types.MAX_VARCHAR_LENGTH, 0, false, String.class.getName()),
        new ExpectedColumnResult("one_constant", "CHARACTER VARYING", false, 11, 11, 0, false, String.class.getName()),
        new ExpectedColumnResult("two_constants", "CHARACTER VARYING", false, 2, 2, 0, false, String.class.getName()),
        new ExpectedColumnResult("right_null", "CHARACTER VARYING", false, 20, 20, 0, false, String.class.getName()),
        new ExpectedColumnResult("left_null", "CHARACTER VARYING", false, 20, 20, 0, false, String.class.getName()),
        new ExpectedColumnResult("both_null", "CHARACTER VARYING", false, 20, 20, 0, false, String.class.getName())
    );

    verifyMetadata(expMetadata, createPrepareStmt(query, false, null).getColumnsList());

    try {
      test("alter session set `planner.enable_limit0_optimization` = true");
      verifyMetadata(expMetadata, createPrepareStmt(query, false, null).getColumnsList());
    } finally {
      test("alter session reset `planner.enable_limit0_optimization`");
    }
  }

  @Test
  public void concat2() throws Exception {
    //String query = "select lname || mi from cp.`customer.json` limit 0"; // both correct when fixed in TypeInferenceUtils
    //String query = "select concat('A', '') from (values(1)) limit 0";
    //String query = "select concat('A', cast(null as varchar(10))) from (values(1)) limit 0";
    String query = "select cast('A' as varchar(10)) || cast(null as varchar(10)) from (values(1)) limit 0";
    System.out.println(createPrepareStmt(query, false, null).getColumnsList());
    test("alter session set `planner.enable_limit0_optimization` = true");
    System.out.println("LIMIT 0");
    System.out.println(createPrepareStmt(query, false, null).getColumnsList());
  }

  @Test
  public void substring() throws Exception {
    String query = "select\n" +
        "substring(sales_city, 1, 2) as unk,\n" + // 2
        "substring(cast(sales_city as varchar(10)), 1, 2) as unk1,\n" + // 2
        "substring(cast(sales_city as varchar(1)), 1, 2) as unk2,\n" + // 1
        "substring(cast(sales_city as varchar(2)), 1, 2) as unk3,\n" + // 2
        "substring(cast(sales_city as varchar(10)), -1, 2) as unk4,\n" + // 0
        "substring(cast(sales_city as varchar(10)), 1, -2) as unk5,\n" + // 0
        "substring(cast(sales_city as varchar(10)), 11, 2) as unk6,\n" + // 0
        "substring(cast(sales_city as varchar(10)), 5, 10) as unk7,\n" + // 6
        "substring(cast(sales_city as varchar(10)), 5) as unk8,\n" + // 6
        "substring(sales_city, 5) as unk9,\n" + // 65536
        "substring(cast(sales_city as varchar(10)), 10) as unk10,\n" + // 1
        "substring(cast(sales_city as varchar(10)), 11) as unk11,\n" + // 0
        "substring(sales_city, -1) as unk12,\n" + // 0
        "substring(sales_city, -1, 2) as unk13,\n" + // 0
        "substring(sales_city, 1, -2) as unk14,\n" + // 0
        "substring(sales_city, '^N') as unk15,\n" + // 65536
        "substring(sales_city, 0, 2) as unk16,\n" + // 0
        "substring(sales_city, 1, 0) as unk17,\n" + // 0
        "substring(sales_city, 0) as unk18,\n" + // 0
        "substring(cast(sales_city as varchar(10)), '^N') as unk19\n" + // 10
        "from cp.`region.json` limit 0";

    //test("alter session set `planner.enable_limit0_optimization` = true");
    List<ResultColumnMetadata> columnsList = createPrepareStmt(query, false, null).getColumnsList();
    for (ResultColumnMetadata resultColumnMetadata : columnsList) {
      System.out.println(resultColumnMetadata.getColumnName() + " - " + resultColumnMetadata.getPrecision());
    }
    //System.out.println(columnsList);
  }

  @Test
  public void testEachSubstring() throws Exception {
    String query = "select\n" +
/*        "substring(sales_city, 1, 2) as unk,\n" + // 2
        "substring(cast(sales_city as varchar(10)), 1, 2) as unk1,\n" + // 2
        "substring(cast(sales_city as varchar(1)), 1, 2) as unk2,\n" + // 1
        "substring(cast(sales_city as varchar(2)), 1, 2) as unk3,\n" + // 2
        "substring(cast(sales_city as varchar(10)), -1, 2) as unk4,\n" + // 0
        "substring(cast(sales_city as varchar(10)), 1, -2) as unk5,\n" + // 0
        "substring(cast(sales_city as varchar(10)), 11, 2) as unk6,\n" + // 0
        "substring(cast(sales_city as varchar(10)), 5, 10) as unk7,\n" + // 6
        "substring(cast(sales_city as varchar(10)), 5) as unk8,\n" + // 6
        "substring(sales_city, 5) as unk9,\n" + // 65536
        "substring(cast(sales_city as varchar(10)), 10) as unk10,\n" + // 1
        "substring(cast(sales_city as varchar(10)), 11) as unk11,\n" + // 0
        "substring(sales_city, -1) as unk12,\n" + // 0
        "substring(sales_city, -1, 2) as unk13,\n" + // 0
        "substring(sales_city, 1, -2) as unk14,\n" + // 0
        "substring(sales_city, '^N') as unk15,\n" + // 65536
        "substring(sales_city, 0, 2) as unk16,\n" + // 0
        "substring(sales_city, 1, 0) as unk17,\n" + // 0*/
        "substring(sales_city, 0) as unk18\n" + // 0
        "from cp.`region.json` limit 0";

    //test("alter session set `planner.enable_limit0_optimization` = true");
    List<ResultColumnMetadata> columnsList = createPrepareStmt(query, false, null).getColumnsList();
    for (ResultColumnMetadata resultColumnMetadata : columnsList) {
      System.out.println(resultColumnMetadata.getColumnName() + " - " + resultColumnMetadata.getPrecision());
    }
  }

  @Test
  public void pad() throws Exception {
    // + lpad
    String query = "SELECT\n" +
        "rpad(cast(sales_city as varchar(10)), 10, 'A') as c,\n" +
        "rpad(cast(sales_city as varchar(10)), 0, 'A') as c1,\n" +
        "rpad(cast(sales_city as varchar(10)), -1, 'A') as c2,\n" +
        "rpad(cast(sales_city as varchar(10)), 9, 'A') as c3,\n" +
        "rpad(sales_city, 10, 'A') as c4\n" + // works without initial precision :)
        "FROM cp.`region.json` LIMIT 0";
    PreparedStatement preparedStatement = createPrepareStmt(query, false, null);
    System.out.println(preparedStatement.getColumnsList());


/*    test("alter session set `planner.enable_limit0_optimization` = true");
    preparedStatement = createPrepareStmt(query, false, null);
    System.out.println(preparedStatement.getColumnsList());*/
  }

  @Test
  public void coalesce() throws Exception {
    String query = "SELECT coalesce(cast(sales_city as varchar(10)), 'zzzzz') as c FROM cp.`region.json` LIMIT 0";
    PreparedStatement preparedStatement = createPrepareStmt(query, false, null);
    System.out.println(preparedStatement.getColumnsList());


/*    test("alter session set `planner.enable_limit0_optimization` = true");
    preparedStatement = createPrepareStmt(query, false, null);
    System.out.println(preparedStatement.getColumnsList());*/
  }

  @Test
  public void ifExpression() throws Exception {
    String query = "select\n" +
        "case when sales_state_province = 'CA' then 'a' when sales_state_province = 'DB' then 'aa' else 'aaa' end as col_123,\n" +
        "case when sales_state_province = 'CA' then 'aa' when sales_state_province = 'DB' then 'a' else 'aaa' end as col_213,\n" +
        "case when sales_state_province = 'CA' then 'a' when sales_state_province = 'DB' then 'aaa' else 'aa' end as col_132,\n" +
        "case when sales_state_province = 'CA' then 'aa' when sales_state_province = 'DB' then 'aaa' else 'a' end as col_231,\n" +
        "case when sales_state_province = 'CA' then 'aaa' when sales_state_province = 'DB' then 'aa' else 'a' end as col_321,\n" +
        "case when sales_state_province = 'CA' then 'aaa' when sales_state_province = 'DB' then 'a' else 'aa' end as col_312,\n" +
        "case when sales_state_province = 'CA' then sales_state_province when sales_state_province = 'DB' then 'a' else 'aa' end as col_unk1,\n" +
        "case when sales_state_province = 'CA' then 'aaa' when sales_state_province = 'DB' then sales_state_province else 'aa' end as col_unk2,\n" +
        "case when sales_state_province = 'CA' then 'aaa' when sales_state_province = 'DB' then 'a' else sales_state_province end as col_unk3\n" +
        "from cp.`region.json` limit 0";

    List<ExpectedColumnResult> expMetadata = ImmutableList.of(
        new ExpectedColumnResult("col_123", "CHARACTER VARYING", false, 3, 3, 0, false, String.class.getName()),
        new ExpectedColumnResult("col_213", "CHARACTER VARYING", false, 3, 3, 0, false, String.class.getName()),
        new ExpectedColumnResult("col_132", "CHARACTER VARYING", false, 3, 3, 0, false, String.class.getName()),
        new ExpectedColumnResult("col_231", "CHARACTER VARYING", false, 3, 3, 0, false, String.class.getName()),
        new ExpectedColumnResult("col_321", "CHARACTER VARYING", false, 3, 3, 0, false, String.class.getName()),
        new ExpectedColumnResult("col_312", "CHARACTER VARYING", false, 3, 3, 0, false, String.class.getName()),
        new ExpectedColumnResult("col_unk1", "CHARACTER VARYING", true, Types.MAX_VARCHAR_LENGTH, Types.MAX_VARCHAR_LENGTH, 0, false, String.class.getName()),
        new ExpectedColumnResult("col_unk2", "CHARACTER VARYING", true, Types.MAX_VARCHAR_LENGTH, Types.MAX_VARCHAR_LENGTH, 0, false, String.class.getName()),
        new ExpectedColumnResult("col_unk3", "CHARACTER VARYING", true, Types.MAX_VARCHAR_LENGTH, Types.MAX_VARCHAR_LENGTH, 0, false, String.class.getName())
    );

    verifyMetadata(expMetadata, createPrepareStmt(query, false, null).getColumnsList());

    try {
      test("alter session set `planner.enable_limit0_optimization` = true");
      verifyMetadata(expMetadata, createPrepareStmt(query, false, null).getColumnsList());
    } finally {
      test("alter session reset `planner.enable_limit0_optimization`");
    }
  }

  //todo test queries with limit 0 and without and then with enabled limit 0 optimization
  @Test
  public void functionsWithTheSameScalarStringSize() throws Exception {
    String query = "select\n" +
        "lower(sales_city) as lower_col,\n" +
        "upper(sales_city) as upper_col,\n" +
        "initcap(sales_city) as initcap_col,\n" +
        "reverse(sales_city) as reverse_col,\n" +
        "lower(cast(sales_city as varchar(30))) as lower_cast_col,\n" +
        "upper(cast(sales_city as varchar(30))) as upper_cast_col,\n" +
        "initcap(cast(sales_city as varchar(30))) as initcap_cast_col,\n" +
        "reverse(cast(sales_city as varchar(30))) as reverse_cast_col\n" +
        "from cp.`region.json` limit 0";

    List<ExpectedColumnResult> expMetadata = ImmutableList.of(
        new ExpectedColumnResult("lower_col", "CHARACTER VARYING", true, Types.MAX_VARCHAR_LENGTH, Types.MAX_VARCHAR_LENGTH, 0, false, String.class.getName()),
        new ExpectedColumnResult("upper_col", "CHARACTER VARYING", true, Types.MAX_VARCHAR_LENGTH, Types.MAX_VARCHAR_LENGTH, 0, false, String.class.getName()),
        new ExpectedColumnResult("initcap_col", "CHARACTER VARYING", true, Types.MAX_VARCHAR_LENGTH, Types.MAX_VARCHAR_LENGTH, 0, false, String.class.getName()),
        new ExpectedColumnResult("reverse_col", "CHARACTER VARYING", true, Types.MAX_VARCHAR_LENGTH, Types.MAX_VARCHAR_LENGTH, 0, false, String.class.getName()),
        new ExpectedColumnResult("lower_cast_col", "CHARACTER VARYING", true, 30, 30, 0, false, String.class.getName()),
        new ExpectedColumnResult("upper_cast_col", "CHARACTER VARYING", true, 30, 30, 0, false, String.class.getName()),
        new ExpectedColumnResult("initcap_cast_col", "CHARACTER VARYING", true, 30, 30, 0, false, String.class.getName()),
        new ExpectedColumnResult("reverse_cast_col", "CHARACTER VARYING", true, 30, 30, 0, false, String.class.getName())
    );

    verifyMetadata(expMetadata, createPrepareStmt(query, false, null).getColumnsList());

    try {
      test("alter session set `planner.enable_limit0_optimization` = true");
      verifyMetadata(expMetadata, createPrepareStmt(query, false, null).getColumnsList());
    } finally {
      test("alter session reset `planner.enable_limit0_optimization`");
    }
  }

  @Test
  public void queryWithScalarStringCastForDecimal() throws Exception {
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

  @Test
  public void union() throws Exception {
    String query = "select * from\n" +
        "(select cast('AAA' as varchar(3)) as ee from (values(1))\n" +
        "union\n" +
        "select cast('AAA' as varchar(5)) as ex from (values(1))\n" +
        ")";
    System.out.println(createPrepareStmt(query, false, null).getColumnsList());

    //todo also for limit 0 optimization
/*    System.out.println("-------------LIMIT 0 OPTIMIZATION-------------");
    try {
      test("alter session set `planner.enable_limit0_optimization` = true");
      System.out.println(createPrepareStmt(query, false, null).getColumnsList());
    } finally {
      test("alter session reset `planner.enable_limit0_optimization`");
    }*/
  }

  @Test
  public void requireOptionalUnion() throws Exception {
    test("use dfs_test.tmp");
    test("create or replace view optional_type_v as select cast(c_varchar as varchar(100))\t\tas c_varchar from\n" +
        "dfs.`F:\\git_repo\\drill-test-framework\\framework\\resources\\Datasources\\subqueries\\j1`");
    test("create or replace view required_type_v as select cast(c_varchar as varchar(100))\t\tas c_varchar from\n" +
        "dfs.`F:\\git_repo\\drill-test-framework\\framework\\resources\\Datasources\\subqueries\\j3`");

    String query = "select c_varchar from optional_type_v union all select c_varchar from required_type_v";
    System.out.println(createPrepareStmt(query, false, null).getColumnsList());

    System.out.println("-------------LIMIT 0 OPTIMIZATION-------------");
    try {
      test("alter session set `planner.enable_limit0_optimization` = true");
      System.out.println(createPrepareStmt(query, false, null).getColumnsList());
    } finally {
      test("alter session reset `planner.enable_limit0_optimization`");
    }
  }

  @Test
  public void varbinary() throws Exception {
    String query = "select cast(`varbinary_col` AS varbinary(65000))\n" +
        "from cp.`/parquet/alltypes.json`";

    System.out.println(createPrepareStmt(query, false, null).getColumnsList());

        System.out.println("-------------LIMIT 0 OPTIMIZATION-------------");
    try {
      test("alter session set `planner.enable_limit0_optimization` = true");
      System.out.println(createPrepareStmt(query, false, null).getColumnsList());
    } finally {
      test("alter session reset `planner.enable_limit0_optimization`");
    }
  }


  /* Helper method which creates a prepared statement for given query. */
  private static PreparedStatement createPrepareStmt(String query, boolean expectFailure, ErrorType errorType) throws Exception {
    CreatePreparedStatementResp resp = client.createPreparedStatement(query).get();

    if (resp.hasError()) {
      System.out.println(resp.getError().getMessage());
    }

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
