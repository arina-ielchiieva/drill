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
package org.apache.drill;

import static org.apache.drill.test.TestBuilder.listOf;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.nio.file.Paths;

import org.apache.drill.categories.OperatorTest;
import org.apache.drill.categories.PlannerTest;
import org.apache.drill.categories.SqlFunctionTest;
import org.apache.drill.categories.UnlikelyTest;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.util.DrillFileUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.test.BaseTestQuery;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.QueryBuilder;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SqlFunctionTest.class, OperatorTest.class, PlannerTest.class})
public class TestExampleQueries extends BaseTestQuery {
  @BeforeClass
  public static void setupTestFiles() {
    dirTestWatcher.copyResourceToRoot(Paths.get("tpchmulti"));
    dirTestWatcher.copyResourceToRoot(Paths.get("store","text"));
  }

  @Test // see DRILL-2328
  @Category(UnlikelyTest.class)
  public void testConcatOnNull() throws Exception {
    try {
      test("use dfs.tmp");
      test("create view concatNull as (select * from cp.`customer.json` where customer_id < 5);");

      // Test Left Null
      testBuilder()
          .sqlQuery("select (mi || lname) as CONCATOperator, mi, lname, concat(mi, lname) as CONCAT from concatNull")
          .ordered()
          .baselineColumns("CONCATOperator", "mi", "lname", "CONCAT")
          .baselineValues("A.Nowmer", "A.", "Nowmer", "A.Nowmer")
          .baselineValues("I.Whelply", "I.", "Whelply", "I.Whelply")
          .baselineValues(null, null, "Derry", "Derry")
          .baselineValues("J.Spence", "J.", "Spence", "J.Spence")
          .build().run();

      // Test Right Null
      testBuilder()
          .sqlQuery("select (lname || mi) as CONCATOperator, lname, mi, concat(lname, mi) as CONCAT from concatNull")
          .ordered()
          .baselineColumns("CONCATOperator", "lname", "mi", "CONCAT")
          .baselineValues("NowmerA.", "Nowmer", "A.", "NowmerA.")
          .baselineValues("WhelplyI.", "Whelply", "I.", "WhelplyI.")
          .baselineValues(null, "Derry", null, "Derry")
          .baselineValues("SpenceJ.", "Spence", "J.", "SpenceJ.")
          .build().run();

      // Test Two Sides
      testBuilder()
          .sqlQuery("select (mi || mi) as CONCATOperator, mi, mi, concat(mi, mi) as CONCAT from concatNull")
          .ordered()
          .baselineColumns("CONCATOperator", "mi", "mi0", "CONCAT")
          .baselineValues("A.A.", "A.", "A.", "A.A.")
          .baselineValues("I.I.", "I.", "I.", "I.I.")
          .baselineValues(null, null, null, "")
          .baselineValues("J.J.", "J.", "J.", "J.J.")
          .build().run();

      testBuilder()
          .sqlQuery("select (cast(null as varchar(10)) || lname) as CONCATOperator, " +
              "cast(null as varchar(10)) as NullColumn, lname, concat(cast(null as varchar(10)), lname) as CONCAT from concatNull")
          .ordered()
          .baselineColumns("CONCATOperator", "NullColumn", "lname", "CONCAT")
          .baselineValues(null, null, "Nowmer", "Nowmer")
          .baselineValues(null, null, "Whelply", "Whelply")
          .baselineValues(null, null, "Derry", "Derry")
          .baselineValues(null, null, "Spence", "Spence")
          .build().run();
    } finally {
      test("drop view concatNull;");
    }
  }

  @Test // see DRILL-2054
  public void testConcatOperator() throws Exception {
    testBuilder()
        .sqlQuery("select n_nationkey || '+' || n_name || '=' as CONCAT, n_nationkey, '+' as PLUS, n_name from cp.`tpch/nation.parquet`")
        .ordered()
        .csvBaselineFile("testframework/testExampleQueries/testConcatOperator.tsv")
        .baselineTypes(TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.INT, TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.VARCHAR)
        .baselineColumns("CONCAT", "n_nationkey", "PLUS", "n_name")
        .build().run();

    testBuilder()
        .sqlQuery("select (n_nationkey || n_name) as CONCAT from cp.`tpch/nation.parquet`")
        .ordered()
        .csvBaselineFile("testframework/testExampleQueries/testConcatOperatorInputTypeCombination.tsv")
        .baselineTypes(TypeProtos.MinorType.VARCHAR)
        .baselineColumns("CONCAT")
        .build().run();


    testBuilder()
        .sqlQuery("select (n_nationkey || cast(n_name as varchar(30))) as CONCAT from cp.`tpch/nation.parquet`")
        .ordered()
        .csvBaselineFile("testframework/testExampleQueries/testConcatOperatorInputTypeCombination.tsv")
        .baselineTypes(TypeProtos.MinorType.VARCHAR)
        .baselineColumns("CONCAT")
        .build().run();

    testBuilder()
        .sqlQuery("select (cast(n_nationkey as varchar(30)) || n_name) as CONCAT from cp.`tpch/nation.parquet`")
        .ordered()
        .csvBaselineFile("testframework/testExampleQueries/testConcatOperatorInputTypeCombination.tsv")
        .baselineTypes(TypeProtos.MinorType.VARCHAR)
        .baselineColumns("CONCAT")
        .build().run();
  }

  @Test // see DRILL-985
  public void testViewFileName() throws Exception {
    test("use dfs.tmp");
    test("create view nation_view_testexamplequeries as select * from cp.`tpch/nation.parquet`;");
    test("select * from dfs.tmp.`nation_view_testexamplequeries.view.drill`");
    test("drop view nation_view_testexamplequeries");
  }

  @Test
  public void testTextInClasspathStorage() throws Exception {
    test("select * from cp.`store/text/classpath_storage_csv_test.csv`");
  }

  @Test
  public void testParquetComplex() throws Exception {
    test("select recipe from cp.`parquet/complex.parquet`");
    test("select * from cp.`parquet/complex.parquet`");
    test("select recipe, c.inventor.name as name, c.inventor.age as age from cp.`parquet/complex.parquet` c");
  }

  @Test // see DRILL-553
  public void testQueryWithNullValues() throws Exception {
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
  }

  @Test
  public void testJoinMerge() throws Exception {
    test("alter session set `planner.enable_hashjoin` = false");
    test("select count(*) \n" +
        "  from (select l.l_orderkey as x, c.c_custkey as y \n" +
        "  from cp.`tpch/lineitem.parquet` l \n" +
        "    left outer join cp.`tpch/customer.parquet` c \n" +
        "      on l.l_orderkey = c.c_custkey) as foo\n" +
        "  where x < 10000\n" +
        "");
    test("alter session set `planner.enable_hashjoin` = true");
  }

  @Test
  public void testJoinExpOn() throws Exception {
    test("select a.n_nationkey from cp.`tpch/nation.parquet` a join cp.`tpch/region.parquet` b on a.n_regionkey + 1 = b.r_regionkey and a.n_regionkey + 1 = b.r_regionkey;");
  }

  @Test
  public void testJoinExpWhere() throws Exception {
    test("select a.n_nationkey from cp.`tpch/nation.parquet` a , cp.`tpch/region.parquet` b where a.n_regionkey + 1 = b.r_regionkey and a.n_regionkey + 1 = b.r_regionkey;");
  }

  @Test
  public void testPushExpInJoinConditionInnerJoin() throws Exception {
    test("select a.n_nationkey from cp.`tpch/nation.parquet` a join cp.`tpch/region.parquet` b " + "" +
      " on a.n_regionkey + 100  = b.r_regionkey + 200" +      // expressions in both sides of equal join filter
      "   and (substr(a.n_name,1,3)= 'L1' or substr(a.n_name,2,2) = 'L2') " +  // left filter
      "   and (substr(b.r_name,1,3)= 'R1' or substr(b.r_name,2,2) = 'R2') " +  // right filter
      "   and (substr(a.n_name,2,3)= 'L3' or substr(b.r_name,3,2) = 'R3');");  // non-equal join filter
  }

  @Test
  public void testPushExpInJoinConditionWhere() throws Exception {
    test("select a.n_nationkey from cp.`tpch/nation.parquet` a , cp.`tpch/region.parquet` b " + "" +
        " where a.n_regionkey + 100  = b.r_regionkey + 200" +      // expressions in both sides of equal join filter
        "   and (substr(a.n_name,1,3)= 'L1' or substr(a.n_name,2,2) = 'L2') " +  // left filter
        "   and (substr(b.r_name,1,3)= 'R1' or substr(b.r_name,2,2) = 'R2') " +  // right filter
        "   and (substr(a.n_name,2,3)= 'L3' or substr(b.r_name,3,2) = 'R3');");  // non-equal join filter
  }

  @Test
  public void testPushExpInJoinConditionLeftJoin() throws Exception {
    test("select a.n_nationkey, b.r_regionkey from cp.`tpch/nation.parquet` a left join cp.`tpch/region.parquet` b " +
        " on a.n_regionkey +100 = b.r_regionkey +200 " +        // expressions in both sides of equal join filter // left filter
        " and (substr(b.r_name,1,3)= 'R1' or substr(b.r_name,2,2) = 'R2') ");   // right filter // non-equal join filter
  }

  @Test
  public void testPushExpInJoinConditionRightJoin() throws Exception {
    test("select a.n_nationkey, b.r_regionkey from cp.`tpch/nation.parquet` a right join cp.`tpch/region.parquet` b " + "" +
        " on a.n_regionkey +100 = b.r_regionkey +200 " +        // expressions in both sides of equal join filter
        "   and (substr(a.n_name,1,3)= 'L1' or substr(a.n_name,2,2) = 'L2') ");  // left filter
  }

  @Test
  @Category(UnlikelyTest.class)
  public void testCaseReturnValueVarChar() throws Exception {
    test("select case when employee_id < 1000 then 'ABC' else 'DEF' end from cp.`employee.json` limit 5");
  }

  @Test
  @Category(UnlikelyTest.class)
  public void testCaseReturnValueBigInt() throws Exception {
    test("select case when employee_id < 1000 then 1000 else 2000 end from cp.`employee.json` limit 5");
  }

  @Test
  public void testHashPartitionSV2() throws Exception {
    test("select count(n_nationkey) from cp.`tpch/nation.parquet` where n_nationkey > 8 group by n_regionkey");
  }

  @Test
  public void testHashPartitionSV4() throws Exception {
    test("select count(n_nationkey) as cnt from cp.`tpch/nation.parquet` group by n_regionkey order by cnt");
  }

  @Test
  public void testSelectWithLimit() throws Exception {
    test("select employee_id,  first_name, last_name from cp.`employee.json` limit 5 ");
  }

  @Test
  public void testSelectWithLimit2() throws Exception {
    test("select l_comment, l_orderkey from cp.`tpch/lineitem.parquet` limit 10000 ");
  }

  @Test
  public void testSVRV4() throws Exception {
    test("select employee_id,  first_name from cp.`employee.json` order by employee_id ");
  }

  @Test
  public void testSVRV4MultBatch() throws Exception {
    test("select l_orderkey from cp.`tpch/lineitem.parquet` order by l_orderkey limit 10000 ");
  }

  @Test
  public void testSVRV4Join() throws Exception {
    test("select count(*) from cp.`tpch/lineitem.parquet` l, cp.`tpch/partsupp.parquet` ps \n" +
        " where l.l_partkey = ps.ps_partkey and l.l_suppkey = ps.ps_suppkey ;");
  }

  @Test
  public void testText() throws Exception {
    test("select * from cp.`store/text/data/regions.csv`");
  }

  @Test
  public void testFilterOnArrayTypes() throws Exception {
    test("select columns[0] from cp.`store/text/data/regions.csv` " +
      " where cast(columns[0] as int) > 1 and cast(columns[1] as varchar(20))='ASIA'");
  }

  @Test
  @Ignore("DRILL-3774")
  public void testTextPartitions() throws Exception {
    test("select * from cp.`store/text/data/`");
  }

  @Test
  @Ignore("DRILL-3004")
  public void testJoin() throws Exception {
    test("alter session set `planner.enable_hashjoin` = false");
    test("SELECT\n" +
        "  nations.N_NAME,\n" +
        "  regions.R_NAME\n" +
        "FROM\n" +
        "  cp.`tpch/nation.parquet` nations\n" +
        "JOIN\n" +
        "  cp.`tpch/region.parquet` regions\n" +
        "  on nations.N_REGIONKEY = regions.R_REGIONKEY where 1 = 0");
  }


  @Test
  public void testWhere() throws Exception {
    test("select * from cp.`employee.json` ");
  }

  @Test
  public void testGroupBy() throws Exception {
    test("select marital_status, COUNT(1) as cnt from cp.`employee.json` group by marital_status");
  }

  @Test
  public void testExplainPhysical() throws Exception {
    test("explain plan for select marital_status, COUNT(1) as cnt from cp.`employee.json` group by marital_status");
  }

  @Test
  public void testExplainLogical() throws Exception {
    test("explain plan without implementation for select marital_status, COUNT(1) as cnt from cp.`employee.json` group by marital_status");
  }

  @Test
  public void testGroupScanRowCountExp1() throws Exception {
    test("EXPLAIN plan for select count(n_nationkey) as mycnt, count(*) + 2 * count(*) as mycnt2 from cp.`tpch/nation.parquet` ");
  }

  @Test
  public void testGroupScanRowCount1() throws Exception {
    test("select count(n_nationkey) as mycnt, count(*) + 2 * count(*) as mycnt2 from cp.`tpch/nation.parquet` ");
  }

  @Test
  public void testColunValueCnt() throws Exception {
    test("select count( 1 + 2) from cp.`tpch/nation.parquet` ");
  }

  @Test
  public void testGroupScanRowCountExp2() throws Exception {
    test("EXPLAIN plan for select count(*) as mycnt, count(*) + 2 * count(*) as mycnt2 from cp.`tpch/nation.parquet` ");
  }

  @Test
  public void testGroupScanRowCount2() throws Exception {
    test("select count(*) as mycnt, count(*) + 2 * count(*) as mycnt2 from cp.`tpch/nation.parquet` where 1 < 2");
  }

  @Test
  @Category(UnlikelyTest.class)
  // cast non-exist column from json file. Should return null value.
  public void testDrill428() throws Exception {
    test("select cast(NON_EXIST_COL as varchar(10)) from cp.`employee.json` limit 2; ");
  }

  @Test  // Bugs DRILL-727, DRILL-940
  public void testOrderByDiffColumn() throws Exception {
    test("select r_name from cp.`tpch/region.parquet` order by r_regionkey");
    test("select r_name from cp.`tpch/region.parquet` order by r_name, r_regionkey");
    test("select cast(r_name as varchar(20)) from cp.`tpch/region.parquet` order by r_name");
  }

  @Test  // tests with LIMIT 0
  @Ignore("DRILL-1866")
  public void testLimit0_1() throws Exception {
    test("select n_nationkey, n_name from cp.`tpch/nation.parquet` limit 0");
    test("select n_nationkey, n_name from cp.`tpch/nation.parquet` limit 0 offset 5");
    test("select n_nationkey, n_name from cp.`tpch/nation.parquet` order by n_nationkey limit 0");
    test("select * from cp.`tpch/nation.parquet` limit 0");
    test("select n.n_nationkey from cp.`tpch/nation.parquet` n, cp.`tpch/region.parquet` r where n.n_regionkey = r.r_regionkey limit 0");
    test("select n_regionkey, count(*) from cp.`tpch/nation.parquet` group by n_regionkey limit 0");
  }

  @Test
  public void testTextJoin() throws Exception {
    test("select t1.columns[1] from cp.`store/text/data/nations.csv` t1, cp.`store/text/data/regions.csv` t2 where t1.columns[0] = t2.columns[0]");
  }

  @Test // DRILL-811
  public void testDRILL_811View() throws Exception {
    test("use dfs.tmp");
    test("create view nation_view_testexamplequeries as select * from cp.`tpch/nation.parquet`;");
    test("select n.n_nationkey, n.n_name, n.n_regionkey from nation_view_testexamplequeries n where n.n_nationkey > 8 order by n.n_regionkey");
    test("select n.n_regionkey, count(*) as cnt from nation_view_testexamplequeries n where n.n_nationkey > 8 group by n.n_regionkey order by n.n_regionkey");
    test("drop view nation_view_testexamplequeries ");
  }

  @Test  // DRILL-811
  public void testDRILL_811ViewJoin() throws Exception {
    test("use dfs.tmp");
    test("create view nation_view_testexamplequeries as select * from cp.`tpch/nation.parquet`;");
    test("create view region_view_testexamplequeries as select * from cp.`tpch/region.parquet`;");
    test("select n.n_nationkey, n.n_regionkey, r.r_name from region_view_testexamplequeries r , nation_view_testexamplequeries n where r.r_regionkey = n.n_regionkey ");
    test("select n.n_regionkey, count(*) as cnt from region_view_testexamplequeries r , nation_view_testexamplequeries n where r.r_regionkey = n.n_regionkey and n.n_nationkey > 8 group by n.n_regionkey order by n.n_regionkey");
    test("select n.n_regionkey, count(*) as cnt from region_view_testexamplequeries r join nation_view_testexamplequeries n on r.r_regionkey = n.n_regionkey and n.n_nationkey > 8 group by n.n_regionkey order by n.n_regionkey");
    test("drop view region_view_testexamplequeries ");
    test("drop view nation_view_testexamplequeries ");
  }

  @Test  // DRILL-811
  public void testDRILL_811Json() throws Exception {
    test("use dfs.tmp");
    test("create view region_view_testexamplequeries as select * from cp.`region.json`;");
    test("select sales_city, sales_region from region_view_testexamplequeries where region_id > 50 order by sales_country; ");
    test("drop view region_view_testexamplequeries ");
  }

  @Test
  @Category(UnlikelyTest.class)
  public void testCase() throws Exception {
    test("select case when n_nationkey > 0 and n_nationkey < 2 then concat(n_name, '_abc') when n_nationkey >=2 and n_nationkey < 4 then '_EFG' else concat(n_name,'_XYZ') end, n_comment from cp.`tpch/nation.parquet` ;");
  }

  @Test // tests join condition that has different input types
  public void testJoinCondWithDifferentTypes() throws Exception {
    test("select t1.department_description from cp.`department.json` t1, cp.`employee.json` t2 where (cast(t1.department_id as double)) = t2.department_id");
    test("select t1.full_name from cp.`employee.json` t1, cp.`department.json` t2 where cast(t1.department_id as double) = t2.department_id and cast(t1.position_id as bigint) = t2.department_id");

    // See DRILL-3995. Re-enable this once fixed.
    // test("select t1.full_name from cp.`employee.json` t1, cp.`department.json` t2 where t1.department_id = t2.department_id and t1.position_id = t2.department_id");
  }

  @Test
  public void testTopNWithSV2() throws Exception {
    int actualRecordCount = testSql("select N_NATIONKEY from cp.`tpch/nation.parquet` where N_NATIONKEY < 10 order by N_NATIONKEY limit 5");
    int expectedRecordCount = 5;
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);
  }

  @Test
  public void testTextQueries() throws Exception {
    test("select cast('285572516' as int) from cp.`tpch/nation.parquet` limit 1");
  }

  @Test // DRILL-1544
  @Category(UnlikelyTest.class)
  public void testLikeEscape() throws Exception {
    int actualRecordCount = testSql("select id, name from cp.`jsoninput/specialchar.json` where name like '%#_%' ESCAPE '#'");
    int expectedRecordCount = 1;
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);

  }

  @Test
  @Category(UnlikelyTest.class)
  public void testSimilarEscape() throws Exception {
    int actualRecordCount = testSql("select id, name from cp.`jsoninput/specialchar.json` where name similar to '(N|S)%#_%' ESCAPE '#'");
    int expectedRecordCount = 1;
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);
  }

  @Test
  @Category(UnlikelyTest.class)
  public void testImplicitDownwardCast() throws Exception {
    int actualRecordCount = testSql("select o_totalprice from cp.`tpch/orders.parquet` where o_orderkey=60000 and o_totalprice=299402");
    int expectedRecordCount = 0;
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);
  }

  @Test // DRILL-1470
  @Category(UnlikelyTest.class)
  public void testCastToVarcharWithLength() throws Exception {
    // cast from varchar with unknown length to a fixed length.
    int actualRecordCount = testSql("select first_name from cp.`employee.json` where cast(first_name as varchar(2)) = 'Sh'");
    int expectedRecordCount = 27;
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);

    // cast from varchar with unknown length to varchar(5), then to varchar(10), then to varchar(2). Should produce the same result as the first query.
    actualRecordCount = testSql("select first_name from cp.`employee.json` where cast(cast(cast(first_name as varchar(5)) as varchar(10)) as varchar(2)) = 'Sh'");
    expectedRecordCount = 27;
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);


    // this long nested cast expression should be essentially equal to substr(), meaning the query should return every row in the table.
    actualRecordCount = testSql("select first_name from cp.`employee.json` where cast(cast(cast(first_name as varchar(5)) as varchar(10)) as varchar(2)) = substr(first_name, 1, 2)");
    expectedRecordCount = 1155;
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);

    // cast is applied to a column from parquet file.
    actualRecordCount = testSql("select n_name from cp.`tpch/nation.parquet` where cast(n_name as varchar(2)) = 'UN'");
    expectedRecordCount = 2;
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);
  }

  @Test // DRILL-1488
  @Category(UnlikelyTest.class)
  public void testIdentifierMaxLength() throws Exception {
    // use long column alias name (approx 160 chars)
    test("select employee_id as  aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa from cp.`employee.json` limit 1");

    // use long table alias name  (approx 160 chars)
    test("select employee_id from cp.`employee.json` as aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa limit 1");

  }

  @Test // DRILL-1846  (this tests issue with SimpleMergeExchange)
  public void testOrderByDiffColumnsInSubqAndOuter() throws Exception {
    String query = "select n.n_nationkey from  (select n_nationkey, n_regionkey from cp.`tpch/nation.parquet` order by n_regionkey) n  order by n.n_nationkey";
    // set slice_target = 1 to force exchanges
    test("alter session set `planner.slice_target` = 1; " + query);
  }

  @Test // DRILL-1846  (this tests issue with UnionExchange)
  @Ignore("DRILL-1866")
  public void testLimitInSubqAndOrderByOuter() throws Exception {
    String query = "select t2.n_nationkey from (select n_nationkey, n_regionkey from cp.`tpch/nation.parquet` t1 group by n_nationkey, n_regionkey limit 10) t2 order by t2.n_nationkey";
    // set slice_target = 1 to force exchanges
    test("alter session set `planner.slice_target` = 1; " + query);
  }

  @Test // DRILL-1788
  @Category(UnlikelyTest.class)
  public void testCaseInsensitiveJoin() throws Exception {
    test("select n3.n_name from (select n2.n_name from cp.`tpch/nation.parquet` n1, cp.`tpch/nation.parquet` n2 where n1.N_name = n2.n_name) n3 " +
        " join cp.`tpch/nation.parquet` n4 on n3.n_name = n4.n_name");
  }

  @Test // DRILL-1561
  public void test2PhaseAggAfterOrderBy() throws Exception {
    String query = "select count(*) from (select o_custkey from cp.`tpch/orders.parquet` order by o_custkey)";
    // set slice_target = 1 to force exchanges and 2-phase aggregation
    test("alter session set `planner.slice_target` = 1; " + query);
  }

  @Test // DRILL-1867
  public void testCaseInsensitiveSubQuery() throws Exception {
    int actualRecordCount = 0, expectedRecordCount = 0;

    // source is JSON
    actualRecordCount = testSql("select EMPID from ( select employee_id as empid from cp.`employee.json` limit 2)");
    expectedRecordCount = 2;
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);

    actualRecordCount = testSql("select EMPLOYEE_ID from ( select employee_id from cp.`employee.json` where Employee_id is not null limit 2)");
    expectedRecordCount = 2;
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);

    actualRecordCount = testSql("select x.EMPLOYEE_ID from ( select employee_id from cp.`employee.json` limit 2) X");
    expectedRecordCount = 2;
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);

    // source is PARQUET
    actualRecordCount = testSql("select NID from ( select n_nationkey as nid from cp.`tpch/nation.parquet`) where NID = 3");
    expectedRecordCount = 1;
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);

    actualRecordCount = testSql("select x.N_nationkey from ( select n_nationkey from cp.`tpch/nation.parquet`) X where N_NATIONKEY = 3");
    expectedRecordCount = 1;
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);

    // source is CSV
    String root = "store/text/data/regions.csv";
    String query = String.format("select rid, x.name from (select columns[0] as RID, columns[1] as NAME from cp.`%s`) X where X.rid = 2", root);
    actualRecordCount = testSql(query);
    expectedRecordCount = 1;
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);

  }

  @Test
  public void testMultipleCountDistinctWithGroupBy() throws Exception {
    String query = "select n_regionkey, count(distinct n_nationkey), count(distinct n_name) from cp.`tpch/nation.parquet` group by n_regionkey;";
    String hashagg_only = "alter session set `planner.enable_hashagg` = true; " +
        "alter session set `planner.enable_streamagg` = false;";
    String streamagg_only = "alter session set `planner.enable_hashagg` = false; " +
        "alter session set `planner.enable_streamagg` = true;";

    // hash agg and streaming agg with default slice target (single phase aggregate)
    test(hashagg_only + query);
    test(streamagg_only + query);

    // hash agg and streaming agg with lower slice target (multiphase aggregate)
    test("alter session set `planner.slice_target` = 1; " + hashagg_only + query);
    test("alter session set `planner.slice_target` = 1; " + streamagg_only + query);
  }

  @Test // DRILL-2019
  public void testFilterInSubqueryAndOutside() throws Exception {
    String query1 = "select r_regionkey from (select r_regionkey from cp.`tpch/region.parquet` o where r_regionkey < 2) where r_regionkey > 2";
    String query2 = "select r_regionkey from (select r_regionkey from cp.`tpch/region.parquet` o where r_regionkey < 4) where r_regionkey > 1";
    int actualRecordCount = 0;
    int expectedRecordCount = 0;

    actualRecordCount = testSql(query1);
    assertEquals(expectedRecordCount, actualRecordCount);

    expectedRecordCount = 2;
    actualRecordCount = testSql(query2);
    assertEquals(expectedRecordCount, actualRecordCount);
  }

  @Test // DRILL-1973
  public void testLimit0SubqueryWithFilter() throws Exception {
    String query1 = "select * from (select sum(1) as x from  cp.`tpch/region.parquet` limit 0) WHERE x < 10";
    String query2 = "select * from (select sum(1) as x from  cp.`tpch/region.parquet` limit 0) WHERE (0 = 1)";
    int actualRecordCount = 0;
    int expectedRecordCount = 0;

    actualRecordCount = testSql(query1);
    assertEquals(expectedRecordCount, actualRecordCount);

    actualRecordCount = testSql(query2);
    assertEquals(expectedRecordCount, actualRecordCount);
  }

  @Test // DRILL-2063
  public void testAggExpressionWithGroupBy() throws Exception {
    String query = "select l_suppkey, sum(l_extendedprice)/sum(l_quantity) as avg_price \n" +
        " from cp.`tpch/lineitem.parquet` where l_orderkey in \n" +
        " (select o_orderkey from cp.`tpch/orders.parquet` where o_custkey = 2) \n" +
        " and l_suppkey = 4 group by l_suppkey";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("l_suppkey", "avg_price")
        .baselineValues(4, 1374.47)
        .build().run();

  }

  @Test // DRILL-1888
  public void testAggExpressionWithGroupByHaving() throws Exception {
    String query = "select l_suppkey, sum(l_extendedprice)/sum(l_quantity) as avg_price \n" +
        " from cp.`tpch/lineitem.parquet` where l_orderkey in \n" +
        " (select o_orderkey from cp.`tpch/orders.parquet` where o_custkey = 2) \n" +
        " group by l_suppkey having sum(l_extendedprice)/sum(l_quantity) > 1850.0";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("l_suppkey", "avg_price")
        .baselineValues(98, 1854.95)
        .build().run();
  }

  @Test
  public void testExchangeRemoveForJoinPlan() throws Exception {
    String sql = "select t2.n_nationkey from dfs.`tpchmulti/region` t1 join dfs.`tpchmulti/nation` t2 on t2.n_regionkey = t1.r_regionkey";

    testBuilder()
        .unOrdered()
        .optionSettingQueriesForTestQuery("alter session set `planner.slice_target` = 10; alter session set `planner.join.row_count_estimate_factor` = 0.1")  // Enforce exchange will be inserted.
        .sqlQuery(sql)
        .optionSettingQueriesForBaseline("alter session set `planner.slice_target` = 100000; alter session set `planner.join.row_count_estimate_factor` = 1.0") // Use default option setting.
        .sqlBaselineQuery(sql)
        .build()
        .run();
  }

  @Test //DRILL-2163
  public void testNestedTypesPastJoinReportsValidResult() throws Exception {
    testBuilder()
        .sqlQuery("select t1.uid, t1.events, t1.events[0].evnt_id as event_id, t2.transactions, " +
          "t2.transactions[0] as trans, t1.odd, t2.even from cp.`project/complex/a.json` t1, " +
          "cp.`project/complex/b.json` t2 where t1.uid = t2.uid")
        .ordered()
        .jsonBaselineFile("project/complex/drill-2163-result.json")
        .build()
        .run();
  }

  @Test
  @Category(UnlikelyTest.class)
  public void testSimilar() throws Exception {
    testBuilder()
        .sqlQuery("select n_nationkey " +
          "from cp.`tpch/nation.parquet` " +
          "where n_name similar to 'CHINA' " +
          "order by n_regionkey")
        .unOrdered()
        .optionSettingQueriesForTestQuery("alter session set `planner.slice_target` = 1")
        .baselineColumns("n_nationkey")
        .baselineValues(18)
        .go();

    test("alter session set `planner.slice_target` = " + ExecConstants.SLICE_TARGET_DEFAULT);
  }

  @Test // DRILL-2311
  @Ignore("Move to TestParquetWriter. Have to ensure same file name does not exist on filesystem.")
  public void testCreateTableSameColumnNames() throws Exception {
    String creatTable = "CREATE TABLE CaseInsensitiveColumnNames as " +
        "select cast(r_regionkey as BIGINT) BIGINT_col, cast(r_regionkey as DECIMAL) bigint_col \n" +
        "FROM cp.`tpch/region.parquet`;\n";

    test("USE dfs.tmp");
    test(creatTable);

    testBuilder()
        .sqlQuery("select * from `CaseInsensitiveColumnNames`")
        .unOrdered()
        .baselineColumns("BIGINT_col", "bigint_col0\n")
        .baselineValues((long) 0, new BigDecimal(0))
        .baselineValues((long) 1, new BigDecimal(1))
        .baselineValues((long) 2, new BigDecimal(2))
        .baselineValues((long) 3, new BigDecimal(3))
        .baselineValues((long) 4, new BigDecimal(4))
        .build().run();
  }

  @Test // DRILL-1943, DRILL-1911
  @Category(UnlikelyTest.class)
  public void testColumnNamesDifferInCaseOnly() throws Exception {
    testBuilder()
        .sqlQuery("select r_regionkey a, r_regionkey A FROM cp.`tpch/region.parquet`")
        .unOrdered()
        .baselineColumns("a", "A0")
        .baselineValues(0, 0)
        .baselineValues(1, 1)
        .baselineValues(2, 2)
        .baselineValues(3, 3)
        .baselineValues(4, 4)
        .build().run();

    testBuilder()
        .sqlQuery("select employee_id, Employee_id from cp.`employee.json` limit 2")
        .unOrdered()
        .baselineColumns("employee_id", "Employee_id0")
        .baselineValues((long) 1, (long) 1)
        .baselineValues((long) 2, (long) 2)
        .build().run();
  }

  @Test // DRILL-2094
  public void testOrderbyArrayElement() throws Exception {
    testBuilder()
        .sqlQuery("select t.id, t.list[0] as SortingElem " +
          "from cp.`store/json/orderByArrayElement.json` t " +
          "order by t.list[0]")
        .ordered()
        .baselineColumns("id", "SortingElem")
        .baselineValues((long) 1, (long) 1)
        .baselineValues((long) 5, (long) 2)
        .baselineValues((long) 4, (long) 3)
        .baselineValues((long) 2, (long) 5)
        .baselineValues((long) 3, (long) 6)
        .build().run();
  }

  @Test // DRILL-2479
  public void testCorrelatedExistsWithInSubq() throws Exception {
    testBuilder()
        .sqlQuery("select count(*) as cnt from cp.`tpch/lineitem.parquet` l where exists "
          + " (select ps.ps_suppkey from cp.`tpch/partsupp.parquet` ps where ps.ps_suppkey = l.l_suppkey and ps.ps_partkey "
          + " in (select p.p_partkey from cp.`tpch/part.parquet` p where p.p_type like '%NICKEL'))")
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(60175l)
        .go();
  }

  @Test // DRILL-2094
  public void testOrderbyArrayElementInSubquery() throws Exception {
    testBuilder()
        .sqlQuery("select s.id from " +
          "(select id " +
          "from cp.`store/json/orderByArrayElement.json` " +
          "order by list[0]) s")
        .ordered()
        .baselineColumns("id")
        .baselineValues((long) 1)
        .baselineValues((long) 5)
        .baselineValues((long) 4)
        .baselineValues((long) 2)
        .baselineValues((long) 3)
        .build().run();
  }

  @Test // DRILL-1978
  public void testCTASOrderByCoumnNotInSelectClause() throws Exception {
    String queryCTAS1 = "CREATE TABLE TestExampleQueries_testCTASOrderByCoumnNotInSelectClause1 as " +
        "select r_name from cp.`tpch/region.parquet` order by r_regionkey;";

    String queryCTAS2 = "CREATE TABLE TestExampleQueries_testCTASOrderByCoumnNotInSelectClause2 as " +
        "SELECT columns[1] as col FROM cp.`store/text/data/regions.csv` ORDER BY cast(columns[0] as double)";

    String query1 = "select * from TestExampleQueries_testCTASOrderByCoumnNotInSelectClause1";
    String query2 = "select * from TestExampleQueries_testCTASOrderByCoumnNotInSelectClause2";

    test("use dfs.tmp");
    test(queryCTAS1);
    test(queryCTAS2);


    testBuilder()
        .sqlQuery(query1)
        .ordered()
        .baselineColumns("r_name")
        .baselineValues("AFRICA")
        .baselineValues("AMERICA")
        .baselineValues("ASIA")
        .baselineValues("EUROPE")
        .baselineValues("MIDDLE EAST")
        .build().run();

    testBuilder()
        .sqlQuery(query2)
        .ordered()
        .baselineColumns("col")
        .baselineValues("AFRICA")
        .baselineValues("AMERICA")
        .baselineValues("ASIA")
        .baselineValues("EUROPE")
        .baselineValues("MIDDLE EAST")
        .build().run();
  }

  @Test // DRILL-2221
  public void createJsonWithEmptyList() throws Exception {
    final String tableName = "jsonWithEmptyList";
    test("USE dfs.tmp");
    test("ALTER SESSION SET `store.format`='json'");
    test("CREATE TABLE %s AS SELECT * FROM cp.`store/json/record_with_empty_list.json`", tableName);
    test("SELECT COUNT(*) FROM %s", tableName);
    test("ALTER SESSION SET `store.format`='parquet'");
  }

  @Test // DRILL-2914
  public void testGroupByStarSchemaless() throws Exception {
    String query = "SELECT n.n_nationkey AS col \n" +
        "FROM (SELECT * FROM cp.`tpch/nation.parquet`) AS n \n" +
        "GROUP BY n.n_nationkey \n" +
        "ORDER BY n.n_nationkey";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .csvBaselineFile("testframework/testExampleQueries/testGroupByStarSchemaless.tsv")
        .baselineTypes(TypeProtos.MinorType.INT)
        .baselineColumns("col")
        .build()
        .run();
  }

  @Test // DRILL-1927
  public void testGroupByCaseInSubquery() throws Exception {
    String query1 = "select (case when t.r_regionkey in (3) then 0 else 1 end) as col \n" +
        "from cp.`tpch/region.parquet` t \n" +
        "group by (case when t.r_regionkey in (3) then 0 else 1 end)";

    String query2 = "select sum(case when t.r_regionkey in (3) then 0 else 1 end) as col \n" +
        "from cp.`tpch/region.parquet` t";

    String query3 = "select (case when (r_regionkey IN (0, 2, 3, 4)) then 0 else r_regionkey end) as col1, min(r_regionkey) as col2 \n" +
        "from cp.`tpch/region.parquet` \n" +
        "group by (case when (r_regionkey IN (0, 2, 3, 4)) then 0 else r_regionkey end)";

    testBuilder()
        .sqlQuery(query1)
        .unOrdered()
        .baselineColumns("col")
        .baselineValues(0)
        .baselineValues(1)
        .build()
        .run();

    testBuilder()
        .sqlQuery(query2)
        .unOrdered()
        .baselineColumns("col")
        .baselineValues((long) 4)
        .build()
        .run();

    testBuilder()
        .sqlQuery(query3)
        .unOrdered()
        .baselineColumns("col1", "col2")
        .baselineValues(0, 0)
        .baselineValues(1, 1)
        .build()
        .run();
  }

  @Test  // DRILL-2966
  public void testHavingAggFunction() throws Exception {
    String query1 = "select n_nationkey as col \n" +
        "from cp.`tpch/nation.parquet` \n" +
        "group by n_nationkey \n" +
        "having sum(case when n_regionkey in (1, 2) then 1 else 0 end) + \n" +
        "sum(case when n_regionkey in (2, 3) then 1 else 0 end) > 1";

    String query2 = "select n_nationkey as col \n"
        + "from cp.`tpch/nation.parquet` \n"
        + "group by n_nationkey \n"
        + "having n_nationkey in \n"
        + "(select r_regionkey \n"
        + "from cp.`tpch/region.parquet` \n"
        + "group by r_regionkey \n"
        + "having sum(r_regionkey) > 0)";

    String query3 = "select n_nationkey as col \n"
        + "from cp.`tpch/nation.parquet` \n"
        + "group by n_nationkey \n"
        + "having max(n_regionkey) > ((select min(r_regionkey) from cp.`tpch/region.parquet`) + 3)";

    testBuilder()
        .sqlQuery(query1)
        .unOrdered()
        .csvBaselineFile("testframework/testExampleQueries/testHavingAggFunction/q1.tsv")
        .baselineTypes(TypeProtos.MinorType.INT)
        .baselineColumns("col")
        .build()
        .run();

    testBuilder()
        .sqlQuery(query2)
        .unOrdered()
        .csvBaselineFile("testframework/testExampleQueries/testHavingAggFunction/q2.tsv")
        .baselineTypes(TypeProtos.MinorType.INT)
        .baselineColumns("col")
        .build()
        .run();

    testBuilder()
        .sqlQuery(query3)
        .unOrdered()
        .csvBaselineFile("testframework/testExampleQueries/testHavingAggFunction/q3.tsv")
        .baselineTypes(TypeProtos.MinorType.INT)
        .baselineColumns("col")
        .build()
        .run();
  }

  @Test  //DRILL-3018
  public void testNestLoopJoinScalarSubQ() throws Exception {
    testBuilder()
        .sqlQuery("select n_nationkey from cp.`tpch/nation.parquet` where n_nationkey >= (select min(c_nationkey) from cp.`tpch/customer.parquet`)")
        .unOrdered()
        .sqlBaselineQuery("select n_nationkey from cp.`tpch/nation.parquet`")
        .build()
        .run();
  }

  @Test //DRILL-2953
  public void testGbAndObDifferentExp() throws Exception {
    testBuilder()
        .sqlQuery("select cast(columns[0] as int) as nation_key " +
          " from cp.`store/text/data/nations.csv` " +
          " group by columns[0] " +
          " order by cast(columns[0] as int)")
        .ordered()
        .csvBaselineFile("testframework/testExampleQueries/testGroupByStarSchemaless.tsv")
        .baselineTypes(TypeProtos.MinorType.INT)
        .baselineColumns("nation_key")
        .build()
        .run();

    testBuilder()
        .sqlQuery("select cast(columns[0] as int) as nation_key " +
          " from cp.`store/text/data/nations.csv` " +
          " group by cast(columns[0] as int) " +
          " order by cast(columns[0] as int)")
        .ordered()
        .csvBaselineFile("testframework/testExampleQueries/testGroupByStarSchemaless.tsv")
        .baselineTypes(TypeProtos.MinorType.INT)
        .baselineColumns("nation_key")
        .build()
        .run();

  }

  @Test  //DRILL_3004
  public void testDRILL_3004() throws Exception {
    final String query =
        "SELECT\n" +
        "  nations.N_NAME,\n" +
        "  regions.R_NAME\n" +
        "FROM\n" +
        "  cp.`tpch/nation.parquet` nations\n" +
        "JOIN\n" +
        "  cp.`tpch/region.parquet` regions\n" +
        "on nations.N_REGIONKEY = regions.R_REGIONKEY " +
        "where 1 = 0";


    testBuilder()
        .sqlQuery(query)
        .expectsEmptyResultSet()
        .optionSettingQueriesForTestQuery("ALTER SESSION SET `planner.enable_hashjoin` = false; " +
            "ALTER SESSION SET `planner.disable_exchanges` = true")
        .build()
        .run();
  }

  @Test
  public void testRepeatedListProjectionPastJoin() throws Exception {
    final String query = "select * from cp.`join/join-left-drill-3032.json` f1 inner join cp.`join/join-right-drill-3032.json` f2 on f1.id = f2.id";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("id", "id0", "aaa")
        .baselineValues(1L, 1L, listOf(listOf(listOf("val1"), listOf("val2"))))
        .go();
  }

  @Test
  @Ignore
  public void testPartitionCTAS() throws  Exception {
    test("use dfs.tmp; " +
        "create table mytable1  partition by (r_regionkey, r_comment) as select r_regionkey, r_name, r_comment from cp.`tpch/region.parquet`");

    test("use dfs.tmp; " +
        "create table mytable2  partition by (r_regionkey, r_comment) as select * from cp.`tpch/region.parquet` where r_name = 'abc' ");

    test("use dfs.tmp; " +
        "create table mytable3  partition by (r_regionkey, n_nationkey) as " +
        "  select r.r_regionkey, r.r_name, n.n_nationkey, n.n_name from cp.`tpch/nation.parquet` n, cp.`tpch/region.parquet` r " +
        "  where n.n_regionkey = r.r_regionkey");

    test("use dfs.tmp; " +
        "create table mytable4  partition by (r_regionkey, r_comment) as " +
        "  select  r.* from cp.`tpch/nation.parquet` n, cp.`tpch/region.parquet` r " +
        "  where n.n_regionkey = r.r_regionkey");


  }

  @Test // DRILL-3210
  public void testWindowFunAndStarCol() throws Exception {
    // SingleTableQuery : star + window function
    final String query =
        " select * , sum(n_nationkey) over (partition by n_regionkey) as sumwin " +
        " from cp.`tpch/nation.parquet`";
    final String baseQuery =
        " select n_nationkey, n_name, n_regionkey, n_comment, " +
        "   sum(n_nationkey) over (partition by n_regionkey) as sumwin " +
        " from cp.`tpch/nation.parquet`";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .sqlBaselineQuery(baseQuery)
        .build()
        .run();

    // JoinQuery: star + window function
    final String joinQuery =
        " select *, sum(n.n_nationkey) over (partition by r.r_regionkey order by r.r_name) as sumwin" +
        " from cp.`tpch/nation.parquet` n, cp.`tpch/region.parquet` r " +
        " where n.n_regionkey = r.r_regionkey";
    final String joinBaseQuery =
        " select n.n_nationkey, n.n_name, n.n_regionkey, n.n_comment, r.r_regionkey, r.r_name, r.r_comment, " +
        "   sum(n.n_nationkey) over (partition by r.r_regionkey order by r.r_name) as sumwin " +
        " from cp.`tpch/nation.parquet` n, cp.`tpch/region.parquet` r " +
        " where n.n_regionkey = r.r_regionkey";

    testBuilder()
        .sqlQuery(joinQuery)
        .unOrdered()
        .sqlBaselineQuery(joinBaseQuery)
        .build()
        .run();
  }

  @Test // see DRILL-3557
  @Category(UnlikelyTest.class)
  public void testEmptyCSVinDirectory() throws Exception {
    test("explain plan for select * from dfs.`store/text/directoryWithEmpyCSV`");
    test("explain plan for select * from cp.`store/text/directoryWithEmpyCSV/empty.csv`");
  }

  @Test
  @Category(UnlikelyTest.class)
  public void testNegativeExtractOperator() throws Exception {
    testBuilder()
        .sqlQuery("select -EXTRACT(DAY FROM birth_date) as col \n" +
          "from cp.`employee.json` \n" +
          "order by col \n" +
          "limit 5")
        .ordered()
        .baselineColumns("col")
        .baselineValues(-27l)
        .baselineValues(-27l)
        .baselineValues(-27l)
        .baselineValues(-26l)
        .baselineValues(-26l)
        .build()
        .run();
  }

  @Test // see DRILL-2313
  public void testDistinctOverAggFunctionWithGroupBy() throws Exception {
    String query1 = "select distinct count(distinct n_nationkey) as col from cp.`tpch/nation.parquet` group by n_regionkey order by 1";
    String query2 = "select distinct count(distinct n_nationkey) as col from cp.`tpch/nation.parquet` group by n_regionkey order by count(distinct n_nationkey)";
    String query3 = "select distinct sum(n_nationkey) as col from cp.`tpch/nation.parquet` group by n_regionkey order by 1";
    String query4 = "select distinct sum(n_nationkey) as col from cp.`tpch/nation.parquet` group by n_regionkey order by col";

    testBuilder()
        .sqlQuery(query1)
        .unOrdered()
        .baselineColumns("col")
        .baselineValues((long) 5)
        .build()
        .run();

    testBuilder()
        .sqlQuery(query2)
        .unOrdered()
        .baselineColumns("col")
        .baselineValues((long) 5)
        .build()
        .run();

    testBuilder()
        .sqlQuery(query3)
        .ordered()
        .baselineColumns("col")
        .baselineValues((long) 47)
        .baselineValues((long) 50)
        .baselineValues((long) 58)
        .baselineValues((long) 68)
        .baselineValues((long) 77)
        .build()
        .run();

    testBuilder()
        .sqlQuery(query4)
        .ordered()
        .baselineColumns("col")
        .baselineValues((long) 47)
        .baselineValues((long) 50)
        .baselineValues((long) 58)
        .baselineValues((long) 68)
        .baselineValues((long) 77)
        .build()
        .run();
  }

  @Test // DRILL-2190
  @Category(UnlikelyTest.class)
  public void testDateImplicitCasting() throws Exception {
    String query = "SELECT birth_date \n" +
        "FROM cp.`employee.json` \n" +
        "WHERE birth_date BETWEEN '1920-01-01' AND cast('1931-01-01' AS DATE) \n" +
        "order by birth_date";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("birth_date")
        .baselineValues("1920-04-17")
        .baselineValues("1921-12-04")
        .baselineValues("1922-08-10")
        .baselineValues("1926-10-27")
        .baselineValues("1928-03-20")
        .baselineValues("1930-01-08")
        .build()
        .run();
  }

  //@Test
  public void t() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher)
            // Easy way to run single threaded for easy debugging
            .maxParallelization(1)
            // Set some session options
            .sessionOption(ExecConstants.ENABLE_VERBOSE_ERRORS_KEY, true);

    String parentDir = "F:\\drill\\files\\fact_dim_tables";

    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "root", parentDir, "parquet");
      //String sql = "select * from `dfs.root`.`fact` t";

      //String sql = "select f.col_vrchr as f_col, d.col_vrchr as d_col from `dfs.root`.`fact` f, `dfs.root`.`dim` d where f.dir0 = d.dir0 and d.dir0 = '1991'";
      String sql = "select f.col_vrchr as f_col, d.col_vrchr as d_col from `dfs.root`.`fact` f inner join `dfs.root`.`dim` d on f.dir0 = d.dir0 where d.dir0 = '1991'";

      QueryBuilder queryBuilder = client.queryBuilder().sql(sql);
      // print data
      //queryBuilder.printCsv();

      // print plan
      System.out.println(queryBuilder.explainText());
    }

    /*
00-00    Screen
00-01      Project(f_col=[$0], d_col=[$1])
00-02        Project(f_col=[$1], d_col=[$3])
00-03          HashJoin(condition=[=($0, $2)], joinType=[inner])
00-05            Scan(groupscan=[ParquetGroupScan [entries=[ReadEntryWithPath [path=file:/F:/drill/files/fact_dim_tables/fact/1991/alltypes_optional.parquet], ReadEntryWithPath [path=file:/F:/drill/files/fact_dim_tables/fact/1992/alltypes_optional.parquet]], selectionRoot=file:/F:/drill/files/fact_dim_tables/fact, numFiles=2, numRowGroups=2, usedMetadataFile=false, columns=[`dir0`, `col_vrchr`]]])
00-04            Project(dir00=[$0], col_vrchr0=[$1])
00-06              Scan(groupscan=[ParquetGroupScan [entries=[ReadEntryWithPath [path=F:/drill/files/fact_dim_tables/dim/1991/alltypes_optional.parquet]], selectionRoot=file:/F:/drill/files/fact_dim_tables/dim, numFiles=1, numRowGroups=1, usedMetadataFile=false, columns=[`dir0`, `col_vrchr`]]])

     */
  }

  //@Test
  public void test_1312() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher)
      // Easy way to run single threaded for easy debugging
      .maxParallelization(1)
      // Set some session options
      .sessionOption(ExecConstants.ENABLE_VERBOSE_ERRORS_KEY, true);

    String parentDir = "F:\\drill\\files\\fact_dim_tables";

    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "root", parentDir, "parquet");
      //String sql = "select * from `dfs.root`.`fact` t";

      String sql = "select * from `dfs.root`.`dim` d where d.dir0 = '1991'";

      QueryBuilder queryBuilder = client.queryBuilder().sql(sql);
      // print data
      //queryBuilder.printCsv();

      // print plan
      System.out.println(queryBuilder.explainText());

      // create view

      client.queryBuilder().sql("create view dfs.tmp.dim_view as select * from `dfs.root`.`dim`").run();

      String view_sql = "select * from `dfs.tmp`.`dim_view` d where d.dir0 = '1991'";

      QueryBuilder viewQueryBuilder = client.queryBuilder().sql(view_sql);
      // print data
      //queryBuilder.printCsv();

      // print plan
      System.out.println(viewQueryBuilder.explainText());

      client.queryBuilder().sql("drop view dfs.tmp.dim_view as select * from `dfs.root`.`dim`").run();
    }

    /*

    // from table
00-00    Screen
00-01      Project(*=[$0])
00-02        Project(*=[$0])
00-03          Scan(groupscan=[ParquetGroupScan [entries=[ReadEntryWithPath [path=file:/F:/drill/files/fact_dim_tables/dim/1991/alltypes_optional.parquet]], selectionRoot=file:/F:/drill/files/fact_dim_tables/dim, numFiles=1, numRowGroups=1, usedMetadataFile=false, columns=[`*`]]])

    // from view
00-00    Screen
00-01      Project(*=[$0])
00-02        SelectionVectorRemover
00-03          Filter(condition=[=(ITEM($0, 'dir0'), '1991')])
00-04            Scan(groupscan=[ParquetGroupScan [entries=[ReadEntryWithPath [path=file:/F:/drill/files/fact_dim_tables/dim/1991/alltypes_optional.parquet], ReadEntryWithPath [path=file:/F:/drill/files/fact_dim_tables/dim/1992/alltypes_optional.parquet]], selectionRoot=file:/F:/drill/files/fact_dim_tables/dim, numFiles=2, numRowGroups=2, usedMetadataFile=false, columns=[`*`]]])
     */

  }

  //@Test
  public void testFilterPDForInterval() throws Exception {
    dirTestWatcher.copyResourceToRoot(Paths.get("parquetFilterPush"));
    final String query = "select o_ordertimestamp from dfs.`parquetFilterPush/tsTbl` " +
      "where o_ordertimestamp >= '1992-01-01' and o_ordertimestamp < date '1992-01-01' + interval '5' day";

    // where L_SHIPDate >= date '1997-01-01' and L_SHIPDate < date '1997-01-01' + interval '3' day
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher)
      // Easy way to run single threaded for easy debugging
      .maxParallelization(1)
      // Set some session options
      .sessionOption(ExecConstants.ENABLE_VERBOSE_ERRORS_KEY, true);

    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      QueryBuilder viewQueryBuilder = client.queryBuilder().sql(query);
      System.out.println(viewQueryBuilder.explainText());
    }

  }

  //@Test
  public void testFilterPushDownForUnion() throws Exception {
    // F:\drill\files\fact_dim_tables\dim\1991
    // F:\drill\files\fact_dim_tables\dim\1992

    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher)
      // Easy way to run single threaded for easy debugging
      .maxParallelization(1)
      // Set some session options
      .sessionOption(ExecConstants.ENABLE_VERBOSE_ERRORS_KEY, true);

    String parentDir = "D:\\drill\\files\\fact_dim_tables\\dim";

    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "root", parentDir, "parquet");

      String query = "select col_vrchr from " +
        "(select col_vrchr from `dfs.root`.`1991` union all select col_vrchr from `dfs.root`.`1992`) v where v.col_vrchr = '10'";

      QueryBuilder viewQueryBuilder = client.queryBuilder().sql(query);
      System.out.println(viewQueryBuilder.explainText());
    }
  }

  //@Test
  public void testWithKnownColumns() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher)
      // Easy way to run single threaded for easy debugging
      .maxParallelization(1)
      // Set some session options
      .sessionOption(ExecConstants.ENABLE_VERBOSE_ERRORS_KEY, true);

    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      //cluster.defineWorkspace("dfs", "root", parentDir, "parquet");

      //String query = "select version from sys.version v join sys.options o on v.version = o.name where o.name = 'xxx'";
      String query = "select 1 from sys.boot b join sys.options o on b.name = o.name and o.name = 'xxx'";

      QueryBuilder viewQueryBuilder = client.queryBuilder().sql(query);
      System.out.println(viewQueryBuilder.explainText()); // physical plan
      // logical -> explain plan without implementation for
    }
  }


  @Test // MD-1360 + MD-1312
  public void testItemOperator() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher)
      // Easy way to run single threaded for easy debugging
      .maxParallelization(1)
      // Set some session options
      .sessionOption(ExecConstants.ENABLE_VERBOSE_ERRORS_KEY, true);

    String parentDir = "F:\\drill\\files\\fact_dim_tables\\fact";

    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "root", parentDir, "parquet");

      //String query = "select col_vrchr, count(*) from `dfs.root`.`1992` group by col_vrchr";
      //String query = "select col_vrchr, count(*) from (select * from `dfs.root`.`1992`) v group by col_vrchr";
      //String query = "select col_vrchr as c1, col_int, count(*) from (select * from `dfs.root`.`1992`) v group by col_vrchr, col_int";
      //String query = "select col_vrchr, * from `dfs.root`.`1992`";
      //String query = "select * from `dfs.root`.`1992`";
      //String query = "select col_vrchr as c1, col_int from (select * from `dfs.root`.`1992`) v ";
     // String query = "select col_vrchr as c1, col_int from (select * from `dfs.root`.`1992`) v2 union all " +
     //   "select col_chr as c1, col_int from (select * from `dfs.root`.`1991`) v1";

      //String query = "select col_vrchr, count(*), ABC from (select *, upper(ABC) from `dfs.root`.`1992`) v group by col_vrchr, ABC";
      //String query = "select col_vrchr, count(*), ABC from (select *, ABC from `dfs.root`.`1992`) v group by col_vrchr, ABC";
      //String query = "select col_vrchr, count(*), c from (select *, 'ABC' as c from `dfs.root`.`1992`) v group by col_vrchr, c";
      //String query = "select cast(convert_to(interests, 'JSON') as varchar(0)) as interests from cp.`complex_student.json`";
      //String query = "select * from (select * from `dfs.root`.`*`) v where dir0 = '1992'"; //todo filter -> scan example
      String query = "select * from (select * from `dfs.root`.`*`) v where upper(dir0) = '1992'";


      //String query = "select * from `dfs.root`.`*` where dir0 = '1992'";
      //String query = "select * from `dfs.root`.`*` where upper(dir0) = '1992'";
      //String query = "select *, dir0 from (select * from `dfs.root`.`*`) v where dir0 = '1992'"; //todo filter -> project -> scan example
      //String query = "select *, dir0 from `dfs.root`.`*` where dir0 = '1992'";
      //String query = "select * from `dfs.root`.`*` where dir0 = '1992'";

      //todo filter should be pushed down for better performance
      // Filter(condition=[=(ITEM($0, 'dir0'), '1992')])

      QueryBuilder viewQueryBuilder = client.queryBuilder().sql(query);
      viewQueryBuilder.printCsv();
      System.out.println(viewQueryBuilder.explainText());
    }
  }

  @Test
  public void testItemWithFilterPushDown() throws Exception {

    //note works with new rule -> DrillReWriteStarRule

    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher)
      // Easy way to run single threaded for easy debugging
      .maxParallelization(1)
      // Set some session options
      .sessionOption(ExecConstants.ENABLE_VERBOSE_ERRORS_KEY, true);


    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {

      QueryBuilder queryBuilder = client.queryBuilder();

      final String tableName = "order_ctas";
      try {
        // create table
        queryBuilder.sql("use dfs.tmp").run();
        queryBuilder.sql("create table `%s/t1` as select cast(o_orderdate as date) as o_orderdate from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-01' and " + "date " +
          "'1992-01-03'", tableName).run();
        queryBuilder.sql("create table `%s/t2` as select cast(o_orderdate as date) as o_orderdate from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-04' and " + "date " +
          "'1992-01-06'", tableName).run();
        queryBuilder.sql("create table `%s/t3` as select cast(o_orderdate as date) as o_orderdate from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-07' and " + "date " +
          "'1992-01-09'", tableName).run();

        // prepare query
        //String query = "select * from order_ctas "; // read 3 files
        //String query = "select * from order_ctas where o_orderdate = date '1992-01-01'"; // read 1 file
        //String query = "select * from order_ctas where upper(o_orderdate) = date '1992-01-01'"; // read 3 file
        //String query = "select * from order_ctas where o_orderdate = date '2007-01-01'"; // read 1 file
        String query = "select * from (select * from order_ctas) where o_orderdate = date '1992-01-01'"; // read 3 files
        //String query = "select * from (select * from order_ctas) where o_orderdate = date '1992-01-01' and ccc = 'aa'"; // read 3 files
        //String query = "select * from (select * from order_ctas) where upper(o_orderdate) = date '1992-01-01'"; // read 3 files

        // print query plan
        queryBuilder.sql(query);
        System.out.println(queryBuilder.explainText());

      } finally {
        queryBuilder.sql("drop table if exists %s", tableName).run();
      }
    }
  }

  /*
00-00    Screen
00-01      Project(col_vrchr=[$1])
00-02        Scan(groupscan=[ParquetGroupScan [entries=[ReadEntryWithPath [path=F:/drill/files/fact_dim_tables/fact/1992/alltypes_optional.parquet]], selectionRoot=file:/F:/drill/files/fact_dim_tables/fact, numFiles=1, numRowGroups=1, usedMetadataFile=false, columns=[`dir0`, `col_vrchr`]]])


-- with star
00-00    Screen
00-01      Project(*=[$0])
00-02        SelectionVectorRemover
00-03          Filter(condition=[=(ITEM($0, 'dir0'), '1992')])
00-04            Scan(groupscan=[ParquetGroupScan [entries=[ReadEntryWithPath [path=file:/F:/drill/files/fact_dim_tables/fact/1991], ReadEntryWithPath [path=file:/F:/drill/files/fact_dim_tables/fact/1992]], selectionRoot=file:/F:/drill/files/fact_dim_tables/fact, numFiles=2, numRowGroups=2, usedMetadataFile=false, columns=[`*`]]])

-- without star
00-00    Screen
00-01      Project(*=[$0])
00-02        Project(*=[$0])
00-03          Scan(groupscan=[ParquetGroupScan [entries=[ReadEntryWithPath [path=F:/drill/files/fact_dim_tables/fact/1992/alltypes_optional.parquet]], selectionRoot=file:/F:/drill/files/fact_dim_tables/fact, numFiles=1, numRowGroups=1, usedMetadataFile=false, columns=[`*`]]])

-- compare
Scan(groupscan=[ParquetGroupScan [entries=[ReadEntryWithPath [path=file:/F:/drill/files/fact_dim_tables/fact/1991], ReadEntryWithPath [path=file:/F:/drill/files/fact_dim_tables/fact/1992]], selectionRoot=file:/F:/drill/files/fact_dim_tables/fact, numFiles=2, numRowGroups=2, usedMetadataFile=false, columns=[`*`]]])
Scan(groupscan=[ParquetGroupScan [entries=[ReadEntryWithPath [path=F:/drill/files/fact_dim_tables/fact/1992/alltypes_optional.parquet]], selectionRoot=file:/F:/drill/files/fact_dim_tables/fact, numFiles=1, numRowGroups=1, usedMetadataFile=false, columns=[`*`]]])


   */

  /*
  DrillScreenRel
  DrillAggregateRel(group=[{0}], EXPR$1=[COUNT($1)])
    DrillProjectRel(col_vrchr=[$0], $f1=[1])
      DrillScanRel(table=[[dfs.root, 1992]], groupscan=[ParquetGroupScan [entries=[ReadEntryWithPath [path=file:/F:/drill/files/fact_dim_tables/fact/1992]], selectionRoot=file:/F:/drill/files/fact_dim_tables/fact/1992, numFiles=1, numRowGroups=1, usedMetadataFile=false, columns=[`col_vrchr`]]])

00-00    Screen
00-01      Project(col_vrchr=[$0], EXPR$1=[$1])
00-02        StreamAgg(group=[{0}], EXPR$1=[COUNT()])
00-03          Sort(sort0=[$0], dir0=[ASC])
00-04            Scan(groupscan=[ParquetGroupScan [entries=[ReadEntryWithPath [path=file:/F:/drill/files/fact_dim_tables/fact/1992]], selectionRoot=file:/F:/drill/files/fact_dim_tables/fact/1992, numFiles=1, numRowGroups=1, usedMetadataFile=false, columns=[`col_vrchr`]]])


DrillScreenRel
  DrillAggregateRel(group=[{0}], EXPR$1=[COUNT()])
    DrillProjectRel(col_vrchr=[ITEM($0, 'col_vrchr')])
      DrillScanRel(table=[[dfs.root, 1992]], groupscan=[ParquetGroupScan [entries=[ReadEntryWithPath [path=file:/F:/drill/files/fact_dim_tables/fact/1992]], selectionRoot=file:/F:/drill/files/fact_dim_tables/fact/1992, numFiles=1, numRowGroups=1, usedMetadataFile=false, columns=[`*`]]])

00-00    Screen
00-01      Project(col_vrchr=[$0], EXPR$1=[$1])
00-02        StreamAgg(group=[{0}], EXPR$1=[COUNT()])
00-03          Sort(sort0=[$0], dir0=[ASC])
00-04            Project(col_vrchr=[ITEM($0, 'col_vrchr')])
00-05              Scan(groupscan=[ParquetGroupScan [entries=[ReadEntryWithPath [path=file:/F:/drill/files/fact_dim_tables/fact/1992]], selectionRoot=file:/F:/drill/files/fact_dim_tables/fact/1992, numFiles=1, numRowGroups=1, usedMetadataFile=false, columns=[`*`]]])


   */

}
