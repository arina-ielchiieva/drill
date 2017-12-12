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
    String root = DrillFileUtils.getResourceAsFile("/store/text/data/regions.csv").toURI().toString();
    String query = String.format("select rid, x.name from (select columns[0] as RID, columns[1] as NAME from dfs.`%s`) X where X.rid = 2", root);
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

  @Test
  public void t() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher)
        // Easy way to run single threaded for easy debugging
        .maxParallelization(1)
        // Set some session options
        .sessionOption(ExecConstants.ENABLE_VERBOSE_ERRORS_KEY, true);

    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "root", "/", "parquet");
      String sql = "select columns[1] ,columns[2] ,columns[3] ,columns[4] ,columns[5] ,columns[6] ,columns[7] ,columns[8] ,columns[9] ,columns[10] ,columns[11] ,columns[12] ," +
          "columns[13] ,columns[14] ,columns[15] ,columns[16] ,columns[17] ,columns[18] ,columns[19] ,columns[20] ,columns[21] ,columns[22] ,columns[23] ,columns[24] ," +
          "columns[25] ,columns[26] ,columns[27] ,columns[28] ,columns[29] ,columns[30] ,columns[31] ,columns[32] ,columns[33] ,columns[34] ,columns[35] ,columns[36] ,columns[37] ,columns[38] ,columns[39] ,columns[40] ,columns[41] ,columns[42] ,columns[43] ,columns[44] ,columns[45] ,columns[46] ,columns[47] ,columns[48] ,columns[49] ,columns[50] ,columns[51] ,columns[52] ,columns[53] ,columns[54] ,columns[55] ,columns[56] ,columns[57] ,columns[58] ,columns[59] ,columns[60] ,columns[61] ,columns[62] ,columns[63] ,columns[64] ,columns[65] ,columns[66] ,columns[67] ,columns[68] ,columns[69] ,columns[70] ,columns[71] ,columns[72] ,columns[73] ,columns[74] ,columns[75] ,columns[76] ,columns[77] ,columns[78] ,columns[79] ,columns[80] ,columns[81] ,columns[82] ,columns[83] ,columns[84] ,columns[85] ,columns[86] ,columns[87] ,columns[88] ,columns[89] ,columns[90] ,columns[91] ,columns[92] ,columns[93] ,columns[94] ,columns[95] ,columns[96] ,columns[97] ,columns[98] ,columns[99] ,columns[100] ,columns[101] ,columns[102] ,columns[103] ,columns[104] ,columns[105] ,columns[106] ,columns[107] ,columns[108] ,columns[109] ,columns[110] ,columns[111] ,columns[112] ,columns[113] ,columns[114] ,columns[115] ,columns[116] ,columns[117] ,columns[118] ,columns[119] ,columns[120] ,columns[121] ,columns[122] ,columns[123] ,columns[124] ,columns[125] ,columns[126] ,columns[127] ,columns[128] ,columns[129] ,columns[130] ,columns[131] ,columns[132] ,columns[133] ,columns[134] ,columns[135] ,columns[136] ,columns[137] ,columns[138] ,columns[139] ,columns[140] ,columns[141] ,columns[142] ,columns[143] ,columns[144] ,columns[145] ,columns[146] ,columns[147] ,columns[148] ,columns[149] ,columns[150] ,columns[151] ,columns[152] ,columns[153] ,columns[154] ,columns[155] ,columns[156] ,columns[157] ,columns[158] ,columns[159] ,columns[160] ,columns[161] ,columns[162] ,columns[163] ,columns[164] ,columns[165] ,columns[166] ,columns[167] ,columns[168] ,columns[169] ,columns[170] ,columns[171] ,columns[172] ,columns[173] ,columns[174] ,columns[175] ,columns[176] ,columns[177] ,columns[178] ,columns[179] ,columns[180] ,columns[181] ,columns[182] ,columns[183] ,columns[184] ,columns[185] ,columns[186] ,columns[187] ,columns[188] ,columns[189] ,columns[190] ,columns[191] ,columns[192] ,columns[193] ,columns[194] ,columns[195] ,columns[196] ,columns[197] ,columns[198] ,columns[199] ,columns[200] ,columns[201] ,columns[202] ,columns[203] ,columns[204] ,columns[205] ,columns[206] ,columns[207] ,columns[208] ,columns[209] ,columns[210] ,columns[211] ,columns[212] ,columns[213] ,columns[214] ,columns[215] ,columns[216] ,columns[217] ,columns[218] ,columns[219] ,columns[220] ,columns[221] ,columns[222] ,columns[223] ,columns[224] ,columns[225] ,columns[226] ,columns[227] ,columns[228] ,columns[229] ,columns[230] ,columns[231] ,columns[232] ,columns[233] ,columns[234] ,columns[235] ,columns[236] ,columns[237] ,columns[238] ,columns[239] ,columns[240] ,columns[241] ,columns[242] ,columns[243] ,columns[244] ,columns[245] ,columns[246] ,columns[247] ,columns[248] ,columns[249] ,columns[250] ,columns[251] ,columns[252] ,columns[253] ,columns[254] ,columns[255] ,columns[256] ,columns[257] ,columns[258] ,columns[259] ,columns[260] ,columns[261] ,columns[262] ,columns[263] ,columns[264] ,columns[265] ,columns[266] ,columns[267] ,columns[268] ,columns[269] ,columns[270] ,columns[271] ,columns[272] ,columns[273] ,columns[274] ,columns[275] ,columns[276] ,columns[277] ,columns[278] ,columns[279] ,columns[280] ,columns[281] ,columns[282] ,columns[283] ,columns[284] ,columns[285] ,columns[286] ,columns[287] ,columns[288] ,columns[289] ,columns[290] ,columns[291] ,columns[292] ,columns[293] ,columns[294] ,columns[295] ,columns[296] ,columns[297] ,columns[298] ,columns[299] ,columns[300] ,columns[301] ,columns[302] ,columns[303] ,columns[304] ,columns[305] ,columns[306] ,columns[307] ,columns[308] ,columns[309] ,columns[310] ,columns[311] ,columns[312] ,columns[313] ,columns[314] ,columns[315] ,columns[316] ,columns[317] ,columns[318] ,columns[319] ,columns[320] ,columns[321] ,columns[322] ,columns[323] ,columns[324] ,columns[325] ,columns[326] ,columns[327] ,columns[328] ,columns[329] ,columns[330] ,columns[331] ,columns[332] ,columns[333] ,columns[334] ,columns[335] ,columns[336] ,columns[337] ,columns[338] ,columns[339] ,columns[340] ,columns[341] ,columns[342] ,columns[343] ,columns[344] ,columns[345] ,columns[346] ,columns[347] ,columns[348] ,columns[349] ,columns[350] ,columns[351] ,columns[352] ,columns[353] ,columns[354] ,columns[355] ,columns[356] ,columns[357] ,columns[358] ,columns[359] ,columns[360] ,columns[361] ,columns[362] ,columns[363] ,columns[364] ,columns[365] ,columns[366] ,columns[367] ,columns[368] ,columns[369] ,columns[370] ,columns[371] ,columns[372] ,columns[373] ,columns[374] ,columns[375] ,columns[376] ,columns[377] ,columns[378] ,columns[379] ,columns[380] ,columns[381] ,columns[382] ,columns[383] ,columns[384] ,columns[385] ,columns[386] ,columns[387] ,columns[388] ,columns[389] ,columns[390] ,columns[391] ,columns[392] ,columns[393] ,columns[394] ,columns[395] ,columns[396] ,columns[397] ,columns[398] ,columns[399] ,columns[400] ,columns[401] ,columns[402] ,columns[403] ,columns[404] ,columns[405] ,columns[406] ,columns[407] ,columns[408] ,columns[409] ,columns[410] ,columns[411] ,columns[412] ,columns[413] ,columns[414] ,columns[415] ,columns[416] ,columns[417] ,columns[418] ,columns[419] ,columns[420] ,columns[421] ,columns[422] ,columns[423] ,columns[424] ,columns[425] ,columns[426] ,columns[427] ,columns[428] ,columns[429] ,columns[430] ,columns[431] ,columns[432] ,columns[433] ,columns[434] ,columns[435] ,columns[436] ,columns[437] ,columns[438] ,columns[439] ,columns[440] ,columns[441] ,columns[442] ,columns[443] ,columns[444] ,columns[445] ,columns[446] ,columns[447] ,columns[448] ,columns[449] ,columns[450] ,columns[451] ,columns[452] ,columns[453] ,columns[454] ,columns[455] ,columns[456] ,columns[457] ,columns[458] ,columns[459] ,columns[460] ,columns[461] ,columns[462] ,columns[463] ,columns[464] ,columns[465] ,columns[466] ,columns[467] ,columns[468] ,columns[469] ,columns[470] ,columns[471] ,columns[472] ,columns[473] ,columns[474] ,columns[475] ,columns[476] ,columns[477] ,columns[478] ,columns[479] ,columns[480] ,columns[481] ,columns[482] ,columns[483] ,columns[484] ,columns[485] ,columns[486] ,columns[487] ,columns[488] ,columns[489] ,columns[490] ,columns[491] ,columns[492] ,columns[493] ,columns[494] ,columns[495] ,columns[496] ,columns[497] ,columns[498] ,columns[499] ,columns[500] ,columns[501] ,columns[502] ,columns[503] ,columns[504] ,columns[505] ,columns[506] ,columns[507] ,columns[508] ,columns[509] ,columns[510] ,columns[511] ,columns[512] ,columns[513] ,columns[514] ,columns[515] ,columns[516] ,columns[517] ,columns[518] ,columns[519] ,columns[520] ,columns[521] ,columns[522] ,columns[523] ,columns[524] ,columns[525] ,columns[526] ,columns[527] ,columns[528] ,columns[529] ,columns[530] ,columns[531] ,columns[532] ,columns[533] ,columns[534] ,columns[535] ,columns[536] ,columns[537] ,columns[538] ,columns[539] ,columns[540] ,columns[541] ,columns[542] ,columns[543] ,columns[544] ,columns[545] ,columns[546] ,columns[547] ,columns[548] ,columns[549] ,columns[550] ,columns[551] ,columns[552] ,columns[553] ,columns[554] ,columns[555] ,columns[556] ,columns[557] ,columns[558] ,columns[559] ,columns[560] ,columns[561] ,columns[562] ,columns[563] ,columns[564] ,columns[565] ,columns[566] ,columns[567] ,columns[568] ,columns[569] ,columns[570] ,columns[571] ,columns[572] ,columns[573] ,columns[574] ,columns[575] ,columns[576] ,columns[577] ,columns[578] ,columns[579] ,columns[580] ,columns[581] ,columns[582] ,columns[583] ,columns[584] ,columns[585] ,columns[586] ,columns[587] ,columns[588] ,columns[589] ,columns[590] ,columns[591] ,columns[592] ,columns[593] ,columns[594] ,columns[595] ,columns[596] ,columns[597] ,columns[598] ,columns[599] ,columns[600] ,columns[601] ,columns[602] ,columns[603] ,columns[604] ,columns[605] ,columns[606] ,columns[607] ,columns[608] ,columns[609] ,columns[610] ,columns[611] ,columns[612] ,columns[613] ,columns[614] ,columns[615] ,columns[616] ,columns[617] ,columns[618] ,columns[619] ,columns[620] ,columns[621] ,columns[622] ,columns[623] ,columns[624] ,columns[625] ,columns[626] ,columns[627] ,columns[628] ,columns[629] ,columns[630] ,columns[631] ,columns[632] ,columns[633] ,columns[634] ,columns[635] ,columns[636] ,columns[637] ,columns[638] ,columns[639] ,columns[640] ,columns[641] ,columns[642] ,columns[643] ,columns[644] ,columns[645] ,columns[646] ,columns[647] ,columns[648] ,columns[649] ,columns[650] ,columns[651] ,columns[652] ,columns[653] ,columns[654] ,columns[655] ,columns[656] ,columns[657] ,columns[658] ,columns[659] ,columns[660] ,columns[661] ,columns[662] ,columns[663] ,columns[664] ,columns[665] ,columns[666] ,columns[667] ,columns[668] ,columns[669] ,columns[670] ,columns[671] ,columns[672] ,columns[673] ,columns[674] ,columns[675] ,columns[676] ,columns[677] ,columns[678] ,columns[679] ,columns[680] ,columns[681] ,columns[682] ,columns[683] ,columns[684] ,columns[685] ,columns[686] ,columns[687] ,columns[688] ,columns[689] ,columns[690] ,columns[691] ,columns[692] ,columns[693] ,columns[694] ,columns[695] ,columns[696] ,columns[697] ,columns[698] ,columns[699] ,columns[700] ,columns[701] ,columns[702] ,columns[703] ,columns[704] ,columns[705] ,columns[706] ,columns[707] ,columns[708] ,columns[709] ,columns[710] ,columns[711] ,columns[712] ,columns[713] ,columns[714] ,columns[715] ,columns[716] ,columns[717] ,columns[718] ,columns[719] ,columns[720] ,columns[721] ,columns[722] ,columns[723] ,columns[724] ,columns[725] ,columns[726] ,columns[727] ,columns[728] ,columns[729] ,columns[730] ,columns[731] ,columns[732] ,columns[733] ,columns[734] ,columns[735] ,columns[736] ,columns[737] ,columns[738] ,columns[739] ,columns[740] ,columns[741] ,columns[742] ,columns[743] ,columns[744] ,columns[745] ,columns[746] ,columns[747] ,columns[748] ,columns[749] ,columns[750] ,columns[751] ,columns[752] ,columns[753] ,columns[754] ,columns[755] ,columns[756] ,columns[757] ,columns[758] ,columns[759] ,columns[760] ,columns[761] ,columns[762] ,columns[763] ,columns[764] ,columns[765] ,columns[766] ,columns[767] ,columns[768] ,columns[769] ,columns[770] ,columns[771] ,columns[772] ,columns[773] ,columns[774] ,columns[775] ,columns[776] ,columns[777] ,columns[778] ,columns[779] ,columns[780] ,columns[781] ,columns[782] ,columns[783] ,columns[784] ,columns[785] ,columns[786] ,columns[787] ,columns[788] ,columns[789] ,columns[790] ,columns[791] ,columns[792] ,columns[793] ,columns[794] ,columns[795] ,columns[796] ,columns[797] ,columns[798] ,columns[799] ,columns[800] ,columns[801] ,columns[802] ,columns[803] ,columns[804] ,columns[805] ,columns[806] ,columns[807] ,columns[808] ,columns[809] ,columns[810] ,columns[811] ,columns[812] ,columns[813] ,columns[814] ,columns[815] ,columns[816] ,columns[817] ,columns[818] ,columns[819] ,columns[820] ,columns[821] ,columns[822] ,columns[823] ,columns[824] ,columns[825] ,columns[826] ,columns[827] ,columns[828] ,columns[829] ,columns[830] ,columns[831] ,columns[832] ,columns[833] ,columns[834] ,columns[835] ,columns[836] ,columns[837] ,columns[838] ,columns[839] ,columns[840] ,columns[841] ,columns[842] ,columns[843] ,columns[844] ,columns[845] ,columns[846] ,columns[847] ,columns[848] ,columns[849] ,columns[850] ,columns[851] ,columns[852] ,columns[853] ,columns[854] ,columns[855] ,columns[856] ,columns[857] ,columns[858] ,columns[859] ,columns[860] ,columns[861] ,columns[862] ,columns[863] ,columns[864] ,columns[865] ,columns[866] ,columns[867] ,columns[868] ,columns[869] ,columns[870] ,columns[871] ,columns[872] ,columns[873] ,columns[874] ,columns[875] ,columns[876] ,columns[877] ,columns[878] ,columns[879] ,columns[880] ,columns[881] ,columns[882] ,columns[883] ,columns[884] ,columns[885] ,columns[886] ,columns[887] ,columns[888] ,columns[889] ,columns[890] ,columns[891] ,columns[892] ,columns[893] ,columns[894] ,columns[895] ,columns[896] ,columns[897] ,columns[898] ,columns[899] ,columns[900] ,columns[901] ,columns[902] ,columns[903] ,columns[904] ,columns[905] ,columns[906] ,columns[907] ,columns[908] ,columns[909] ,columns[910] ,columns[911] ,columns[912] ,columns[913] ,columns[914] ,columns[915] ,columns[916] ,columns[917] ,columns[918] ,columns[919] ,columns[920] ,columns[921] ,columns[922] ,columns[923] ,columns[924] ,columns[925] ,columns[926] ,columns[927] ,columns[928] ,columns[929] ,columns[930] ,columns[931] ,columns[932] ,columns[933] ,columns[934] ,columns[935] ,columns[936] ,columns[937] ,columns[938] ,columns[939] ,columns[940] ,columns[941] ,columns[942] ,columns[943] ,columns[944] ,columns[945] ,columns[946] ,columns[947] ,columns[948] ,columns[949] ,columns[950] ,columns[951] ,columns[952] ,columns[953] ,columns[954] ,columns[955] ,columns[956] ,columns[957] ,columns[958] ,columns[959] ,columns[960] ,columns[961] ,columns[962] ,columns[963] ,columns[964] ,columns[965] ,columns[966] ,columns[967] ,columns[968] ,columns[969] ,columns[970] ,columns[971] ,columns[972] ,columns[973] ,columns[974] ,columns[975] ,columns[976] ,columns[977] ,columns[978] ,columns[979] ,columns[980] ,columns[981] ,columns[982] ,columns[983] ,columns[984] ,columns[985] ,columns[986] ,columns[987] ,columns[988] ,columns[989] ,columns[990] ,columns[991] ,columns[992] ,columns[993] ,columns[994] ,columns[995] ,columns[996] ,columns[997] ,columns[998] ,columns[999] ,columns[1000] ,columns[1001] ,columns[1002] ,columns[1003] ,columns[1004] ,columns[1005] ,columns[1006] ,columns[1007] ,columns[1008] ,columns[1009] ,columns[1010] ,columns[1011] ,columns[1012] ,columns[1013] ,columns[1014] ,columns[1015] ,columns[1016] ,columns[1017] ,columns[1018] ,columns[1019] ,columns[1020] ,columns[1021] ,columns[1022] ,columns[1023] ,columns[1024] ,columns[1025] ,columns[1026] ,columns[1027] ,columns[1028] ,columns[1029] ,columns[1030] ,columns[1031] ,columns[1032] ,columns[1033] ,columns[1034] ,columns[1035] ,columns[1036] ,columns[1037] ,columns[1038] ,columns[1039] ,columns[1040] ,columns[1041] ,columns[1042] ,columns[1043] ,columns[1044] ,columns[1045] ,columns[1046] ,columns[1047] ,columns[1048] ,columns[1049] ,columns[1050] ,columns[1051] ,columns[1052] ,columns[1053] ,columns[1054] ,columns[1055] ,columns[1056] ,columns[1057] ,columns[1058] ,columns[1059] ,columns[1060] ,columns[1061] ,columns[1062] ,columns[1063] ,columns[1064] ,columns[1065] ,columns[1066] ,columns[1067] ,columns[1068] ,columns[1069] ,columns[1070] ,columns[1071] ,columns[1072] ,columns[1073] ,columns[1074] ,columns[1075] ,columns[1076] ,columns[1077] ,columns[1078] ,columns[1079] ,columns[1080] ,columns[1081] ,columns[1082] ,columns[1083] ,columns[1084] ,columns[1085] ,columns[1086] ,columns[1087] ,columns[1088] ,columns[1089] ,columns[1090] ,columns[1091] ,columns[1092] ,columns[1093] ,columns[1094] ,columns[1095] ,columns[1096] ,columns[1097] ,columns[1098] ,columns[1099] ,columns[1100] ,columns[1101] ,columns[1102] ,columns[1103] ,columns[1104] ,columns[1105] ,columns[1106] ,columns[1107] ,columns[1108] ,columns[1109] ,columns[1110] ,columns[1111] ,columns[1112] ,columns[1113] ,columns[1114] ,columns[1115] ,columns[1116] ,columns[1117] ,columns[1118] ,columns[1119] ,columns[1120] ,columns[1121] ,columns[1122] ,columns[1123] ,columns[1124] ,columns[1125] ,columns[1126] ,columns[1127] ,columns[1128] ,columns[1129] ,columns[1130] ,columns[1131] ,columns[1132] ,columns[1133] ,columns[1134] ,columns[1135] ,columns[1136] ,columns[1137] ,columns[1138] ,columns[1139] ,columns[1140] ,columns[1141] ,columns[1142] ,columns[1143] ,columns[1144] ,columns[1145] ,columns[1146] ,columns[1147] ,columns[1148] ,columns[1149] ,columns[1150] ,columns[1151] ,columns[1152] ,columns[1153] ,columns[1154] ,columns[1155] ,columns[1156] ,columns[1157] ,columns[1158] ,columns[1159] ,columns[1160] ,columns[1161] ,columns[1162] ,columns[1163] ,columns[1164] ,columns[1165] ,columns[1166] ,columns[1167] ,columns[1168] ,columns[1169] ,columns[1170] ,columns[1171] ,columns[1172] ,columns[1173] ,columns[1174] ,columns[1175] ,columns[1176] ,columns[1177] ,columns[1178] ,columns[1179] ,columns[1180] ,columns[1181] ,columns[1182] ,columns[1183] ,columns[1184] ,columns[1185] ,columns[1186] ,columns[1187] ,columns[1188] ,columns[1189] ,columns[1190] ,columns[1191] ,columns[1192] ,columns[1193] ,columns[1194] ,columns[1195] ,columns[1196] ,columns[1197] ,columns[1198] ,columns[1199] ,columns[1200] from dfs.`F:\\drill\\files\\3.csv`" +
          "union all\n" +
          "select columns[1] ,columns[2] ,columns[3] ,columns[4] ,columns[5] ,columns[6] ,columns[7] ,columns[8] ,columns[9] ,columns[10] ,columns[11] ,columns[12] ,columns[13] " +
          ",columns[14] ,columns[15] ,columns[16] ,columns[17] ,columns[18] ,columns[19] ,columns[20] ,columns[21] ,columns[22] ,columns[23] ,columns[24] ,columns[25] ," +
          "columns[26] ,columns[27] ,columns[28] ,columns[29] ,columns[30] ,columns[31] ,columns[32] ,columns[33] ,columns[34] ,columns[35] ,columns[36] ,columns[37] ,columns[38] ,columns[39] ,columns[40] ,columns[41] ,columns[42] ,columns[43] ,columns[44] ,columns[45] ,columns[46] ,columns[47] ,columns[48] ,columns[49] ,columns[50] ,columns[51] ,columns[52] ,columns[53] ,columns[54] ,columns[55] ,columns[56] ,columns[57] ,columns[58] ,columns[59] ,columns[60] ,columns[61] ,columns[62] ,columns[63] ,columns[64] ,columns[65] ,columns[66] ,columns[67] ,columns[68] ,columns[69] ,columns[70] ,columns[71] ,columns[72] ,columns[73] ,columns[74] ,columns[75] ,columns[76] ,columns[77] ,columns[78] ,columns[79] ,columns[80] ,columns[81] ,columns[82] ,columns[83] ,columns[84] ,columns[85] ,columns[86] ,columns[87] ,columns[88] ,columns[89] ,columns[90] ,columns[91] ,columns[92] ,columns[93] ,columns[94] ,columns[95] ,columns[96] ,columns[97] ,columns[98] ,columns[99] ,columns[100] ,columns[101] ,columns[102] ,columns[103] ,columns[104] ,columns[105] ,columns[106] ,columns[107] ,columns[108] ,columns[109] ,columns[110] ,columns[111] ,columns[112] ,columns[113] ,columns[114] ,columns[115] ,columns[116] ,columns[117] ,columns[118] ,columns[119] ,columns[120] ,columns[121] ,columns[122] ,columns[123] ,columns[124] ,columns[125] ,columns[126] ,columns[127] ,columns[128] ,columns[129] ,columns[130] ,columns[131] ,columns[132] ,columns[133] ,columns[134] ,columns[135] ,columns[136] ,columns[137] ,columns[138] ,columns[139] ,columns[140] ,columns[141] ,columns[142] ,columns[143] ,columns[144] ,columns[145] ,columns[146] ,columns[147] ,columns[148] ,columns[149] ,columns[150] ,columns[151] ,columns[152] ,columns[153] ,columns[154] ,columns[155] ,columns[156] ,columns[157] ,columns[158] ,columns[159] ,columns[160] ,columns[161] ,columns[162] ,columns[163] ,columns[164] ,columns[165] ,columns[166] ,columns[167] ,columns[168] ,columns[169] ,columns[170] ,columns[171] ,columns[172] ,columns[173] ,columns[174] ,columns[175] ,columns[176] ,columns[177] ,columns[178] ,columns[179] ,columns[180] ,columns[181] ,columns[182] ,columns[183] ,columns[184] ,columns[185] ,columns[186] ,columns[187] ,columns[188] ,columns[189] ,columns[190] ,columns[191] ,columns[192] ,columns[193] ,columns[194] ,columns[195] ,columns[196] ,columns[197] ,columns[198] ,columns[199] ,columns[200] ,columns[201] ,columns[202] ,columns[203] ,columns[204] ,columns[205] ,columns[206] ,columns[207] ,columns[208] ,columns[209] ,columns[210] ,columns[211] ,columns[212] ,columns[213] ,columns[214] ,columns[215] ,columns[216] ,columns[217] ,columns[218] ,columns[219] ,columns[220] ,columns[221] ,columns[222] ,columns[223] ,columns[224] ,columns[225] ,columns[226] ,columns[227] ,columns[228] ,columns[229] ,columns[230] ,columns[231] ,columns[232] ,columns[233] ,columns[234] ,columns[235] ,columns[236] ,columns[237] ,columns[238] ,columns[239] ,columns[240] ,columns[241] ,columns[242] ,columns[243] ,columns[244] ,columns[245] ,columns[246] ,columns[247] ,columns[248] ,columns[249] ,columns[250] ,columns[251] ,columns[252] ,columns[253] ,columns[254] ,columns[255] ,columns[256] ,columns[257] ,columns[258] ,columns[259] ,columns[260] ,columns[261] ,columns[262] ,columns[263] ,columns[264] ,columns[265] ,columns[266] ,columns[267] ,columns[268] ,columns[269] ,columns[270] ,columns[271] ,columns[272] ,columns[273] ,columns[274] ,columns[275] ,columns[276] ,columns[277] ,columns[278] ,columns[279] ,columns[280] ,columns[281] ,columns[282] ,columns[283] ,columns[284] ,columns[285] ,columns[286] ,columns[287] ,columns[288] ,columns[289] ,columns[290] ,columns[291] ,columns[292] ,columns[293] ,columns[294] ,columns[295] ,columns[296] ,columns[297] ,columns[298] ,columns[299] ,columns[300] ,columns[301] ,columns[302] ,columns[303] ,columns[304] ,columns[305] ,columns[306] ,columns[307] ,columns[308] ,columns[309] ,columns[310] ,columns[311] ,columns[312] ,columns[313] ,columns[314] ,columns[315] ,columns[316] ,columns[317] ,columns[318] ,columns[319] ,columns[320] ,columns[321] ,columns[322] ,columns[323] ,columns[324] ,columns[325] ,columns[326] ,columns[327] ,columns[328] ,columns[329] ,columns[330] ,columns[331] ,columns[332] ,columns[333] ,columns[334] ,columns[335] ,columns[336] ,columns[337] ,columns[338] ,columns[339] ,columns[340] ,columns[341] ,columns[342] ,columns[343] ,columns[344] ,columns[345] ,columns[346] ,columns[347] ,columns[348] ,columns[349] ,columns[350] ,columns[351] ,columns[352] ,columns[353] ,columns[354] ,columns[355] ,columns[356] ,columns[357] ,columns[358] ,columns[359] ,columns[360] ,columns[361] ,columns[362] ,columns[363] ,columns[364] ,columns[365] ,columns[366] ,columns[367] ,columns[368] ,columns[369] ,columns[370] ,columns[371] ,columns[372] ,columns[373] ,columns[374] ,columns[375] ,columns[376] ,columns[377] ,columns[378] ,columns[379] ,columns[380] ,columns[381] ,columns[382] ,columns[383] ,columns[384] ,columns[385] ,columns[386] ,columns[387] ,columns[388] ,columns[389] ,columns[390] ,columns[391] ,columns[392] ,columns[393] ,columns[394] ,columns[395] ,columns[396] ,columns[397] ,columns[398] ,columns[399] ,columns[400] ,columns[401] ,columns[402] ,columns[403] ,columns[404] ,columns[405] ,columns[406] ,columns[407] ,columns[408] ,columns[409] ,columns[410] ,columns[411] ,columns[412] ,columns[413] ,columns[414] ,columns[415] ,columns[416] ,columns[417] ,columns[418] ,columns[419] ,columns[420] ,columns[421] ,columns[422] ,columns[423] ,columns[424] ,columns[425] ,columns[426] ,columns[427] ,columns[428] ,columns[429] ,columns[430] ,columns[431] ,columns[432] ,columns[433] ,columns[434] ,columns[435] ,columns[436] ,columns[437] ,columns[438] ,columns[439] ,columns[440] ,columns[441] ,columns[442] ,columns[443] ,columns[444] ,columns[445] ,columns[446] ,columns[447] ,columns[448] ,columns[449] ,columns[450] ,columns[451] ,columns[452] ,columns[453] ,columns[454] ,columns[455] ,columns[456] ,columns[457] ,columns[458] ,columns[459] ,columns[460] ,columns[461] ,columns[462] ,columns[463] ,columns[464] ,columns[465] ,columns[466] ,columns[467] ,columns[468] ,columns[469] ,columns[470] ,columns[471] ,columns[472] ,columns[473] ,columns[474] ,columns[475] ,columns[476] ,columns[477] ,columns[478] ,columns[479] ,columns[480] ,columns[481] ,columns[482] ,columns[483] ,columns[484] ,columns[485] ,columns[486] ,columns[487] ,columns[488] ,columns[489] ,columns[490] ,columns[491] ,columns[492] ,columns[493] ,columns[494] ,columns[495] ,columns[496] ,columns[497] ,columns[498] ,columns[499] ,columns[500] ,columns[501] ,columns[502] ,columns[503] ,columns[504] ,columns[505] ,columns[506] ,columns[507] ,columns[508] ,columns[509] ,columns[510] ,columns[511] ,columns[512] ,columns[513] ,columns[514] ,columns[515] ,columns[516] ,columns[517] ,columns[518] ,columns[519] ,columns[520] ,columns[521] ,columns[522] ,columns[523] ,columns[524] ,columns[525] ,columns[526] ,columns[527] ,columns[528] ,columns[529] ,columns[530] ,columns[531] ,columns[532] ,columns[533] ,columns[534] ,columns[535] ,columns[536] ,columns[537] ,columns[538] ,columns[539] ,columns[540] ,columns[541] ,columns[542] ,columns[543] ,columns[544] ,columns[545] ,columns[546] ,columns[547] ,columns[548] ,columns[549] ,columns[550] ,columns[551] ,columns[552] ,columns[553] ,columns[554] ,columns[555] ,columns[556] ,columns[557] ,columns[558] ,columns[559] ,columns[560] ,columns[561] ,columns[562] ,columns[563] ,columns[564] ,columns[565] ,columns[566] ,columns[567] ,columns[568] ,columns[569] ,columns[570] ,columns[571] ,columns[572] ,columns[573] ,columns[574] ,columns[575] ,columns[576] ,columns[577] ,columns[578] ,columns[579] ,columns[580] ,columns[581] ,columns[582] ,columns[583] ,columns[584] ,columns[585] ,columns[586] ,columns[587] ,columns[588] ,columns[589] ,columns[590] ,columns[591] ,columns[592] ,columns[593] ,columns[594] ,columns[595] ,columns[596] ,columns[597] ,columns[598] ,columns[599] ,columns[600] ,columns[601] ,columns[602] ,columns[603] ,columns[604] ,columns[605] ,columns[606] ,columns[607] ,columns[608] ,columns[609] ,columns[610] ,columns[611] ,columns[612] ,columns[613] ,columns[614] ,columns[615] ,columns[616] ,columns[617] ,columns[618] ,columns[619] ,columns[620] ,columns[621] ,columns[622] ,columns[623] ,columns[624] ,columns[625] ,columns[626] ,columns[627] ,columns[628] ,columns[629] ,columns[630] ,columns[631] ,columns[632] ,columns[633] ,columns[634] ,columns[635] ,columns[636] ,columns[637] ,columns[638] ,columns[639] ,columns[640] ,columns[641] ,columns[642] ,columns[643] ,columns[644] ,columns[645] ,columns[646] ,columns[647] ,columns[648] ,columns[649] ,columns[650] ,columns[651] ,columns[652] ,columns[653] ,columns[654] ,columns[655] ,columns[656] ,columns[657] ,columns[658] ,columns[659] ,columns[660] ,columns[661] ,columns[662] ,columns[663] ,columns[664] ,columns[665] ,columns[666] ,columns[667] ,columns[668] ,columns[669] ,columns[670] ,columns[671] ,columns[672] ,columns[673] ,columns[674] ,columns[675] ,columns[676] ,columns[677] ,columns[678] ,columns[679] ,columns[680] ,columns[681] ,columns[682] ,columns[683] ,columns[684] ,columns[685] ,columns[686] ,columns[687] ,columns[688] ,columns[689] ,columns[690] ,columns[691] ,columns[692] ,columns[693] ,columns[694] ,columns[695] ,columns[696] ,columns[697] ,columns[698] ,columns[699] ,columns[700] ,columns[701] ,columns[702] ,columns[703] ,columns[704] ,columns[705] ,columns[706] ,columns[707] ,columns[708] ,columns[709] ,columns[710] ,columns[711] ,columns[712] ,columns[713] ,columns[714] ,columns[715] ,columns[716] ,columns[717] ,columns[718] ,columns[719] ,columns[720] ,columns[721] ,columns[722] ,columns[723] ,columns[724] ,columns[725] ,columns[726] ,columns[727] ,columns[728] ,columns[729] ,columns[730] ,columns[731] ,columns[732] ,columns[733] ,columns[734] ,columns[735] ,columns[736] ,columns[737] ,columns[738] ,columns[739] ,columns[740] ,columns[741] ,columns[742] ,columns[743] ,columns[744] ,columns[745] ,columns[746] ,columns[747] ,columns[748] ,columns[749] ,columns[750] ,columns[751] ,columns[752] ,columns[753] ,columns[754] ,columns[755] ,columns[756] ,columns[757] ,columns[758] ,columns[759] ,columns[760] ,columns[761] ,columns[762] ,columns[763] ,columns[764] ,columns[765] ,columns[766] ,columns[767] ,columns[768] ,columns[769] ,columns[770] ,columns[771] ,columns[772] ,columns[773] ,columns[774] ,columns[775] ,columns[776] ,columns[777] ,columns[778] ,columns[779] ,columns[780] ,columns[781] ,columns[782] ,columns[783] ,columns[784] ,columns[785] ,columns[786] ,columns[787] ,columns[788] ,columns[789] ,columns[790] ,columns[791] ,columns[792] ,columns[793] ,columns[794] ,columns[795] ,columns[796] ,columns[797] ,columns[798] ,columns[799] ,columns[800] ,columns[801] ,columns[802] ,columns[803] ,columns[804] ,columns[805] ,columns[806] ,columns[807] ,columns[808] ,columns[809] ,columns[810] ,columns[811] ,columns[812] ,columns[813] ,columns[814] ,columns[815] ,columns[816] ,columns[817] ,columns[818] ,columns[819] ,columns[820] ,columns[821] ,columns[822] ,columns[823] ,columns[824] ,columns[825] ,columns[826] ,columns[827] ,columns[828] ,columns[829] ,columns[830] ,columns[831] ,columns[832] ,columns[833] ,columns[834] ,columns[835] ,columns[836] ,columns[837] ,columns[838] ,columns[839] ,columns[840] ,columns[841] ,columns[842] ,columns[843] ,columns[844] ,columns[845] ,columns[846] ,columns[847] ,columns[848] ,columns[849] ,columns[850] ,columns[851] ,columns[852] ,columns[853] ,columns[854] ,columns[855] ,columns[856] ,columns[857] ,columns[858] ,columns[859] ,columns[860] ,columns[861] ,columns[862] ,columns[863] ,columns[864] ,columns[865] ,columns[866] ,columns[867] ,columns[868] ,columns[869] ,columns[870] ,columns[871] ,columns[872] ,columns[873] ,columns[874] ,columns[875] ,columns[876] ,columns[877] ,columns[878] ,columns[879] ,columns[880] ,columns[881] ,columns[882] ,columns[883] ,columns[884] ,columns[885] ,columns[886] ,columns[887] ,columns[888] ,columns[889] ,columns[890] ,columns[891] ,columns[892] ,columns[893] ,columns[894] ,columns[895] ,columns[896] ,columns[897] ,columns[898] ,columns[899] ,columns[900] ,columns[901] ,columns[902] ,columns[903] ,columns[904] ,columns[905] ,columns[906] ,columns[907] ,columns[908] ,columns[909] ,columns[910] ,columns[911] ,columns[912] ,columns[913] ,columns[914] ,columns[915] ,columns[916] ,columns[917] ,columns[918] ,columns[919] ,columns[920] ,columns[921] ,columns[922] ,columns[923] ,columns[924] ,columns[925] ,columns[926] ,columns[927] ,columns[928] ,columns[929] ,columns[930] ,columns[931] ,columns[932] ,columns[933] ,columns[934] ,columns[935] ,columns[936] ,columns[937] ,columns[938] ,columns[939] ,columns[940] ,columns[941] ,columns[942] ,columns[943] ,columns[944] ,columns[945] ,columns[946] ,columns[947] ,columns[948] ,columns[949] ,columns[950] ,columns[951] ,columns[952] ,columns[953] ,columns[954] ,columns[955] ,columns[956] ,columns[957] ,columns[958] ,columns[959] ,columns[960] ,columns[961] ,columns[962] ,columns[963] ,columns[964] ,columns[965] ,columns[966] ,columns[967] ,columns[968] ,columns[969] ,columns[970] ,columns[971] ,columns[972] ,columns[973] ,columns[974] ,columns[975] ,columns[976] ,columns[977] ,columns[978] ,columns[979] ,columns[980] ,columns[981] ,columns[982] ,columns[983] ,columns[984] ,columns[985] ,columns[986] ,columns[987] ,columns[988] ,columns[989] ,columns[990] ,columns[991] ,columns[992] ,columns[993] ,columns[994] ,columns[995] ,columns[996] ,columns[997] ,columns[998] ,columns[999] ,columns[1000] ,columns[1001] ,columns[1002] ,columns[1003] ,columns[1004] ,columns[1005] ,columns[1006] ,columns[1007] ,columns[1008] ,columns[1009] ,columns[1010] ,columns[1011] ,columns[1012] ,columns[1013] ,columns[1014] ,columns[1015] ,columns[1016] ,columns[1017] ,columns[1018] ,columns[1019] ,columns[1020] ,columns[1021] ,columns[1022] ,columns[1023] ,columns[1024] ,columns[1025] ,columns[1026] ,columns[1027] ,columns[1028] ,columns[1029] ,columns[1030] ,columns[1031] ,columns[1032] ,columns[1033] ,columns[1034] ,columns[1035] ,columns[1036] ,columns[1037] ,columns[1038] ,columns[1039] ,columns[1040] ,columns[1041] ,columns[1042] ,columns[1043] ,columns[1044] ,columns[1045] ,columns[1046] ,columns[1047] ,columns[1048] ,columns[1049] ,columns[1050] ,columns[1051] ,columns[1052] ,columns[1053] ,columns[1054] ,columns[1055] ,columns[1056] ,columns[1057] ,columns[1058] ,columns[1059] ,columns[1060] ,columns[1061] ,columns[1062] ,columns[1063] ,columns[1064] ,columns[1065] ,columns[1066] ,columns[1067] ,columns[1068] ,columns[1069] ,columns[1070] ,columns[1071] ,columns[1072] ,columns[1073] ,columns[1074] ,columns[1075] ,columns[1076] ,columns[1077] ,columns[1078] ,columns[1079] ,columns[1080] ,columns[1081] ,columns[1082] ,columns[1083] ,columns[1084] ,columns[1085] ,columns[1086] ,columns[1087] ,columns[1088] ,columns[1089] ,columns[1090] ,columns[1091] ,columns[1092] ,columns[1093] ,columns[1094] ,columns[1095] ,columns[1096] ,columns[1097] ,columns[1098] ,columns[1099] ,columns[1100] ,columns[1101] ,columns[1102] ,columns[1103] ,columns[1104] ,columns[1105] ,columns[1106] ,columns[1107] ,columns[1108] ,columns[1109] ,columns[1110] ,columns[1111] ,columns[1112] ,columns[1113] ,columns[1114] ,columns[1115] ,columns[1116] ,columns[1117] ,columns[1118] ,columns[1119] ,columns[1120] ,columns[1121] ,columns[1122] ,columns[1123] ,columns[1124] ,columns[1125] ,columns[1126] ,columns[1127] ,columns[1128] ,columns[1129] ,columns[1130] ,columns[1131] ,columns[1132] ,columns[1133] ,columns[1134] ,columns[1135] ,columns[1136] ,columns[1137] ,columns[1138] ,columns[1139] ,columns[1140] ,columns[1141] ,columns[1142] ,columns[1143] ,columns[1144] ,columns[1145] ,columns[1146] ,columns[1147] ,columns[1148] ,columns[1149] ,columns[1150] ,columns[1151] ,columns[1152] ,columns[1153] ,columns[1154] ,columns[1155] ,columns[1156] ,columns[1157] ,columns[1158] ,columns[1159] ,columns[1160] ,columns[1161] ,columns[1162] ,columns[1163] ,columns[1164] ,columns[1165] ,columns[1166] ,columns[1167] ,columns[1168] ,columns[1169] ,columns[1170] ,columns[1171] ,columns[1172] ,columns[1173] ,columns[1174] ,columns[1175] ,columns[1176] ,columns[1177] ,columns[1178] ,columns[1179] ,columns[1180] ,columns[1181] ,columns[1182] ,columns[1183] ,columns[1184] ,columns[1185] ,columns[1186] ,columns[1187] ,columns[1188] ,columns[1189] ,columns[1190] ,columns[1191] ,columns[1192] ,columns[1193] ,columns[1194] ,columns[1195] ,columns[1196] ,columns[1197] ,columns[1198] ,columns[1199] ,columns[1200] from dfs.`F:\\drill\\files\\3.csv`";
      client.queryBuilder().sql(sql).printCsv();
    }
  }

  @Test
  public void ttt() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher)
            // Easy way to run single threaded for easy debugging
            .maxParallelization(1)
            // Set some session options
            .sessionOption(ExecConstants.ENABLE_VERBOSE_ERRORS_KEY, true);

    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "root", "/", "parquet");
      StringBuilder sb = new StringBuilder();
      sb.append("select " +
              "columns[1] ,columns[2] ,columns[3] ,columns[4] ,columns[5] ,columns[6] ,columns[7] ,columns[8] ,columns[9] ,columns[10] ,columns[11] ,columns[12] , columns[13] ,columns[14] ,columns[15] ,columns[16] ,columns[17] ,columns[18] ,columns[19] ,columns[20] ,columns[21] ,columns[22] ,columns[23] ,columns[24]," +
              " columns[25] ,columns[26] ,columns[27] ,columns[28] ,columns[29] ,columns[30] ,columns[31] ,columns[32] ,columns[33] ,columns[34] ,columns[35] ,columns[36] ,columns[37] ,columns[38] ,columns[39] ,columns[40] ,columns[41] ,columns[42] ,columns[43] ,columns[44] ,columns[45] ,columns[46] ,columns[47] ,columns[48] ,columns[49] ,columns[50] ,columns[51] ,columns[52] ,columns[53] ,columns[54] ,columns[55] ,columns[56] ,columns[57] ,columns[58] ,columns[59] ,columns[60] ,columns[61] ,columns[62] ,columns[63] ,columns[64] ,columns[65] ,columns[66] ,columns[67] ,columns[68] ,columns[69] ,columns[70] ,columns[71] ,columns[72] ,columns[73] ,columns[74] ,columns[75] ,columns[76] ,columns[77] ,columns[78] ,columns[79] ,columns[80] ,columns[81] ,columns[82] ,columns[83] ,columns[84] ,columns[85] ,columns[86] ,columns[87] ,columns[88] ,columns[89] ,columns[90] ,columns[91] ,columns[92] ,columns[93] ,columns[94] ,columns[95] ,columns[96] ,columns[97] ,columns[98] ,columns[99] ,columns[100] ,columns[101] ,columns[102] ,columns[103] ,columns[104] ,columns[105] ,columns[106] ,columns[107] ,columns[108] ,columns[109] ,columns[110] ,columns[111] ,columns[112] ,columns[113] ,columns[114] ,columns[115] ,columns[116] ,columns[117] ,columns[118] ,columns[119] ,columns[120] ,columns[121] ,columns[122] ,columns[123] ,columns[124] ,columns[125] ,columns[126] ,columns[127] ,columns[128] ,columns[129] ,columns[130] ,columns[131] ,columns[132] ,columns[133] ,columns[134] ,columns[135] ,columns[136] ,columns[137] ,columns[138] ,columns[139] ,columns[140] ,columns[141] ,columns[142] ,columns[143] ,columns[144] ,columns[145] ,columns[146] ,columns[147] ,columns[148] ,columns[149] ,columns[150] ,columns[151] ,columns[152] ,columns[153] ,columns[154] ,columns[155] ,columns[156] ,columns[157] ,columns[158] ,columns[159] ,columns[160] ,columns[161] ,columns[162] ,columns[163] ,columns[164] ,columns[165] ,columns[166] ,columns[167] ,columns[168] ,columns[169] ,columns[170] ,columns[171] ,columns[172] ,columns[173] ,columns[174] ,columns[175] ,columns[176] ,columns[177] ,columns[178] ,columns[179] ,columns[180] ,columns[181] ,columns[182] ,columns[183] ,columns[184] ,columns[185] ,columns[186] ,columns[187] ,columns[188] ,columns[189] ,columns[190] ,columns[191] ,columns[192] ,columns[193] ,columns[194] ,columns[195] ,columns[196] ,columns[197] ,columns[198] ,columns[199] ,columns[200] ,columns[201] ,columns[202] ,columns[203] ,columns[204] ,columns[205] ,columns[206] ,columns[207] ,columns[208] ,columns[209] ,columns[210] ,columns[211] ,columns[212] ,columns[213] ,columns[214] ,columns[215] ,columns[216] ,columns[217] ,columns[218] ,columns[219] ,columns[220] ,columns[221] ,columns[222] ,columns[223] ,columns[224] ,columns[225] ,columns[226] ,columns[227] ,columns[228] ,columns[229] ,columns[230] ,columns[231] ,columns[232] ,columns[233] ,columns[234] ,columns[235] ,columns[236] ,columns[237] ,columns[238] ,columns[239] ,columns[240] ,columns[241] ,columns[242] ,columns[243] ,columns[244] ,columns[245] ,columns[246] ,columns[247] ,columns[248] ,columns[249] ,columns[250] ,columns[251] ,columns[252] ,columns[253] ,columns[254] ,columns[255] ,columns[256] ,columns[257] ,columns[258] ,columns[259] ,columns[260] ,columns[261] ,columns[262] ,columns[263] ,columns[264] ,columns[265] ,columns[266] ,columns[267] ,columns[268] ,columns[269] ,columns[270] ,columns[271] ,columns[272] ,columns[273] ,columns[274] ,columns[275] ,columns[276] ,columns[277] ,columns[278] ,columns[279] ,columns[280] ,columns[281] ,columns[282] ,columns[283] ,columns[284] ,columns[285] ,columns[286] ,columns[287] ,columns[288] ,columns[289] ,columns[290] ,columns[291] ,columns[292] ,columns[293] ,columns[294] ,columns[295] ,columns[296] ,columns[297] ,columns[298] ,columns[299] ,columns[300] ,columns[301] ,columns[302] ,columns[303] ,columns[304] ,columns[305] ,columns[306] ,columns[307] ,columns[308] ,columns[309] ,columns[310] ,columns[311] ,columns[312] ,columns[313] ,columns[314] ,columns[315] ,columns[316] ,columns[317] ,columns[318] ,columns[319] ,columns[320] ,columns[321] ,columns[322] ,columns[323] ,columns[324] ,columns[325] ,columns[326] ,columns[327] ,columns[328] ,columns[329] ,columns[330] ,columns[331] ,columns[332] ,columns[333] ,columns[334] ,columns[335] ,columns[336] ,columns[337] ,columns[338] ,columns[339] ,columns[340] ,columns[341] ,columns[342] ,columns[343] ,columns[344] ,columns[345] ,columns[346] ,columns[347] ,columns[348] ,columns[349] ,columns[350] ,columns[351] ,columns[352] ,columns[353] ,columns[354] ,columns[355] ,columns[356] ,columns[357] ,columns[358] ,columns[359] ,columns[360] ,columns[361] ,columns[362] ,columns[363] ,columns[364] ,columns[365] ,columns[366] ,columns[367] ,columns[368] ,columns[369] ,columns[370] ,columns[371] ,columns[372] ,columns[373] ,columns[374] ,columns[375] ,columns[376] ,columns[377] ,columns[378] ,columns[379] ,columns[380] ,columns[381] ,columns[382] ,columns[383] ,columns[384] ,columns[385] ,columns[386] ,columns[387] ,columns[388] ,columns[389] ,columns[390] ,columns[391] ,columns[392] ,columns[393] ,columns[394] ,columns[395] ,columns[396] ,columns[397] ,columns[398] ,columns[399] ,columns[400] ,columns[401] ,columns[402] ,columns[403] ,columns[404] ,columns[405] ,columns[406] ,columns[407] ,columns[408] ,columns[409] ,columns[410] ,columns[411] ,columns[412] ,columns[413] ,columns[414] ,columns[415] ,columns[416] ,columns[417] ,columns[418] ,columns[419] ,columns[420] ,columns[421] ,columns[422] ,columns[423] ,columns[424] ,columns[425] ,columns[426] ,columns[427] ,columns[428] ,columns[429] ,columns[430] ,columns[431] ,columns[432] ,columns[433] ,columns[434] ,columns[435] ,columns[436] ,columns[437] ,columns[438] ,columns[439] ,columns[440] ,columns[441] ,columns[442] ,columns[443] ,columns[444] ,columns[445] ,columns[446] ,columns[447] ,columns[448] ,columns[449] ,columns[450] ,columns[451] ,columns[452] ,columns[453] ,columns[454] ,columns[455] ,columns[456] ,columns[457] ,columns[458] ,columns[459] ,columns[460] ,columns[461] ,columns[462] ,columns[463] ,columns[464] ,columns[465] ,columns[466] ,columns[467] ,columns[468] ,columns[469] ,columns[470] ,columns[471] ,columns[472] ,columns[473] ,columns[474] ,columns[475] ,columns[476] ,columns[477] ,columns[478] ,columns[479] ,columns[480] ,columns[481] ,columns[482] ,columns[483] ,columns[484] ,columns[485] ,columns[486] ,columns[487] ,columns[488] ,columns[489] ,columns[490] ,columns[491] ,columns[492] ,columns[493] ,columns[494] ,columns[495] ,columns[496] ,columns[497] ,columns[498] ,columns[499] ,columns[500] ,columns[501] ,columns[502] ,columns[503] ,columns[504] ,columns[505] ,columns[506] ,columns[507] ,columns[508] ,columns[509] ,columns[510] ,columns[511] ,columns[512] ,columns[513] ,columns[514] ,columns[515] ,columns[516] ,columns[517] ,columns[518] ,columns[519] ,columns[520] ,columns[521] ,columns[522] ,columns[523] ,columns[524] ,columns[525] ,columns[526] ,columns[527] ,columns[528] ,columns[529] ,columns[530] ,columns[531] ,columns[532] ,columns[533] ,columns[534] ,columns[535] ,columns[536] ,columns[537] ,columns[538] ,columns[539] ,columns[540] ,columns[541] ,columns[542] ,columns[543] ,columns[544] ,columns[545] ,columns[546] ,columns[547] ,columns[548] ,columns[549] ,columns[550] ,columns[551] ,columns[552] ,columns[553] ,columns[554] ,columns[555] ,columns[556] ,columns[557] ,columns[558] ,columns[559] ,columns[560] ,columns[561] ,columns[562] ,columns[563] ,columns[564] ,columns[565] ,columns[566] ,columns[567] ,columns[568] ,columns[569] ,columns[570] ,columns[571] ,columns[572] ,columns[573] ,columns[574] ,columns[575] ,columns[576] ,columns[577] ,columns[578] ,columns[579] ,columns[580] ,columns[581] ,columns[582] ,columns[583] ,columns[584] ,columns[585] ,columns[586] ,columns[587] ,columns[588] ,columns[589] ,columns[590] ,columns[591] ,columns[592] ,columns[593] ,columns[594] ,columns[595] ,columns[596] ,columns[597] ,columns[598] ,columns[599] ,columns[600] ,columns[601] ,columns[602] ,columns[603] ,columns[604] ,columns[605] ,columns[606] ,columns[607] ,columns[608] ,columns[609] ,columns[610] ,columns[611] ,columns[612] ,columns[613] ,columns[614] ,columns[615] ,columns[616] ,columns[617] ,columns[618] ,columns[619] ,columns[620] ,columns[621] ,columns[622] ,columns[623] ,columns[624] ,columns[625] ,columns[626] ,columns[627] ,columns[628] ,columns[629] ,columns[630] ,columns[631] ,columns[632] ,columns[633] ,columns[634] ,columns[635] ,columns[636] ,columns[637] ,columns[638] ,columns[639] ,columns[640] ,columns[641] ,columns[642] ,columns[643] ,columns[644] ,columns[645] ,columns[646] ,columns[647] ,columns[648] ,columns[649] ,columns[650] ,columns[651] ,columns[652] ,columns[653] ,columns[654] ,columns[655] ,columns[656] ,columns[657] ,columns[658] ,columns[659] ,columns[660] ,columns[661] ,columns[662] ,columns[663] ,columns[664] ,columns[665] ,columns[666] ,columns[667] ,columns[668] ,columns[669] ,columns[670] ,columns[671] ,columns[672] ,columns[673] ,columns[674] ,columns[675] ,columns[676] ,columns[677] ,columns[678] ,columns[679] ,columns[680] ,columns[681] ,columns[682] ,columns[683] ,columns[684] ,columns[685] ,columns[686] ,columns[687] ,columns[688] ,columns[689] ,columns[690] ,columns[691] ,columns[692] ,columns[693] ,columns[694] ,columns[695] ,columns[696] ,columns[697] ,columns[698] ,columns[699] ,columns[700] ,columns[701] ,columns[702] ,columns[703] ,columns[704] ,columns[705] ,columns[706] ,columns[707] ,columns[708] ,columns[709] ,columns[710] ,columns[711] ,columns[712] ,columns[713] ,columns[714] ,columns[715] ,columns[716] ,columns[717] ,columns[718] ,columns[719] ,columns[720] ,columns[721] ,columns[722] ,columns[723] ,columns[724] ,columns[725] ,columns[726] ,columns[727] ,columns[728] ,columns[729] ,columns[730] ,columns[731] ,columns[732] ,columns[733] ,columns[734] ,columns[735] ,columns[736] ,columns[737] ,columns[738] ,columns[739] ,columns[740] ,columns[741] ,columns[742] ,columns[743] ,columns[744] ,columns[745] ,columns[746] ,columns[747] ,columns[748] ,columns[749] ,columns[750] ,columns[751] ,columns[752] ,columns[753] ,columns[754] ,columns[755] ,columns[756] ,columns[757] ,columns[758] ,columns[759] ,columns[760] ,columns[761] ,columns[762] ,columns[763] ,columns[764] ,columns[765] ,columns[766] ,columns[767] ,columns[768] ,columns[769] ,columns[770] ,columns[771] ,columns[772] ,columns[773] ,columns[774] ,columns[775] ,columns[776] ,columns[777] ,columns[778] ,columns[779] ,columns[780] ,columns[781] ,columns[782] ,columns[783] ,columns[784] ,columns[785] ,columns[786] ,columns[787] ,columns[788] ,columns[789] ,columns[790] ,columns[791] ,columns[792] ,columns[793] ,columns[794] ,columns[795] ,columns[796] ,columns[797] ,columns[798] ,columns[799] ,columns[800] ,columns[801] ,columns[802] ,columns[803] ,columns[804] ,columns[805] ,columns[806] ,columns[807] ,columns[808] ,columns[809] ,columns[810] ,columns[811] ,columns[812] ,columns[813] ,columns[814] ,columns[815] ,columns[816] ,columns[817] ,columns[818] ,columns[819] ,columns[820] ,columns[821] ,columns[822] ,columns[823] ,columns[824] ,columns[825] ,columns[826] ,columns[827] ,columns[828] ,columns[829] ,columns[830] ,columns[831] ,columns[832] ,columns[833] ,columns[834] ,columns[835] ,columns[836] ,columns[837] ,columns[838] ,columns[839] ,columns[840] ,columns[841] ,columns[842] ,columns[843] ,columns[844] ,columns[845] ,columns[846] ,columns[847] ,columns[848] ,columns[849] ,columns[850] ,columns[851] ,columns[852] ,columns[853] ,columns[854] ,columns[855] ,columns[856] ,columns[857] ,columns[858] ,columns[859] ,columns[860] ,columns[861] ,columns[862] ,columns[863] ,columns[864] ,columns[865] ,columns[866] ,columns[867] ,columns[868] ,columns[869] ,columns[870] ,columns[871] ,columns[872] ,columns[873] ,columns[874] ,columns[875] ,columns[876] ,columns[877] ,columns[878] ,columns[879] ,columns[880] ,columns[881] ,columns[882] ,columns[883] ,columns[884] ,columns[885] ,columns[886] ,columns[887] ,columns[888] ,columns[889] ,columns[890] ,columns[891] ,columns[892] ,columns[893] ,columns[894] ,columns[895] ,columns[896] ,columns[897] ,columns[898] ,columns[899] ,columns[900] ,columns[901] ,columns[902] ,columns[903] ,columns[904] ,columns[905] ,columns[906] ,columns[907] ,columns[908] ,columns[909] ,columns[910] ,columns[911] ,columns[912] ,columns[913] ,columns[914] ,columns[915] ,columns[916] ,columns[917] ,columns[918] ,columns[919] ,columns[920] ,columns[921] ,columns[922] ,columns[923] ,columns[924] ,columns[925] ,columns[926] ,columns[927] ,columns[928] ,columns[929] ,columns[930] ,columns[931] ,columns[932] ,columns[933] ,columns[934] ,columns[935] ,columns[936] ,columns[937] ,columns[938] ,columns[939] ,columns[940] ,columns[941] ,columns[942] ,columns[943] ,columns[944] ,columns[945] ,columns[946] ,columns[947] ,columns[948] ,columns[949] ,columns[950] ,columns[951] ,columns[952] ,columns[953] ,columns[954] ,columns[955] ,columns[956] ,columns[957] ,columns[958] ,columns[959] ,columns[960] ,columns[961] ,columns[962] ,columns[963] ,columns[964] ,columns[965] ,columns[966] ,columns[967] ,columns[968] ,columns[969] ,columns[970] ,columns[971] ,columns[972] ,columns[973] ,columns[974] ,columns[975] ,columns[976] ,columns[977] ,columns[978] ,columns[979] ,columns[980] ,columns[981] ,columns[982] ,columns[983] ,columns[984] ,columns[985] ,columns[986] ,columns[987] ,columns[988] ,columns[989] ,columns[990] ,columns[991] ,columns[992] ,columns[993] ,columns[994] ,columns[995] ,columns[996] ,columns[997] ,columns[998] ,columns[999] ,columns[1000] ,columns[1001] ,columns[1002] ,columns[1003] ,columns[1004] ,columns[1005] ,columns[1006] ,columns[1007] ,columns[1008] ,columns[1009] ,columns[1010] ,columns[1011] ,columns[1012] ,columns[1013] ,columns[1014] ,columns[1015] ,columns[1016] ,columns[1017] ,columns[1018] ,columns[1019] ,columns[1020] ,columns[1021] ,columns[1022] ,columns[1023] ,columns[1024] ,columns[1025] ,columns[1026] ,columns[1027] ,columns[1028] ,columns[1029] ,columns[1030] ,columns[1031] ,columns[1032] ,columns[1033] ,columns[1034] ,columns[1035] ,columns[1036] ,columns[1037] ,columns[1038] ,columns[1039] ,columns[1040] ,columns[1041] ,columns[1042] ,columns[1043] ,columns[1044] ,columns[1045] ,columns[1046] ,columns[1047] ,columns[1048] ,columns[1049] ,columns[1050] ,columns[1051] ,columns[1052] ,columns[1053] ,columns[1054] ,columns[1055] ,columns[1056] ,columns[1057] ,columns[1058] ,columns[1059] ,columns[1060] ,columns[1061] ,columns[1062] ,columns[1063] ,columns[1064] ,columns[1065] ,columns[1066] ,columns[1067] ,columns[1068] ,columns[1069] ,columns[1070] ,columns[1071] ,columns[1072] ,columns[1073] ,columns[1074] ,columns[1075] ,columns[1076] ,columns[1077] ,columns[1078] ,columns[1079] ,columns[1080] ,columns[1081] ,columns[1082] ,columns[1083] ,columns[1084] ,columns[1085] ,columns[1086] ,columns[1087] ,columns[1088] ,columns[1089] ,columns[1090] ,columns[1091] ,columns[1092] ,columns[1093] ,columns[1094] ,columns[1095] ,columns[1096] ,columns[1097] ,columns[1098] ,columns[1099] ,columns[1100] ,columns[1101] ,columns[1102] ,columns[1103] ,columns[1104] ,columns[1105] ,columns[1106] ,columns[1107] ,columns[1108] ,columns[1109] ,columns[1110] ,columns[1111] ,columns[1112] ,columns[1113] ,columns[1114] ,columns[1115] ,columns[1116] ,columns[1117] ,columns[1118] ,columns[1119] ,columns[1120] ,columns[1121] ,columns[1122] ,columns[1123] ,columns[1124] ,columns[1125] ,columns[1126] ,columns[1127] ,columns[1128] ,columns[1129] ,columns[1130] ,columns[1131] ,columns[1132] ,columns[1133] ,columns[1134] ,columns[1135] ,columns[1136] ,columns[1137] ,columns[1138] ,columns[1139] ,columns[1140] ,columns[1141] ,columns[1142] ,columns[1143] ,columns[1144] ,columns[1145] ,columns[1146] ,columns[1147] ,columns[1148] ,columns[1149] ,columns[1150] ,columns[1151] ,columns[1152] ,columns[1153] ,columns[1154] ,columns[1155] ,columns[1156] ,columns[1157] ,columns[1158] ,columns[1159] ,columns[1160] ,columns[1161] ,columns[1162] ,columns[1163] ,columns[1164] ,columns[1165] ,columns[1166] ,columns[1167] ,columns[1168] ,columns[1169] ,columns[1170] ,columns[1171] ,columns[1172] ,columns[1173] ,columns[1174] ,columns[1175] ,columns[1176] ,columns[1177] ,columns[1178] ,columns[1179] ,columns[1180] ,columns[1181] ,columns[1182] ,columns[1183] ,columns[1184] ,columns[1185] ,columns[1186] ,columns[1187] ,columns[1188] ,columns[1189] ,columns[1190] ,columns[1191] ,columns[1192] ,columns[1193] ,columns[1194] ,columns[1195] ,columns[1196] ,columns[1197] ,columns[1198] ,columns[1199] ,columns[1200]");


         sb.append(" from (" +
              "select " +
              "columns[1] ,columns[2] ,columns[3] ,columns[4] ,columns[5] ,columns[6] ,columns[7] ,columns[8] ,columns[9] ,columns[10] ,columns[11] ,columns[12] ," +
              "columns[13] ,columns[14] ,columns[15] ,columns[16] ,columns[17] ,columns[18] ,columns[19] ,columns[20] ,columns[21] ,columns[22] ,columns[23] ,columns[24] ," +
              "columns[25] ,columns[26] ,columns[27] ,columns[28] ,columns[29] ,columns[30] ,columns[31] ,columns[32] ,columns[33] ,columns[34] ,columns[35] ,columns[36] ,columns[37] ,columns[38] ,columns[39] ,columns[40] ,columns[41] ,columns[42] ,columns[43] ,columns[44] ,columns[45] ,columns[46] ,columns[47] ,columns[48] ,columns[49] ,columns[50] ,columns[51] ,columns[52] ,columns[53] ,columns[54] ,columns[55] ,columns[56] ,columns[57] ,columns[58] ,columns[59] ,columns[60] ,columns[61] ,columns[62] ,columns[63] ,columns[64] ,columns[65] ,columns[66] ,columns[67] ,columns[68] ,columns[69] ,columns[70] ,columns[71] ,columns[72] ,columns[73] ,columns[74] ,columns[75] ,columns[76] ,columns[77] ,columns[78] ,columns[79] ,columns[80] ,columns[81] ,columns[82] ,columns[83] ,columns[84] ,columns[85] ,columns[86] ,columns[87] ,columns[88] ,columns[89] ,columns[90] ,columns[91] ,columns[92] ,columns[93] ,columns[94] ,columns[95] ,columns[96] ,columns[97] ,columns[98] ,columns[99] ,columns[100] ,columns[101] ,columns[102] ,columns[103] ,columns[104] ,columns[105] ,columns[106] ,columns[107] ,columns[108] ,columns[109] ,columns[110] ,columns[111] ,columns[112] ,columns[113] ,columns[114] ,columns[115] ,columns[116] ,columns[117] ,columns[118] ,columns[119] ,columns[120] ,columns[121] ,columns[122] ,columns[123] ,columns[124] ,columns[125] ,columns[126] ,columns[127] ,columns[128] ,columns[129] ,columns[130] ,columns[131] ,columns[132] ,columns[133] ,columns[134] ,columns[135] ,columns[136] ,columns[137] ,columns[138] ,columns[139] ,columns[140] ,columns[141] ,columns[142] ,columns[143] ,columns[144] ,columns[145] ,columns[146] ,columns[147] ,columns[148] ,columns[149] ,columns[150] ,columns[151] ,columns[152] ,columns[153] ,columns[154] ,columns[155] ,columns[156] ,columns[157] ,columns[158] ,columns[159] ,columns[160] ,columns[161] ,columns[162] ,columns[163] ,columns[164] ,columns[165] ,columns[166] ,columns[167] ,columns[168] ,columns[169] ,columns[170] ,columns[171] ,columns[172] ,columns[173] ,columns[174] ,columns[175] ,columns[176] ,columns[177] ,columns[178] ,columns[179] ,columns[180] ,columns[181] ,columns[182] ,columns[183] ,columns[184] ,columns[185] ,columns[186] ,columns[187] ,columns[188] ,columns[189] ,columns[190] ,columns[191] ,columns[192] ,columns[193] ,columns[194] ,columns[195] ,columns[196] ,columns[197] ,columns[198] ,columns[199] ,columns[200] ,columns[201] ,columns[202] ,columns[203] ,columns[204] ,columns[205] ,columns[206] ,columns[207] ,columns[208] ,columns[209] ,columns[210] ,columns[211] ,columns[212] ,columns[213] ,columns[214] ,columns[215] ,columns[216] ,columns[217] ,columns[218] ,columns[219] ,columns[220] ,columns[221] ,columns[222] ,columns[223] ,columns[224] ,columns[225] ,columns[226] ,columns[227] ,columns[228] ,columns[229] ,columns[230] ,columns[231] ,columns[232] ,columns[233] ,columns[234] ,columns[235] ,columns[236] ,columns[237] ,columns[238] ,columns[239] ,columns[240] ,columns[241] ,columns[242] ,columns[243] ,columns[244] ,columns[245] ,columns[246] ,columns[247] ,columns[248] ,columns[249] ,columns[250] ,columns[251] ,columns[252] ,columns[253] ,columns[254] ,columns[255] ,columns[256] ,columns[257] ,columns[258] ,columns[259] ,columns[260] ,columns[261] ,columns[262] ,columns[263] ,columns[264] ,columns[265] ,columns[266] ,columns[267] ,columns[268] ,columns[269] ,columns[270] ,columns[271] ,columns[272] ,columns[273] ,columns[274] ,columns[275] ,columns[276] ,columns[277] ,columns[278] ,columns[279] ,columns[280] ,columns[281] ,columns[282] ,columns[283] ,columns[284] ,columns[285] ,columns[286] ,columns[287] ,columns[288] ,columns[289] ,columns[290] ,columns[291] ,columns[292] ,columns[293] ,columns[294] ,columns[295] ,columns[296] ,columns[297] ,columns[298] ,columns[299] ,columns[300] ,columns[301] ,columns[302] ,columns[303] ,columns[304] ,columns[305] ,columns[306] ,columns[307] ,columns[308] ,columns[309] ,columns[310] ,columns[311] ,columns[312] ,columns[313] ,columns[314] ,columns[315] ,columns[316] ,columns[317] ,columns[318] ,columns[319] ,columns[320] ,columns[321] ,columns[322] ,columns[323] ,columns[324] ,columns[325] ,columns[326] ,columns[327] ,columns[328] ,columns[329] ,columns[330] ,columns[331] ,columns[332] ,columns[333] ,columns[334] ,columns[335] ,columns[336] ,columns[337] ,columns[338] ,columns[339] ,columns[340] ,columns[341] ,columns[342] ,columns[343] ,columns[344] ,columns[345] ,columns[346] ,columns[347] ,columns[348] ,columns[349] ,columns[350] ,columns[351] ,columns[352] ,columns[353] ,columns[354] ,columns[355] ,columns[356] ,columns[357] ,columns[358] ,columns[359] ,columns[360] ,columns[361] ,columns[362] ,columns[363] ,columns[364] ,columns[365] ,columns[366] ,columns[367] ,columns[368] ,columns[369] ,columns[370] ,columns[371] ,columns[372] ,columns[373] ,columns[374] ,columns[375] ,columns[376] ,columns[377] ,columns[378] ,columns[379] ,columns[380] ,columns[381] ,columns[382] ,columns[383] ,columns[384] ,columns[385] ,columns[386] ,columns[387] ,columns[388] ,columns[389] ,columns[390] ,columns[391] ,columns[392] ,columns[393] ,columns[394] ,columns[395] ,columns[396] ,columns[397] ,columns[398] ,columns[399] ,columns[400] ,columns[401] ,columns[402] ,columns[403] ,columns[404] ,columns[405] ,columns[406] ,columns[407] ,columns[408] ,columns[409] ,columns[410] ,columns[411] ,columns[412] ,columns[413] ,columns[414] ,columns[415] ,columns[416] ,columns[417] ,columns[418] ,columns[419] ,columns[420] ,columns[421] ,columns[422] ,columns[423] ,columns[424] ,columns[425] ,columns[426] ,columns[427] ,columns[428] ,columns[429] ,columns[430] ,columns[431] ,columns[432] ,columns[433] ,columns[434] ,columns[435] ,columns[436] ,columns[437] ,columns[438] ,columns[439] ,columns[440] ,columns[441] ,columns[442] ,columns[443] ,columns[444] ,columns[445] ,columns[446] ,columns[447] ,columns[448] ,columns[449] ,columns[450] ,columns[451] ,columns[452] ,columns[453] ,columns[454] ,columns[455] ,columns[456] ,columns[457] ,columns[458] ,columns[459] ,columns[460] ,columns[461] ,columns[462] ,columns[463] ,columns[464] ,columns[465] ,columns[466] ,columns[467] ,columns[468] ,columns[469] ,columns[470] ,columns[471] ,columns[472] ,columns[473] ,columns[474] ,columns[475] ,columns[476] ,columns[477] ,columns[478] ,columns[479] ,columns[480] ,columns[481] ,columns[482] ,columns[483] ,columns[484] ,columns[485] ,columns[486] ,columns[487] ,columns[488] ,columns[489] ,columns[490] ,columns[491] ,columns[492] ,columns[493] ,columns[494] ,columns[495] ,columns[496] ,columns[497] ,columns[498] ,columns[499] ,columns[500] ,columns[501] ,columns[502] ,columns[503] ,columns[504] ,columns[505] ,columns[506] ,columns[507] ,columns[508] ,columns[509] ,columns[510] ,columns[511] ,columns[512] ,columns[513] ,columns[514] ,columns[515] ,columns[516] ,columns[517] ,columns[518] ,columns[519] ,columns[520] ,columns[521] ,columns[522] ,columns[523] ,columns[524] ,columns[525] ,columns[526] ,columns[527] ,columns[528] ,columns[529] ,columns[530] ,columns[531] ,columns[532] ,columns[533] ,columns[534] ,columns[535] ,columns[536] ,columns[537] ,columns[538] ,columns[539] ,columns[540] ,columns[541] ,columns[542] ,columns[543] ,columns[544] ,columns[545] ,columns[546] ,columns[547] ,columns[548] ,columns[549] ,columns[550] ,columns[551] ,columns[552] ,columns[553] ,columns[554] ,columns[555] ,columns[556] ,columns[557] ,columns[558] ,columns[559] ,columns[560] ,columns[561] ,columns[562] ,columns[563] ,columns[564] ,columns[565] ,columns[566] ,columns[567] ,columns[568] ,columns[569] ,columns[570] ,columns[571] ,columns[572] ,columns[573] ,columns[574] ,columns[575] ,columns[576] ,columns[577] ,columns[578] ,columns[579] ,columns[580] ,columns[581] ,columns[582] ,columns[583] ,columns[584] ,columns[585] ,columns[586] ,columns[587] ,columns[588] ,columns[589] ,columns[590] ,columns[591] ,columns[592] ,columns[593] ,columns[594] ,columns[595] ,columns[596] ,columns[597] ,columns[598] ,columns[599] ,columns[600] ,columns[601] ,columns[602] ,columns[603] ,columns[604] ,columns[605] ,columns[606] ,columns[607] ,columns[608] ,columns[609] ,columns[610] ,columns[611] ,columns[612] ,columns[613] ,columns[614] ,columns[615] ,columns[616] ,columns[617] ,columns[618] ,columns[619] ,columns[620] ,columns[621] ,columns[622] ,columns[623] ,columns[624] ,columns[625] ,columns[626] ,columns[627] ,columns[628] ,columns[629] ,columns[630] ,columns[631] ,columns[632] ,columns[633] ,columns[634] ,columns[635] ,columns[636] ,columns[637] ,columns[638] ,columns[639] ,columns[640] ,columns[641] ,columns[642] ,columns[643] ,columns[644] ,columns[645] ,columns[646] ,columns[647] ,columns[648] ,columns[649] ,columns[650] ,columns[651] ,columns[652] ,columns[653] ,columns[654] ,columns[655] ,columns[656] ,columns[657] ,columns[658] ,columns[659] ,columns[660] ,columns[661] ,columns[662] ,columns[663] ,columns[664] ,columns[665] ,columns[666] ,columns[667] ,columns[668] ,columns[669] ,columns[670] ,columns[671] ,columns[672] ,columns[673] ,columns[674] ,columns[675] ,columns[676] ,columns[677] ,columns[678] ,columns[679] ,columns[680] ,columns[681] ,columns[682] ,columns[683] ,columns[684] ,columns[685] ,columns[686] ,columns[687] ,columns[688] ,columns[689] ,columns[690] ,columns[691] ,columns[692] ,columns[693] ,columns[694] ,columns[695] ,columns[696] ,columns[697] ,columns[698] ,columns[699] ,columns[700] ,columns[701] ,columns[702] ,columns[703] ,columns[704] ,columns[705] ,columns[706] ,columns[707] ,columns[708] ,columns[709] ,columns[710] ,columns[711] ,columns[712] ,columns[713] ,columns[714] ,columns[715] ,columns[716] ,columns[717] ,columns[718] ,columns[719] ,columns[720] ,columns[721] ,columns[722] ,columns[723] ,columns[724] ,columns[725] ,columns[726] ,columns[727] ,columns[728] ,columns[729] ,columns[730] ,columns[731] ,columns[732] ,columns[733] ,columns[734] ,columns[735] ,columns[736] ,columns[737] ,columns[738] ,columns[739] ,columns[740] ,columns[741] ,columns[742] ,columns[743] ,columns[744] ,columns[745] ,columns[746] ,columns[747] ,columns[748] ,columns[749] ,columns[750] ,columns[751] ,columns[752] ,columns[753] ,columns[754] ,columns[755] ,columns[756] ,columns[757] ,columns[758] ,columns[759] ,columns[760] ,columns[761] ,columns[762] ,columns[763] ,columns[764] ,columns[765] ,columns[766] ,columns[767] ,columns[768] ,columns[769] ,columns[770] ,columns[771] ,columns[772] ,columns[773] ,columns[774] ,columns[775] ,columns[776] ,columns[777] ,columns[778] ,columns[779] ,columns[780] ,columns[781] ,columns[782] ,columns[783] ,columns[784] ,columns[785] ,columns[786] ,columns[787] ,columns[788] ,columns[789] ,columns[790] ,columns[791] ,columns[792] ,columns[793] ,columns[794] ,columns[795] ,columns[796] ,columns[797] ,columns[798] ,columns[799] ,columns[800] ,columns[801] ,columns[802] ,columns[803] ,columns[804] ,columns[805] ,columns[806] ,columns[807] ,columns[808] ,columns[809] ,columns[810] ,columns[811] ,columns[812] ,columns[813] ,columns[814] ,columns[815] ,columns[816] ,columns[817] ,columns[818] ,columns[819] ,columns[820] ,columns[821] ,columns[822] ,columns[823] ,columns[824] ,columns[825] ,columns[826] ,columns[827] ,columns[828] ,columns[829] ,columns[830] ,columns[831] ,columns[832] ,columns[833] ,columns[834] ,columns[835] ,columns[836] ,columns[837] ,columns[838] ,columns[839] ,columns[840] ,columns[841] ,columns[842] ,columns[843] ,columns[844] ,columns[845] ,columns[846] ,columns[847] ,columns[848] ,columns[849] ,columns[850] ,columns[851] ,columns[852] ,columns[853] ,columns[854] ,columns[855] ,columns[856] ,columns[857] ,columns[858] ,columns[859] ,columns[860] ,columns[861] ,columns[862] ,columns[863] ,columns[864] ,columns[865] ,columns[866] ,columns[867] ,columns[868] ,columns[869] ,columns[870] ,columns[871] ,columns[872] ,columns[873] ,columns[874] ,columns[875] ,columns[876] ,columns[877] ,columns[878] ,columns[879] ,columns[880] ,columns[881] ,columns[882] ,columns[883] ,columns[884] ,columns[885] ,columns[886] ,columns[887] ,columns[888] ,columns[889] ,columns[890] ,columns[891] ,columns[892] ,columns[893] ,columns[894] ,columns[895] ,columns[896] ,columns[897] ,columns[898] ,columns[899] ,columns[900] ,columns[901] ,columns[902] ,columns[903] ,columns[904] ,columns[905] ,columns[906] ,columns[907] ,columns[908] ,columns[909] ,columns[910] ,columns[911] ,columns[912] ,columns[913] ,columns[914] ,columns[915] ,columns[916] ,columns[917] ,columns[918] ,columns[919] ,columns[920] ,columns[921] ,columns[922] ,columns[923] ,columns[924] ,columns[925] ,columns[926] ,columns[927] ,columns[928] ,columns[929] ,columns[930] ,columns[931] ,columns[932] ,columns[933] ,columns[934] ,columns[935] ,columns[936] ,columns[937] ,columns[938] ,columns[939] ,columns[940] ,columns[941] ,columns[942] ,columns[943] ,columns[944] ,columns[945] ,columns[946] ,columns[947] ,columns[948] ,columns[949] ,columns[950] ,columns[951] ,columns[952] ,columns[953] ,columns[954] ,columns[955] ,columns[956] ,columns[957] ,columns[958] ,columns[959] ,columns[960] ,columns[961] ,columns[962] ,columns[963] ,columns[964] ,columns[965] ,columns[966] ,columns[967] ,columns[968] ,columns[969] ,columns[970] ,columns[971] ,columns[972] ,columns[973] ,columns[974] ,columns[975] ,columns[976] ,columns[977] ,columns[978] ,columns[979] ,columns[980] ,columns[981] ,columns[982] ,columns[983] ,columns[984] ,columns[985] ,columns[986] ,columns[987] ,columns[988] ,columns[989] ,columns[990] ,columns[991] ,columns[992] ,columns[993] ,columns[994] ,columns[995] ,columns[996] ,columns[997] ,columns[998] ,columns[999] ,columns[1000] ,columns[1001] ,columns[1002] ,columns[1003] ,columns[1004] ,columns[1005] ,columns[1006] ,columns[1007] ,columns[1008] ,columns[1009] ,columns[1010] ,columns[1011] ,columns[1012] ,columns[1013] ,columns[1014] ,columns[1015] ,columns[1016] ,columns[1017] ,columns[1018] ,columns[1019] ,columns[1020] ,columns[1021] ,columns[1022] ,columns[1023] ,columns[1024] ,columns[1025] ,columns[1026] ,columns[1027] ,columns[1028] ,columns[1029] ,columns[1030] ,columns[1031] ,columns[1032] ,columns[1033] ,columns[1034] ,columns[1035] ,columns[1036] ,columns[1037] ,columns[1038] ,columns[1039] ,columns[1040] ,columns[1041] ,columns[1042] ,columns[1043] ,columns[1044] ,columns[1045] ,columns[1046] ,columns[1047] ,columns[1048] ,columns[1049] ,columns[1050] ,columns[1051] ,columns[1052] ,columns[1053] ,columns[1054] ,columns[1055] ,columns[1056] ,columns[1057] ,columns[1058] ,columns[1059] ,columns[1060] ,columns[1061] ,columns[1062] ,columns[1063] ,columns[1064] ,columns[1065] ,columns[1066] ,columns[1067] ,columns[1068] ,columns[1069] ,columns[1070] ,columns[1071] ,columns[1072] ,columns[1073] ,columns[1074] ,columns[1075] ,columns[1076] ,columns[1077] ,columns[1078] ,columns[1079] ,columns[1080] ,columns[1081] ,columns[1082] ,columns[1083] ,columns[1084] ,columns[1085] ,columns[1086] ,columns[1087] ,columns[1088] ,columns[1089] ,columns[1090] ,columns[1091] ,columns[1092] ,columns[1093] ,columns[1094] ,columns[1095] ,columns[1096] ,columns[1097] ,columns[1098] ,columns[1099] ,columns[1100] ,columns[1101] ,columns[1102] ,columns[1103] ,columns[1104] ,columns[1105] ,columns[1106] ,columns[1107] ,columns[1108] ,columns[1109] ,columns[1110] ,columns[1111] ,columns[1112] ,columns[1113] ,columns[1114] ,columns[1115] ,columns[1116] ,columns[1117] ,columns[1118] ,columns[1119] ,columns[1120] ,columns[1121] ,columns[1122] ,columns[1123] ,columns[1124] ,columns[1125] ,columns[1126] ,columns[1127] ,columns[1128] ,columns[1129] ,columns[1130] ,columns[1131] ,columns[1132] ,columns[1133] ,columns[1134] ,columns[1135] ,columns[1136] ,columns[1137] ,columns[1138] ,columns[1139] ,columns[1140] ,columns[1141] ,columns[1142] ,columns[1143] ,columns[1144] ,columns[1145] ,columns[1146] ,columns[1147] ,columns[1148] ,columns[1149] ,columns[1150] ,columns[1151] ,columns[1152] ,columns[1153] ,columns[1154] ,columns[1155] ,columns[1156] ,columns[1157] ,columns[1158] ,columns[1159] ,columns[1160] ,columns[1161] ,columns[1162] ,columns[1163] ,columns[1164] ,columns[1165] ,columns[1166] ,columns[1167] ,columns[1168] ,columns[1169] ,columns[1170] ,columns[1171] ,columns[1172] ,columns[1173] ,columns[1174] ,columns[1175] ,columns[1176] ,columns[1177] ,columns[1178] ,columns[1179] ,columns[1180] ,columns[1181] ,columns[1182] ,columns[1183] ,columns[1184] ,columns[1185] ,columns[1186] ,columns[1187] ,columns[1188] ,columns[1189] ,columns[1190] ,columns[1191] ,columns[1192] ,columns[1193] ,columns[1194] ,columns[1195] ,columns[1196] ,columns[1197] ,columns[1198] ,columns[1199] ,columns[1200] from dfs.`F:\\drill\\files\\3.csv`" +
              "union all\n" +
              "select columns[1] ,columns[2] ,columns[3] ,columns[4] ,columns[5] ,columns[6] ,columns[7] ,columns[8] ,columns[9] ,columns[10] ,columns[11] ,columns[12] ,columns[13] " +
              ",columns[14] ,columns[15] ,columns[16] ,columns[17] ,columns[18] ,columns[19] ,columns[20] ,columns[21] ,columns[22] ,columns[23] ,columns[24] ,columns[25] ," +
              "columns[26] ,columns[27] ,columns[28] ,columns[29] ,columns[30] ,columns[31] ,columns[32] ,columns[33] ,columns[34] ,columns[35] ,columns[36] ,columns[37] ,columns[38] ,columns[39] ,columns[40] ,columns[41] ,columns[42] ,columns[43] ,columns[44] ,columns[45] ,columns[46] ,columns[47] ,columns[48] ,columns[49] ,columns[50] ,columns[51] ,columns[52] ,columns[53] ,columns[54] ,columns[55] ,columns[56] ,columns[57] ,columns[58] ,columns[59] ,columns[60] ,columns[61] ,columns[62] ,columns[63] ,columns[64] ,columns[65] ,columns[66] ,columns[67] ,columns[68] ,columns[69] ,columns[70] ,columns[71] ,columns[72] ,columns[73] ,columns[74] ,columns[75] ,columns[76] ,columns[77] ,columns[78] ,columns[79] ,columns[80] ,columns[81] ,columns[82] ,columns[83] ,columns[84] ,columns[85] ,columns[86] ,columns[87] ,columns[88] ,columns[89] ,columns[90] ,columns[91] ,columns[92] ,columns[93] ,columns[94] ,columns[95] ,columns[96] ,columns[97] ,columns[98] ,columns[99] ,columns[100] ,columns[101] ,columns[102] ,columns[103] ,columns[104] ,columns[105] ,columns[106] ,columns[107] ,columns[108] ,columns[109] ,columns[110] ,columns[111] ,columns[112] ,columns[113] ,columns[114] ,columns[115] ,columns[116] ,columns[117] ,columns[118] ,columns[119] ,columns[120] ,columns[121] ,columns[122] ,columns[123] ,columns[124] ,columns[125] ,columns[126] ,columns[127] ,columns[128] ,columns[129] ,columns[130] ,columns[131] ,columns[132] ,columns[133] ,columns[134] ,columns[135] ,columns[136] ,columns[137] ,columns[138] ,columns[139] ,columns[140] ,columns[141] ,columns[142] ,columns[143] ,columns[144] ,columns[145] ,columns[146] ,columns[147] ,columns[148] ,columns[149] ,columns[150] ,columns[151] ,columns[152] ,columns[153] ,columns[154] ,columns[155] ,columns[156] ,columns[157] ,columns[158] ,columns[159] ,columns[160] ,columns[161] ,columns[162] ,columns[163] ,columns[164] ,columns[165] ,columns[166] ,columns[167] ,columns[168] ,columns[169] ,columns[170] ,columns[171] ,columns[172] ,columns[173] ,columns[174] ,columns[175] ,columns[176] ,columns[177] ,columns[178] ,columns[179] ,columns[180] ,columns[181] ,columns[182] ,columns[183] ,columns[184] ,columns[185] ,columns[186] ,columns[187] ,columns[188] ,columns[189] ,columns[190] ,columns[191] ,columns[192] ,columns[193] ,columns[194] ,columns[195] ,columns[196] ,columns[197] ,columns[198] ,columns[199] ,columns[200] ,columns[201] ,columns[202] ,columns[203] ,columns[204] ,columns[205] ,columns[206] ,columns[207] ,columns[208] ,columns[209] ,columns[210] ,columns[211] ,columns[212] ,columns[213] ,columns[214] ,columns[215] ,columns[216] ,columns[217] ,columns[218] ,columns[219] ,columns[220] ,columns[221] ,columns[222] ,columns[223] ,columns[224] ,columns[225] ,columns[226] ,columns[227] ,columns[228] ,columns[229] ,columns[230] ,columns[231] ,columns[232] ,columns[233] ,columns[234] ,columns[235] ,columns[236] ,columns[237] ,columns[238] ,columns[239] ,columns[240] ,columns[241] ,columns[242] ,columns[243] ,columns[244] ,columns[245] ,columns[246] ,columns[247] ,columns[248] ,columns[249] ,columns[250] ,columns[251] ,columns[252] ,columns[253] ,columns[254] ,columns[255] ,columns[256] ,columns[257] ,columns[258] ,columns[259] ,columns[260] ,columns[261] ,columns[262] ,columns[263] ,columns[264] ,columns[265] ,columns[266] ,columns[267] ,columns[268] ,columns[269] ,columns[270] ,columns[271] ,columns[272] ,columns[273] ,columns[274] ,columns[275] ,columns[276] ,columns[277] ,columns[278] ,columns[279] ,columns[280] ,columns[281] ,columns[282] ,columns[283] ,columns[284] ,columns[285] ,columns[286] ,columns[287] ,columns[288] ,columns[289] ,columns[290] ,columns[291] ,columns[292] ,columns[293] ,columns[294] ,columns[295] ,columns[296] ,columns[297] ,columns[298] ,columns[299] ,columns[300] ,columns[301] ,columns[302] ,columns[303] ,columns[304] ,columns[305] ,columns[306] ,columns[307] ,columns[308] ,columns[309] ,columns[310] ,columns[311] ,columns[312] ,columns[313] ,columns[314] ,columns[315] ,columns[316] ,columns[317] ,columns[318] ,columns[319] ,columns[320] ,columns[321] ,columns[322] ,columns[323] ,columns[324] ,columns[325] ,columns[326] ,columns[327] ,columns[328] ,columns[329] ,columns[330] ,columns[331] ,columns[332] ,columns[333] ,columns[334] ,columns[335] ,columns[336] ,columns[337] ,columns[338] ,columns[339] ,columns[340] ,columns[341] ,columns[342] ,columns[343] ,columns[344] ,columns[345] ,columns[346] ,columns[347] ,columns[348] ,columns[349] ,columns[350] ,columns[351] ,columns[352] ,columns[353] ,columns[354] ,columns[355] ,columns[356] ,columns[357] ,columns[358] ,columns[359] ,columns[360] ,columns[361] ,columns[362] ,columns[363] ,columns[364] ,columns[365] ,columns[366] ,columns[367] ,columns[368] ,columns[369] ,columns[370] ,columns[371] ,columns[372] ,columns[373] ,columns[374] ,columns[375] ,columns[376] ,columns[377] ,columns[378] ,columns[379] ,columns[380] ,columns[381] ,columns[382] ,columns[383] ,columns[384] ,columns[385] ,columns[386] ,columns[387] ,columns[388] ,columns[389] ,columns[390] ,columns[391] ,columns[392] ,columns[393] ,columns[394] ,columns[395] ,columns[396] ,columns[397] ,columns[398] ,columns[399] ,columns[400] ,columns[401] ,columns[402] ,columns[403] ,columns[404] ,columns[405] ,columns[406] ,columns[407] ,columns[408] ,columns[409] ,columns[410] ,columns[411] ,columns[412] ,columns[413] ,columns[414] ,columns[415] ,columns[416] ,columns[417] ,columns[418] ,columns[419] ,columns[420] ,columns[421] ,columns[422] ,columns[423] ,columns[424] ,columns[425] ,columns[426] ,columns[427] ,columns[428] ,columns[429] ,columns[430] ,columns[431] ,columns[432] ,columns[433] ,columns[434] ,columns[435] ,columns[436] ,columns[437] ,columns[438] ,columns[439] ,columns[440] ,columns[441] ,columns[442] ,columns[443] ,columns[444] ,columns[445] ,columns[446] ,columns[447] ,columns[448] ,columns[449] ,columns[450] ,columns[451] ,columns[452] ,columns[453] ,columns[454] ,columns[455] ,columns[456] ,columns[457] ,columns[458] ,columns[459] ,columns[460] ,columns[461] ,columns[462] ,columns[463] ,columns[464] ,columns[465] ,columns[466] ,columns[467] ,columns[468] ,columns[469] ,columns[470] ,columns[471] ,columns[472] ,columns[473] ,columns[474] ,columns[475] ,columns[476] ,columns[477] ,columns[478] ,columns[479] ,columns[480] ,columns[481] ,columns[482] ,columns[483] ,columns[484] ,columns[485] ,columns[486] ,columns[487] ,columns[488] ,columns[489] ,columns[490] ,columns[491] ,columns[492] ,columns[493] ,columns[494] ,columns[495] ,columns[496] ,columns[497] ,columns[498] ,columns[499] ,columns[500] ,columns[501] ,columns[502] ,columns[503] ,columns[504] ,columns[505] ,columns[506] ,columns[507] ,columns[508] ,columns[509] ,columns[510] ,columns[511] ,columns[512] ,columns[513] ,columns[514] ,columns[515] ,columns[516] ,columns[517] ,columns[518] ,columns[519] ,columns[520] ,columns[521] ,columns[522] ,columns[523] ,columns[524] ,columns[525] ,columns[526] ,columns[527] ,columns[528] ,columns[529] ,columns[530] ,columns[531] ,columns[532] ,columns[533] ,columns[534] ,columns[535] ,columns[536] ,columns[537] ,columns[538] ,columns[539] ,columns[540] ,columns[541] ,columns[542] ,columns[543] ,columns[544] ,columns[545] ,columns[546] ,columns[547] ,columns[548] ,columns[549] ,columns[550] ,columns[551] ,columns[552] ,columns[553] ,columns[554] ,columns[555] ,columns[556] ,columns[557] ,columns[558] ,columns[559] ,columns[560] ,columns[561] ,columns[562] ,columns[563] ,columns[564] ,columns[565] ,columns[566] ,columns[567] ,columns[568] ,columns[569] ,columns[570] ,columns[571] ,columns[572] ,columns[573] ,columns[574] ,columns[575] ,columns[576] ,columns[577] ,columns[578] ,columns[579] ,columns[580] ,columns[581] ,columns[582] ,columns[583] ,columns[584] ,columns[585] ,columns[586] ,columns[587] ,columns[588] ,columns[589] ,columns[590] ,columns[591] ,columns[592] ,columns[593] ,columns[594] ,columns[595] ,columns[596] ,columns[597] ,columns[598] ,columns[599] ,columns[600] ,columns[601] ,columns[602] ,columns[603] ,columns[604] ,columns[605] ,columns[606] ,columns[607] ,columns[608] ,columns[609] ,columns[610] ,columns[611] ,columns[612] ,columns[613] ,columns[614] ,columns[615] ,columns[616] ,columns[617] ,columns[618] ,columns[619] ,columns[620] ,columns[621] ,columns[622] ,columns[623] ,columns[624] ,columns[625] ,columns[626] ,columns[627] ,columns[628] ,columns[629] ,columns[630] ,columns[631] ,columns[632] ,columns[633] ,columns[634] ,columns[635] ,columns[636] ,columns[637] ,columns[638] ,columns[639] ,columns[640] ,columns[641] ,columns[642] ,columns[643] ,columns[644] ,columns[645] ,columns[646] ,columns[647] ,columns[648] ,columns[649] ,columns[650] ,columns[651] ,columns[652] ,columns[653] ,columns[654] ,columns[655] ,columns[656] ,columns[657] ,columns[658] ,columns[659] ,columns[660] ,columns[661] ,columns[662] ,columns[663] ,columns[664] ,columns[665] ,columns[666] ,columns[667] ,columns[668] ,columns[669] ,columns[670] ,columns[671] ,columns[672] ,columns[673] ,columns[674] ,columns[675] ,columns[676] ,columns[677] ,columns[678] ,columns[679] ,columns[680] ,columns[681] ,columns[682] ,columns[683] ,columns[684] ,columns[685] ,columns[686] ,columns[687] ,columns[688] ,columns[689] ,columns[690] ,columns[691] ,columns[692] ,columns[693] ,columns[694] ,columns[695] ,columns[696] ,columns[697] ,columns[698] ,columns[699] ,columns[700] ,columns[701] ,columns[702] ,columns[703] ,columns[704] ,columns[705] ,columns[706] ,columns[707] ,columns[708] ,columns[709] ,columns[710] ,columns[711] ,columns[712] ,columns[713] ,columns[714] ,columns[715] ,columns[716] ,columns[717] ,columns[718] ,columns[719] ,columns[720] ,columns[721] ,columns[722] ,columns[723] ,columns[724] ,columns[725] ,columns[726] ,columns[727] ,columns[728] ,columns[729] ,columns[730] ,columns[731] ,columns[732] ,columns[733] ,columns[734] ,columns[735] ,columns[736] ,columns[737] ,columns[738] ,columns[739] ,columns[740] ,columns[741] ,columns[742] ,columns[743] ,columns[744] ,columns[745] ,columns[746] ,columns[747] ,columns[748] ,columns[749] ,columns[750] ,columns[751] ,columns[752] ,columns[753] ,columns[754] ,columns[755] ,columns[756] ,columns[757] ,columns[758] ,columns[759] ,columns[760] ,columns[761] ,columns[762] ,columns[763] ,columns[764] ,columns[765] ,columns[766] ,columns[767] ,columns[768] ,columns[769] ,columns[770] ,columns[771] ,columns[772] ,columns[773] ,columns[774] ,columns[775] ,columns[776] ,columns[777] ,columns[778] ,columns[779] ,columns[780] ,columns[781] ,columns[782] ,columns[783] ,columns[784] ,columns[785] ,columns[786] ,columns[787] ,columns[788] ,columns[789] ,columns[790] ,columns[791] ,columns[792] ,columns[793] ,columns[794] ,columns[795] ,columns[796] ,columns[797] ,columns[798] ,columns[799] ,columns[800] ,columns[801] ,columns[802] ,columns[803] ,columns[804] ,columns[805] ,columns[806] ,columns[807] ,columns[808] ,columns[809] ,columns[810] ,columns[811] ,columns[812] ,columns[813] ,columns[814] ,columns[815] ,columns[816] ,columns[817] ,columns[818] ,columns[819] ,columns[820] ,columns[821] ,columns[822] ,columns[823] ,columns[824] ,columns[825] ,columns[826] ,columns[827] ,columns[828] ,columns[829] ,columns[830] ,columns[831] ,columns[832] ,columns[833] ,columns[834] ,columns[835] ,columns[836] ,columns[837] ,columns[838] ,columns[839] ,columns[840] ,columns[841] ,columns[842] ,columns[843] ,columns[844] ,columns[845] ,columns[846] ,columns[847] ,columns[848] ,columns[849] ,columns[850] ,columns[851] ,columns[852] ,columns[853] ,columns[854] ,columns[855] ,columns[856] ,columns[857] ,columns[858] ,columns[859] ,columns[860] ,columns[861] ,columns[862] ,columns[863] ,columns[864] ,columns[865] ,columns[866] ,columns[867] ,columns[868] ,columns[869] ,columns[870] ,columns[871] ,columns[872] ,columns[873] ,columns[874] ,columns[875] ,columns[876] ,columns[877] ,columns[878] ,columns[879] ,columns[880] ,columns[881] ,columns[882] ,columns[883] ,columns[884] ,columns[885] ,columns[886] ,columns[887] ,columns[888] ,columns[889] ,columns[890] ,columns[891] ,columns[892] ,columns[893] ,columns[894] ,columns[895] ,columns[896] ,columns[897] ,columns[898] ,columns[899] ,columns[900] ,columns[901] ,columns[902] ,columns[903] ,columns[904] ,columns[905] ,columns[906] ,columns[907] ,columns[908] ,columns[909] ,columns[910] ,columns[911] ,columns[912] ,columns[913] ,columns[914] ,columns[915] ,columns[916] ,columns[917] ,columns[918] ,columns[919] ,columns[920] ,columns[921] ,columns[922] ,columns[923] ,columns[924] ,columns[925] ,columns[926] ,columns[927] ,columns[928] ,columns[929] ,columns[930] ,columns[931] ,columns[932] ,columns[933] ,columns[934] ,columns[935] ,columns[936] ,columns[937] ,columns[938] ,columns[939] ,columns[940] ,columns[941] ,columns[942] ,columns[943] ,columns[944] ,columns[945] ,columns[946] ,columns[947] ,columns[948] ,columns[949] ,columns[950] ,columns[951] ,columns[952] ,columns[953] ,columns[954] ,columns[955] ,columns[956] ,columns[957] ,columns[958] ,columns[959] ,columns[960] ,columns[961] ,columns[962] ,columns[963] ,columns[964] ,columns[965] ,columns[966] ,columns[967] ,columns[968] ,columns[969] ,columns[970] ,columns[971] ,columns[972] ,columns[973] ,columns[974] ,columns[975] ,columns[976] ,columns[977] ,columns[978] ,columns[979] ,columns[980] ,columns[981] ,columns[982] ,columns[983] ,columns[984] ,columns[985] ,columns[986] ,columns[987] ,columns[988] ,columns[989] ,columns[990] ,columns[991] ,columns[992] ,columns[993] ,columns[994] ,columns[995] ,columns[996] ,columns[997] ,columns[998] ,columns[999] ,columns[1000] ,columns[1001] ,columns[1002] ,columns[1003] ,columns[1004] ,columns[1005] ,columns[1006] ,columns[1007] ,columns[1008] ,columns[1009] ,columns[1010] ,columns[1011] ,columns[1012] ,columns[1013] ,columns[1014] ,columns[1015] ,columns[1016] ,columns[1017] ,columns[1018] ,columns[1019] ,columns[1020] ,columns[1021] ,columns[1022] ,columns[1023] ,columns[1024] ,columns[1025] ,columns[1026] ,columns[1027] ,columns[1028] ,columns[1029] ,columns[1030] ,columns[1031] ,columns[1032] ,columns[1033] ,columns[1034] ,columns[1035] ,columns[1036] ,columns[1037] ,columns[1038] ,columns[1039] ,columns[1040] ,columns[1041] ,columns[1042] ,columns[1043] ,columns[1044] ,columns[1045] ,columns[1046] ,columns[1047] ,columns[1048] ,columns[1049] ,columns[1050] ,columns[1051] ,columns[1052] ,columns[1053] ,columns[1054] ,columns[1055] ,columns[1056] ,columns[1057] ,columns[1058] ,columns[1059] ,columns[1060] ,columns[1061] ,columns[1062] ,columns[1063] ,columns[1064] ,columns[1065] ,columns[1066] ,columns[1067] ,columns[1068] ,columns[1069] ,columns[1070] ,columns[1071] ,columns[1072] ,columns[1073] ,columns[1074] ,columns[1075] ,columns[1076] ,columns[1077] ,columns[1078] ,columns[1079] ,columns[1080] ,columns[1081] ,columns[1082] ,columns[1083] ,columns[1084] ,columns[1085] ,columns[1086] ,columns[1087] ,columns[1088] ,columns[1089] ,columns[1090] ,columns[1091] ,columns[1092] ,columns[1093] ,columns[1094] ,columns[1095] ,columns[1096] ,columns[1097] ,columns[1098] ,columns[1099] ,columns[1100] ,columns[1101] ,columns[1102] ,columns[1103] ,columns[1104] ,columns[1105] ,columns[1106] ,columns[1107] ,columns[1108] ,columns[1109] ,columns[1110] ,columns[1111] ,columns[1112] ,columns[1113] ,columns[1114] ,columns[1115] ,columns[1116] ,columns[1117] ,columns[1118] ,columns[1119] ,columns[1120] ,columns[1121] ,columns[1122] ,columns[1123] ,columns[1124] ,columns[1125] ,columns[1126] ,columns[1127] ,columns[1128] ,columns[1129] ,columns[1130] ,columns[1131] ,columns[1132] ,columns[1133] ,columns[1134] ,columns[1135] ,columns[1136] ,columns[1137] ,columns[1138] ,columns[1139] ,columns[1140] ,columns[1141] ,columns[1142] ,columns[1143] ,columns[1144] ,columns[1145] ,columns[1146] ,columns[1147] ,columns[1148] ,columns[1149] ,columns[1150] ,columns[1151] ,columns[1152] ,columns[1153] ,columns[1154] ,columns[1155] ,columns[1156] ,columns[1157] ,columns[1158] ,columns[1159] ,columns[1160] ,columns[1161] ,columns[1162] ,columns[1163] ,columns[1164] ,columns[1165] ,columns[1166] ,columns[1167] ,columns[1168] ,columns[1169] ,columns[1170] ,columns[1171] ,columns[1172] ,columns[1173] ,columns[1174] ,columns[1175] ,columns[1176] ,columns[1177] ,columns[1178] ,columns[1179] ,columns[1180] ,columns[1181] ,columns[1182] ,columns[1183] ,columns[1184] ,columns[1185] ,columns[1186] ,columns[1187] ,columns[1188] ,columns[1189] ,columns[1190] ,columns[1191] ,columns[1192] ,columns[1193] ,columns[1194] ,columns[1195] ,columns[1196] ,columns[1197] ,columns[1198] ,columns[1199] ,columns[1200] from dfs.`F:\\drill\\files\\3.csv`) t ");
              client.queryBuilder().sql(sb.toString()).printCsv();
    }
  }

  @Test
  public void t2() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher)
        // Easy way to run single threaded for easy debugging
        .maxParallelization(1)
        // Set some session options
        .sessionOption(ExecConstants.ENABLE_VERBOSE_ERRORS_KEY, true);

    String sql ="select columns[1] ,columns[2] from dfs.root.`files\\3.csv`\n" +
        "union\n" +
        "select columns[1] ,columns[2] from dfs.root.`files\\3.csv`";

    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "root", "D:\\drill", "parquet");

      client.queryBuilder().sql(sql).printCsv();

/*      String plan = client.queryBuilder().sql(sql).explainText();
      System.out.println(plan);*/
    }
  }

  @Test
  public void queryRunner() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher)
            // Easy way to run single threaded for easy debugging
            .maxParallelization(1)
            // Set some session options
            .sessionOption(ExecConstants.ENABLE_VERBOSE_ERRORS_KEY, true);

    String sql = generateQuery(1200);

    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "root", "D:\\drill", "parquet");

    client.queryBuilder().sql(sql).printCsv();

/*      String plan = client.queryBuilder().sql(sql).explainText();
      System.out.println(plan);*/
    }
  }

  @Test
  public void checkSort() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher)
        // Easy way to run single threaded for easy debugging
        .maxParallelization(1)
            // Set some session options
        .sessionOption(ExecConstants.ENABLE_VERBOSE_ERRORS_KEY, true);

    String sql = generateSortQuery(12000);

    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "root", "D:\\drill", "parquet");

      client.queryBuilder().sql(sql).printCsv();

/*      String plan = client.queryBuilder().sql(sql).explainText();
      System.out.println(plan);*/
    }
  }

  public String generateSortQuery(int numberOfColumns) {
    String tableName = "dfs.root.`files\\3.csv`";
    String queryFormat = "select %s from %s order by %s";

    StringBuilder columnsAndAlias = new StringBuilder();
    StringBuilder aliasOnly = new StringBuilder();
    for (int i = 1; i <= numberOfColumns; i++) {
      String column = "columns[" + i + "]";
      String alias = "column_" + i;
      columnsAndAlias.append(column).append(" as ").append(alias).append(",");
      aliasOnly.append(alias).append(",");
    }
    // strip last comma
    columnsAndAlias.deleteCharAt(columnsAndAlias.length() - 1);
    aliasOnly.deleteCharAt(aliasOnly.length() - 1);

    String query = String.format(queryFormat, columnsAndAlias.toString(), tableName, aliasOnly.toString());
    return query;
  }

  public String generateQuery(int numberOfColumns) {
    String tableName = "dfs.root.`files\\3.csv`";
    String initialQueryFormat = "select %s from %s";
    String queryFormat = "select distinct %s from (%s union all %s) t";

    StringBuilder columnsAndAlias = new StringBuilder();
    StringBuilder aliasOnly = new StringBuilder();
    for (int i = 1; i <= numberOfColumns; i++) {
      String column = "columns[" + i + "]";
      String alias = "column_" + i;
      columnsAndAlias.append(column).append(" as ").append(alias).append(",");
      aliasOnly.append(alias).append(",");
    }
    // strip last comma
    columnsAndAlias.deleteCharAt(columnsAndAlias.length() - 1);
    aliasOnly.deleteCharAt(aliasOnly.length() - 1);

    String initialQuery = String.format(initialQueryFormat, columnsAndAlias.toString(), tableName);
    String query = String.format(queryFormat, aliasOnly.toString(), initialQuery, initialQuery);

   // System.out.println(query);
    return query;
  }
}

/*
UNION ALL
00-00    Screen
00-01      Project(EXPR$0=[$0], EXPR$1=[$1])
00-02        UnionAll(all=[true])
00-04          Project(EXPR$0=[ITEM($0, 1)], EXPR$1=[ITEM($0, 2)])
00-06            Scan(groupscan=[EasyGroupScan [selectionRoot=file:/F:/drill/files/3.csv, numFiles=1, columns=[`columns`[1], `columns`[2]], files=[file:/F:/drill/files/3.csv]]])
00-03          Project(EXPR$0=[ITEM($0, 1)], EXPR$1=[ITEM($0, 2)])
00-05            Scan(groupscan=[EasyGroupScan [selectionRoot=file:/F:/drill/files/3.csv, numFiles=1, columns=[`columns`[1], `columns`[2]], files=[file:/F:/drill/files/3.csv]]])

UNION
00-00    Screen
00-01      Project(EXPR$0=[$0], EXPR$1=[$1])
00-02        HashAgg(group=[{0, 1}])
00-03          UnionAll(all=[true])
00-05            Project(EXPR$0=[ITEM($0, 1)], EXPR$1=[ITEM($0, 2)])
00-07              Scan(groupscan=[EasyGroupScan [selectionRoot=file:/F:/drill/files/3.csv, numFiles=1, columns=[`columns`[1], `columns`[2]], files=[file:/F:/drill/files/3.csv]]])
00-04            Project(EXPR$0=[ITEM($0, 1)], EXPR$1=[ITEM($0, 2)])
00-06              Scan(groupscan=[EasyGroupScan [selectionRoot=file:/F:/drill/files/3.csv, numFiles=1, columns=[`columns`[1], `columns`[2]], files=[file:/F:/drill/files/3.csv]]])

 */
