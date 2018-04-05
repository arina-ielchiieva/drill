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
package org.apache.drill.exec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.drill.PlanTestBase;
import org.apache.drill.categories.HiveStorageTest;
import org.apache.drill.exec.hive.HiveTestBase;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;

@Category({SlowTest.class, HiveStorageTest.class})
public class TestHiveProjectPushDown extends HiveTestBase {

  // enable decimal data type
  @BeforeClass
  public static void enableDecimalDataType() throws Exception {
    test(String.format("alter session set `%s` = true", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
  }

  @AfterClass
  public static void disableDecimalDataType() throws Exception {
    test(String.format("alter session set `%s` = false", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
  }

  private void testHelper(String query, int expectedRecordCount, String... expectedSubstrs)throws Exception {
    testPhysicalPlan(query, expectedSubstrs);

    int actualRecordCount = testSql(query);
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);
  }

  @Test
  public void testSingleColumnProject() throws Exception {
    String query = "SELECT `value` as v FROM hive.`default`.kv";
    String expectedColNames = " \"columns\" : [ \"`value`\" ]";

    testHelper(query, 5, expectedColNames);
  }

  @Test
  public void testMultipleColumnsProject() throws Exception {
    String query = "SELECT boolean_field as b_f, tinyint_field as ti_f FROM hive.`default`.readtest";
    String expectedColNames = " \"columns\" : [ \"`boolean_field`\", \"`tinyint_field`\" ]";

    testHelper(query, 2, expectedColNames);
  }

  @Test
  public void testPartitionColumnProject() throws Exception {
    String query = "SELECT double_part as dbl_p FROM hive.`default`.readtest";
    String expectedColNames = " \"columns\" : [ \"`double_part`\" ]";

    testHelper(query, 2, expectedColNames);
  }

  @Test
  public void testMultiplePartitionColumnsProject() throws Exception {
    String query = "SELECT double_part as dbl_p, decimal0_part as dec_p FROM hive.`default`.readtest";
    String expectedColNames = " \"columns\" : [ \"`double_part`\", \"`decimal0_part`\" ]";

    testHelper(query, 2, expectedColNames);
  }

  @Test
  public void testPartitionAndRegularColumnProjectColumn() throws Exception {
    String query = "SELECT boolean_field as b_f, tinyint_field as ti_f, " +
        "double_part as dbl_p, decimal0_part as dec_p FROM hive.`default`.readtest";
    String expectedColNames = " \"columns\" : [ \"`boolean_field`\", \"`tinyint_field`\", " +
        "\"`double_part`\", \"`decimal0_part`\" ]";

    testHelper(query, 2, expectedColNames);
  }

  @Test
  public void testStarProject() throws Exception {
    String query = "SELECT * FROM hive.`default`.kv";
    String expectedColNames = " \"columns\" : [ \"`key`\", \"`value`\" ]";

    testHelper(query, 5, expectedColNames);
  }

  @Test
  public void testHiveCountStar() throws Exception {
    String query = "SELECT count(*) as cnt FROM hive.`default`.kv";
    String expectedColNames = "\"columns\" : [ ]";

    testHelper(query, 1, expectedColNames);
  }

  @Test
  public void projectPushDownOnHiveParquetTable() throws Exception {
    try {
      test(String.format("alter session set `%s` = true", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
      String query = "SELECT boolean_field, boolean_part, int_field, int_part FROM hive.readtest_parquet";
      String expectedColNames = "\"columns\" : [ \"`boolean_field`\", \"`dir0`\", \"`int_field`\", \"`dir9`\" ]";

      testHelper(query, 2, expectedColNames, "hive-drill-native-parquet-scan");
    } finally {
      test(String.format("alter session set `%s` = false", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
    }
  }

  @Test
  public void testPushDown() throws Exception {
    test(String.format("alter session set `%s` = true", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
    //String query = "select * from hive.readtest_parquet where int_field = 123456";
    String query = "select * from hive.readtest_parquet";
    setColumnWidths(new int[] {40});
    List<QueryDataBatch> res = testSqlWithResults(query);
    printResult(res);

    String plan = PlanTestBase.getPlanInString("explain plan for " + query, PlanTestBase.OPTIQ_FORMAT);
    System.out.println(plan);

    PlanTestBase.testPhysicalPlanExecutionBasedOnQuery(query);

    /*
00-00    Screen
00-01      Project(binary_field=[$0], boolean_field=[$1], tinyint_field=[$2], decimal0_field=[$3], decimal9_field=[$4], decimal18_field=[$5], decimal28_field=[$6], decimal38_field=[$7], double_field=[$8], float_field=[$9], int_field=[$10], bigint_field=[$11], smallint_field=[$12], string_field=[$13], varchar_field=[$14], timestamp_field=[convert_fromTIMESTAMP_IMPALA($15)], char_field=[$16], boolean_part=[CAST($17):BOOLEAN], tinyint_part=[CAST($18):INTEGER], decimal0_part=[CAST($19):DECIMAL(10, 0)], decimal9_part=[CAST($20):DECIMAL(6, 2)], decimal18_part=[CAST($21):DECIMAL(15, 5)], decimal28_part=[CAST($22):DECIMAL(23, 1)], decimal38_part=[CAST($23):DECIMAL(30, 3)], double_part=[CAST($24):DOUBLE], float_part=[CAST($25):FLOAT], int_part=[CAST($26):INTEGER], bigint_part=[CAST($27):BIGINT], smallint_part=[CAST($28):INTEGER], string_part=[CAST($29):VARCHAR(65535) CHARACTER SET "UTF-16LE" COLLATE "UTF-16LE$en_US$primary"], varchar_part=[CAST($30):VARCHAR(50) CHARACTER SET "UTF-16LE" COLLATE "UTF-16LE$en_US$primary"], timestamp_part=[CAST($31):TIMESTAMP(0)], date_part=[CAST($32):DATE], char_part=[RTRIM($33)])
00-02        Scan(groupscan=[HiveDrillNativeParquetScan [table=Table(dbName:default, tableName:readtest_parquet), columns=[`binary_field`, `boolean_field`, `tinyint_field`, `decimal0_field`, `decimal9_field`, `decimal18_field`, `decimal28_field`, `decimal38_field`, `double_field`, `float_field`, `int_field`, `bigint_field`, `smallint_field`, `string_field`, `varchar_field`, `timestamp_field`, `char_field`, `dir0`, `dir1`, `dir2`, `dir3`, `dir4`, `dir5`, `dir6`, `dir7`, `dir8`, `dir9`, `dir10`, `dir11`, `dir12`, `dir13`, `dir14`, `dir15`, `dir16`], numPartitions=2, partitions= [Partition(values:[true, 64, 37, 36.9, 3289379872.94565, 39579334534534.4, 363945093845093890.9, 8.345, 4.67, 123456, 234235, 3455, string, varchar, 2013-07-05 17:01:00.0, 2013-07-05, char      ]), Partition(values:[true, 65, 37, 36.9, 3289379872.94565, 39579334534534.4, 363945093845093890.9, 8.345, 4.67, 123456, 234235, 3455, string, varchar, 2013-07-05 17:01:00.0, 2013-07-05, char      ])], inputDirectories=[file:/home/arina/git_repo/drill/contrib/storage-hive/core/target/org.apache.drill.exec.TestHiveProjectPushDown/root/warehouse/readtest_parquet/boolean_part=true/tinyint_part=64/decimal0_part=37/decimal9_part=36.9/decimal18_part=3289379872.94565/decimal28_part=39579334534534.4/decimal38_part=363945093845093890.9/double_part=8.345/float_part=4.67/int_part=123456/bigint_part=234235/smallint_part=3455/string_part=string/varchar_part=varchar/timestamp_part=2013-07-05 17%3A01%3A00.0/date_part=2013-07-05/char_part=char      , file:/home/arina/git_repo/drill/contrib/storage-hive/core/target/org.apache.drill.exec.TestHiveProjectPushDown/root/warehouse/readtest_parquet/boolean_part=true/tinyint_part=65/decimal0_part=37/decimal9_part=36.9/decimal18_part=3289379872.94565/decimal28_part=39579334534534.4/decimal38_part=363945093845093890.9/double_part=8.345/float_part=4.67/int_part=123456/bigint_part=234235/smallint_part=3455/string_part=string/varchar_part=varchar/timestamp_part=2013-07-05 17%3A01%3A00.0/date_part=2013-07-05/char_part=char      ]]])

     */

  }

  @Test
  public void drillDecimal() throws Exception {
    try {
      test(String.format("alter session set `%s` = true", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
      test(String.format("alter session set `%s` = true", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
      String query = "SELECT * from hive.md4107_hive";

      //String expected = "a,b\n" + "a,1.00\n" + "null,null\n";
      String plan = PlanTestBase.getPlanInString("explain plan for " + query, PlanTestBase.OPTIQ_FORMAT);
      System.out.println(plan);
      List<QueryDataBatch> res = testSqlWithResults(query);
      printResult(res);
    } finally {
      test(String.format("alter session set `%s` = false", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
      test(String.format("alter session set `%s` = false", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
    }
  }

  @Test
  public void testExternal() throws Exception {
    try {
      test(String.format("alter session set `%s` = true", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
      test(String.format("alter session set `%s` = true", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
      String query = "SELECT * from hive.kv_parquet_non";

      //String expected = "a,b\n" + "a,1.00\n" + "null,null\n";
      String plan = PlanTestBase.getPlanInString("explain plan for " + query, PlanTestBase.OPTIQ_FORMAT);
      System.out.println(plan);
      List<QueryDataBatch> res = testSqlWithResults(query);
      printResult(res);
    } finally {
      test(String.format("alter session set `%s` = false", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
      test(String.format("alter session set `%s` = false", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
    }
  }

  /*
  Test cases to check:
    1. item star operator re-write
    2. partition pruning based on Hive partitions
    3. partition pruning via Drill
    4. ser / de
    5. empty hive table, maybe empty partitions
    6. external Hive table
    7. partitions in different locations
   */

}
