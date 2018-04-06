/*
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
package org.apache.drill.exec;

import org.apache.drill.PlanTestBase;
import org.apache.drill.categories.HiveStorageTest;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.exec.hive.HiveTestBase;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

@Category({SlowTest.class, HiveStorageTest.class})
public class TestHiveDrillNativeReader extends HiveTestBase {

  //todo Java 8 enhancements, replace for loops with streams...

  //todo check stats calculation in native reader vs current by parquet
  //todo current impl does not show

    /*
  Test cases to check:
    1. item star operator re-write + (works with simple sub-select)
    2. partition pruning based on Hive partitions (applied earlier on logical stage): check it was actually applied
    3. partition pruning via Drill -
    4. ser / de : group scan +, row group scan
    5. empty hive table + / maybe empty partitions + (empty partition is allowed to be added, but ignored)
    6. external simple Hive table + statistics --> calculate how many bytes in one row boolean file....
    7. external partitions in different locations (external) +
    8. limit push down without filter +
    9. project push down +
    10. count to direct scan optimization +
    11. partitioned table managed (not external) +
    12. check with nested partitions +
    13. check parallelization is working fine (need to check on the cluster, that several drillbits can read data)
   */

  @BeforeClass
  public static void init() {
    setSessionOption(ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS, true);
  }


  @AfterClass
  public static void cleanup() {
    resetSessionOption(ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS);
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testFilterPushDown() throws Exception {
    String query = "select `key` from hive.kv_push where key > 1";

    // work fine on physical and logical convert

    String plan = PlanTestBase.getPlanInString("explain plan for " + query, PlanTestBase.OPTIQ_FORMAT);
    System.out.println(plan);
    List<QueryDataBatch> res = testSqlWithResults(query);
    printResult(res);
  }

  @Test
  public void testPartitionPruning() throws Exception {
    String query = "select `key` from hive.kv_push where key = 2";
    //String query = "select * from hive.kv_push where key = 2 limit 2";
    //String query = "select * from hive.kv_push limit 2"; // work without filter only, which is expected

    // does not work at all

    String plan = PlanTestBase.getPlanInString("explain plan for " + query, PlanTestBase.OPTIQ_FORMAT);
    System.out.println(plan);
    List<QueryDataBatch> res = testSqlWithResults(query);
    printResult(res);
  }



  @Test
  public void testSimpleItemStarSubqueryFilterPushDown() throws Exception {
    //String query = "select * from (select * from hive.kv_push) where key > 1";
    //String query = "select * from (select *, sub_key as sk from hive.kv_push) where key > 1";
    String query = "select * from (select * from (select * from hive.kv_push)) where key > 1";

    // work fine on physical and logical convert

    String plan = PlanTestBase.getPlanInString("explain plan for " + query, PlanTestBase.OPTIQ_FORMAT);
    System.out.println(plan);
    List<QueryDataBatch> res = testSqlWithResults(query);
    printResult(res);
  }

  @Test
  public void testExternal() throws Exception {
    String query = "select * from hive.kv_push_ext";
    String plan = PlanTestBase.getPlanInString("explain plan for " + query, PlanTestBase.OPTIQ_FORMAT);
    System.out.println(plan);
    List<QueryDataBatch> res = testSqlWithResults(query);
    printResult(res);
  }

  // project push down

  @Test
  public void testPhysicalPlanSubmission() throws Exception {
    // checks only group scan
    PlanTestBase.testPhysicalPlanExecutionBasedOnQuery("select * from hive.kv_native_push");
    //todo need to check row group scan
  }

  @Test
  public void testLimitPushDown() throws Exception {
    String query = "select * from hive.kv_native limit 2";

    testPlanMatchingPatterns(query, new String[]{"HiveDrillNativeParquetScan", "numFiles=1"}, new String[]{});

    test

  }

  @Test
  public void testEmptyTable() throws Exception {
    String query = "select * from hive.kv_native_empty";
    // Hive reader should be chosen to output the schema
    testPlanMatchingPatterns(query, new String[]{"HiveScan"}, new String[]{});
  }

  @Test
  public void testConvertCountToDirectScanOptimization() throws Exception {
    String query = "select count(1) as cnt from hive.kv_native";

    testPlanMatchingPatterns(query, new String[]{"DynamicPojoRecordReader"}, new String[]{});

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("cnt")
      .baselineValues(8L)
      .go();
  }

  @Test
  public void testImplicitColumns() throws Exception {
    thrown.expect(UserRemoteException.class);
    thrown.expectMessage(CoreMatchers.allOf(containsString("VALIDATION ERROR"), containsString("not found in any table")));

    test("select *, filename, fqn, filepath, suffix from hive.kv_native_push");
  }


  /*

  1. simple item star has worked
  2. with additional column not
  3. nested subquery

00-00    Screen
00-01      Project(key=[$0], sub_key=[$1], sub_key0=[$2])
00-02        SelectionVectorRemover
00-03          Filter(condition=[>($0, 2)])
00-04            Project(key=[$0], sub_key=[$1], sub_key0=[$1])
00-05              Project(key=[$0], sub_key=[$1])
00-06                Scan(groupscan=[HiveDrillNativeParquetScan [entries=[ReadEntryWithPath [path=/home/arina/git_repo/drill/contrib/storage-hive/core/target/org.apache.drill.exec.TestHiveDrillNativeReader/root/warehouse/kv_push/000000_0], ReadEntryWithPath [path=/home/arina/git_repo/drill/contrib/storage-hive/core/target/org.apache.drill.exec.TestHiveDrillNativeReader/root/warehouse/kv_push/000000_0_copy_1], ReadEntryWithPath [path=/home/arina/git_repo/drill/contrib/storage-hive/core/target/org.apache.drill.exec.TestHiveDrillNativeReader/root/warehouse/kv_push/000000_0_copy_2], ReadEntryWithPath [path=/home/arina/git_repo/drill/contrib/storage-hive/core/target/org.apache.drill.exec.TestHiveDrillNativeReader/root/warehouse/kv_push/000000_0_copy_3]], numFiles=4, numRowGroups=4, columns=[`key`, `sub_key`]]])

is not working because has two projects...
and no item star project...

Do we have a rule to simplify two projects?

   */

}


/*
00-00    Screen
00-01      Project(key=[$0], sub_key=[$1])
00-02        SelectionVectorRemover
00-03          Limit(fetch=[2])
00-04            Filter(condition=[=($0, 2)])
00-05              Scan(groupscan=[HiveScan [table=Table(dbName:default, tableName:kv_push), columns=[`**`], numPartitions=0, partitions= null, inputDirectories=[file:/home/arina/git_repo/drill/contrib/storage-hive/core/target/org.apache.drill.exec.TestHiveDrillNativeReader/root/warehouse/kv_push]]])


00-00    Screen
00-01      Project(key=[$0], sub_key=[$1])
00-02        SelectionVectorRemover
00-03          Limit(fetch=[2])
00-04            Filter(condition=[=($0, 2)])
00-05              Project(key=[$0], sub_key=[$1])
00-06                Scan(groupscan=[HiveDrillNativeParquetScan [entries=[ReadEntryWithPath [path=/home/arina/git_repo/drill/contrib/storage-hive/core/target/org.apache.drill.exec.TestHiveDrillNativeReader/root/warehouse/kv_push/000000_0_copy_2]], numFiles=1, numRowGroups=1, columns=[`**`]]])


 */