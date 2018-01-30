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
package org.apache.drill.exec.store.parquet;

import org.apache.drill.PlanTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.drill.exec.util.StoragePluginTestUtils.DFS_TMP_SCHEMA;

public class TestPushDownAndPruningWithItemStar extends PlanTestBase {

  private static final String TABLE_NAME = "order_ctas";

  @BeforeClass
  public static void setup() throws Exception {
    test("create table `%s`.`%s/t1` as select cast(o_orderdate as date) as o_orderdate from cp.`tpch/orders.parquet` " +
        "where o_orderdate between date '1992-01-01' and date '1992-01-03'", DFS_TMP_SCHEMA, TABLE_NAME);
    test("create table `%s`.`%s/t2` as select cast(o_orderdate as date) as o_orderdate from cp.`tpch/orders.parquet` " +
        "where o_orderdate between date '1992-01-04' and date '1992-01-06'", DFS_TMP_SCHEMA, TABLE_NAME);
    test("create table `%s`.`%s/t3` as select cast(o_orderdate as date) as o_orderdate from cp.`tpch/orders.parquet` " +
        "where o_orderdate between date '1992-01-07' and date '1992-01-09'", DFS_TMP_SCHEMA, TABLE_NAME);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    test("drop table if exists `%s`.`%s`", DFS_TMP_SCHEMA, TABLE_NAME);
  }

  @Test
  public void testPushProjectIntoScan() throws Exception {
    String query = String.format("select o_orderdate, count(*) from (select * from `%s`.`%s`) group by o_orderdate", DFS_TMP_SCHEMA, TABLE_NAME);

    String[] expectedPlan = {"numFiles=3, numRowGroups=3, usedMetadataFile=false, columns=\\[`o_orderdate`\\]"};
    String[] excludedPlan = {};

    PlanTestBase.testPlanMatchingPatterns(query, expectedPlan, excludedPlan);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .sqlBaselineQuery("select o_orderdate, count(*) from `%s`.`%s` group by o_orderdate", DFS_TMP_SCHEMA, TABLE_NAME)
        .build();
  }

  @Test
  public void testDirectoryPruning() throws Exception {
    String query = String.format("select * from (select * from `%s`.`%s`) where dir0 = 't1'", DFS_TMP_SCHEMA, TABLE_NAME);

    String[] expectedPlan = {"numFiles=1, numRowGroups=1, usedMetadataFile=false, columns=\\[`\\*\\*`\\]"};
    String[] excludedPlan = {};

    PlanTestBase.testPlanMatchingPatterns(query, expectedPlan, excludedPlan);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .sqlBaselineQuery("select * from `%s`.`%s` where dir0 = 't1'", DFS_TMP_SCHEMA, TABLE_NAME)
        .build();
  }

  @Test
  public void testFilterPushDownSingleCondition() throws Exception {
    String query = String.format("select * from (select * from `%s`.`%s`) where o_orderdate = date '1992-01-01'", DFS_TMP_SCHEMA, TABLE_NAME);

    String[] expectedPlan = {"numFiles=1, numRowGroups=1, usedMetadataFile=false, columns=\\[`\\*\\*`\\]"};
    String[] excludedPlan = {};

    PlanTestBase.testPlanMatchingPatterns(query, expectedPlan, excludedPlan);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .sqlBaselineQuery("select * from `%s`.`%s` where o_orderdate = date '1992-01-01'", DFS_TMP_SCHEMA, TABLE_NAME)
        .build();
  }

  @Test
  public void testFilterPushDownMultipleConditions() throws Exception {
    String query = String.format("select * from (select * from `%s`.`%s`) where o_orderdate = date '1992-01-01' or o_orderdate = date '1992-01-09'",
        DFS_TMP_SCHEMA, TABLE_NAME);

    String[] expectedPlan = {"numFiles=2, numRowGroups=2, usedMetadataFile=false, columns=\\[`\\*\\*`\\]"};
    String[] excludedPlan = {};

    PlanTestBase.testPlanMatchingPatterns(query, expectedPlan, excludedPlan);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .sqlBaselineQuery("select * from `%s`.`%s` where o_orderdate = date '1992-01-01' or o_orderdate = date '1992-01-09'",
            DFS_TMP_SCHEMA, TABLE_NAME)
        .build();
  }

}
