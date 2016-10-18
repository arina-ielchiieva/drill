/**
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
package org.apache.drill.exec.sql;

import org.apache.commons.io.FileUtils;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.junit.Test;

import java.io.File;
import java.util.List;

public class TestCTTAS extends BaseTestQuery {

  @Test
  public void test() throws Exception {
    final String newTblName = "nation_ctas";

    //test("ALTER SESSION SET `store.format`='json';");

    try {
/*      final String ctasQuery = String.format("CREATE temporary TABLE %s.%s   " +
              "partition by (n_regionkey) AS SELECT n_nationkey, n_regionkey from cp.`tpch/nation.parquet` order by n_nationkey limit 1",
          TEMP_SCHEMA, newTblName);*/

      final String ctasQuery = String.format("CREATE temporary TABLE %s.%s   " +
              "AS SELECT n_nationkey, n_regionkey from cp.`tpch/nation.parquet` order by n_nationkey limit 1",
          TEMP_SCHEMA, newTblName);

      setColumnWidths(new int[] {40});
      List<QueryDataBatch> res = testSqlWithResults(ctasQuery);
      printResult(res);

      final String ctasQuery1 = String.format("CREATE TABLE %s.%s   " + "partition by (n_regionkey) AS SELECT n_nationkey, n_regionkey from cp.`tpch/nation.parquet` " +
          "order by n_nationkey limit 1", TEMP_SCHEMA, "mytable");
      // test(ctasQuery1);

      //final String selectFromCreatedTable = String.format(" select * from %s.%s", TEMP_SCHEMA, newTblName + "_arina");
      final String selectFromCreatedTable = String.format(" select * from %s.%s", TEMP_SCHEMA, newTblName);
      final String baselineQuery = "select n_nationkey, n_regionkey from cp.`tpch/nation.parquet` order by n_nationkey limit 1";

      testBuilder()
          .sqlQuery(selectFromCreatedTable)
          .ordered()
          .sqlBaselineQuery(baselineQuery)
          .build()
          .run();

      setColumnWidths(new int[] {40});
      List<QueryDataBatch> res2 = testSqlWithResults(String.format("drop table %s.%s", TEMP_SCHEMA, newTblName));
      printResult(res2);

/*      final String selectFromCreatedTable2 = String.format(" select * from %s.%s", TEMP_SCHEMA, newTblName + "_" + "1");

      testBuilder()
          .sqlQuery(selectFromCreatedTable2)
          .ordered()
          .sqlBaselineQuery(baselineQuery)
          .build()
          .run();*/

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test
  public void testNewCTTAS() throws Exception {
    final String newTblName = "nation_ctas";
/*    final String ctasQuery = String.format("CREATE temporary TABLE %s.%s   " +
            "AS SELECT n_nationkey, n_regionkey from cp.`tpch/nation.parquet` order by n_nationkey limit 1",
        TEMP_SCHEMA, newTblName);*/

    final String ctasQuery = String.format("CREATE temporary TABLE %s   " +
        "AS SELECT n_nationkey, n_regionkey from cp.`tpch/nation.parquet` order by n_nationkey limit 1",newTblName);

    setColumnWidths(new int[] {40});
    List<QueryDataBatch> res = testSqlWithResults(ctasQuery);
    printResult(res);

    //final String selectFromCreatedTable = String.format(" select * from %s.%s", TEMP_SCHEMA, newTblName);
    final String selectFromCreatedTable = String.format(" select * from %s", newTblName);
    final String baselineQuery = "select n_nationkey, n_regionkey from cp.`tpch/nation.parquet` order by n_nationkey limit 1";

    testBuilder()
        .sqlQuery(selectFromCreatedTable)
        .ordered()
        .sqlBaselineQuery(baselineQuery)
        .build()
        .run();
  }

  @Test
  public void testSimpleQuery() throws Exception {
    final String newTblName = "nation_ctas";
    final String ctasQuery = String.format("CREATE temp TABLE %s   " +
        "AS SELECT n_nationkey, n_regionkey from cp.`tpch/nation.parquet` order by n_nationkey limit 1",newTblName);

    setColumnWidths(new int[] {40});
    List<QueryDataBatch> res = testSqlWithResults(ctasQuery);
    printResult(res);

    System.out.println("ARINA: start check");
    //final String selectFromCreatedTable = String.format(" select * from %s", newTblName);
    final String selectFromCreatedTable = String.format(" select t1.n_nationkey," +
        " t2.n_regionkey from %s t1 join nation_ctas t2 on t1.n_nationkey = t2.n_nationkey", newTblName);
    res = testSqlWithResults(selectFromCreatedTable);
    printResult(res);
    System.out.println("ARINA: end check");
  }

  @Test
  public void testBenchmark() throws Exception {
    test("use dfs");
    String query = "select t1.columns[0] from `/home/osboxes/files/dirN/dir1` t1" +
        " join `/home/osboxes/files/dirN/dir2` t2 on t1.columns[0] = t2.columns[0]" +
        " join `/home/osboxes/files/dirN/dir3` t3 on t3.columns[0] = t2.columns[0]" +
        " join `/home/osboxes/files/dirN/dir4` t4 on t4.columns[0] = t2.columns[0]" +
        " join `/home/osboxes/files/dirN/dir5` t5 on t5.columns[0] = t2.columns[0]" +
        " join `/home/osboxes/files/dirN/dir6` t6 on t6.columns[0] = t2.columns[0]" +
        " join `/home/osboxes/files/dirN/dir7` t7 on t7.columns[0] = t2.columns[0]" +
        " join `/home/osboxes/files/dirN/dir8` t8 on t8.columns[0] = t2.columns[0]" +
        " join `/home/osboxes/files/dirN/dir9` t9 on t9.columns[0] = t2.columns[0]" +
        " join `/home/osboxes/files/dirN/dir10` t10 on t10.columns[0] = t2.columns[0]";
    setColumnWidths(new int[] {40});
    List<QueryDataBatch> res = testSqlWithResults(query);
    printResult(res);
  }


}
