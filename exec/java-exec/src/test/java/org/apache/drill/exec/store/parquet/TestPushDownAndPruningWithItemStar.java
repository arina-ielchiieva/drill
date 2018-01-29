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

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.test.BaseDirTestWatcher;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.QueryBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class TestPushDownAndPruningWithItemStar {

  @ClassRule
  public static final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();

  private static ClusterFixture cluster;
  private static ClientFixture client;

  @BeforeClass
  public static void setup() {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher)
        // Easy way to run single threaded for easy debugging
        .maxParallelization(1)
        // Set some session options
        .sessionOption(ExecConstants.ENABLE_VERBOSE_ERRORS_KEY, true);

    cluster = builder.build();
    client = cluster.clientFixture();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (client != null) {
      client.close();
    }

    if (cluster != null) {
      cluster.close();
    }
  }

  //@Test
  public void testPushProjectIntoScan() throws Exception {
    String parentDir = "D:\\drill\\files\\fact_dim_tables\\fact";

    cluster.defineWorkspace("dfs", "root", parentDir, "parquet");
    // String sql = "select col_vrchr, count(*) from `dfs.root`.`1992` group by col_vrchr";
    String sql = "select col_vrchr, count(*) from (select * from `dfs.root`.`1992`) group by col_vrchr";

    QueryBuilder queryBuilder = client.queryBuilder().sql(sql);
    // print data
    //queryBuilder.printCsv();

    // print plan
    System.out.println(queryBuilder.explainText());

  }

  /*
  Original:
  00-00    Screen
00-01      Project(col_vrchr=[$0], EXPR$1=[$1])
00-02        StreamAgg(group=[{0}], EXPR$1=[COUNT()])
00-03          Sort(sort0=[$0], dir0=[ASC])
00-04            Project(col_vrchr=[ITEM($0, 'col_vrchr')])
00-05              Scan(groupscan=[ParquetGroupScan [entries=[ReadEntryWithPath [path=file:/D:/drill/files/fact_dim_tables/fact/1992]], selectionRoot=file:/D:/drill/files/fact_dim_tables/fact/1992, numFiles=1, numRowGroups=1, usedMetadataFile=false, columns=[`*`]]])


00-00    Screen
00-01      Project(col_vrchr=[$0], EXPR$1=[$1])
00-02        StreamAgg(group=[{0}], EXPR$1=[COUNT()])
00-03          Sort(sort0=[$0], dir0=[ASC])
00-04            Scan(groupscan=[ParquetGroupScan [entries=[ReadEntryWithPath [path=file:/D:/drill/files/fact_dim_tables/fact/1992]], selectionRoot=file:/D:/drill/files/fact_dim_tables/fact/1992, numFiles=1, numRowGroups=1, usedMetadataFile=false, columns=[`col_vrchr`]]])




   */


  //@Test
  public void testDirectoryPruning() throws Exception {
    String parentDir = "D:\\drill\\files\\fact_dim_tables";

    cluster.defineWorkspace("dfs", "root", parentDir, "parquet");
    //String sql = "select * from `dfs.root`.`fact` t";

    //String sql = "select * from `dfs.root`.`dim` d where d.dir0 = '1991'";
    String sql = "select * from (select * from `dfs.root`.`dim`) d where d.dir0 = '1991'";

    QueryBuilder queryBuilder = client.queryBuilder().sql(sql);
    // print data
    //queryBuilder.printCsv();

    // print plan
    System.out.println(queryBuilder.explainText());
  }

    /*
  Original:
  00-00    Screen
00-01      Project(**=[$0])
00-02        SelectionVectorRemover
00-03          Filter(condition=[=(ITEM($0, 'dir0'), '1991')])
00-04            Scan(groupscan=[ParquetGroupScan [entries=[ReadEntryWithPath [path=file:/D:/drill/files/fact_dim_tables/dim/1991/alltypes_optional.parquet], ReadEntryWithPath [path=file:/D:/drill/files/fact_dim_tables/dim/1992/alltypes_optional.parquet]], selectionRoot=file:/D:/drill/files/fact_dim_tables/dim, numFiles=2, numRowGroups=2, usedMetadataFile=false, columns=[`*`]]])

   */

  /*
  00-00    Screen
00-01      Project(**=[$0])
00-02        Project(**=[$0])
00-03          Scan(groupscan=[ParquetGroupScan [entries=[ReadEntryWithPath [path=file:/D:/drill/files/fact_dim_tables/dim/1991/alltypes_optional.parquet]], selectionRoot=file:/D:/drill/files/fact_dim_tables/dim, numFiles=1, numRowGroups=1, usedMetadataFile=false, columns=[`*`]]])

   */

  @Test
  public void testFilterPushDown() throws Exception {
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
      String query = "select * from (select * from order_ctas) where o_orderdate = date '1992-01-01' or o_orderdate = date '1992-01-09'"; // read 3 files
      //String query = "select * from order_ctas where o_orderdate = date '1992-01-01' or o_orderdate = date '1992-01-09'"; // read 3 files
      //String query = "select * from (select * from order_ctas) where o_orderdate = date '1992-01-01' and ccc = 'aa'"; // read 3 files
      //String query = "select * from (select * from order_ctas) where upper(o_orderdate) = date '1992-01-01'"; // read 3 files

      // print query plan
      queryBuilder.sql(query);
      queryBuilder.printCsv();
      System.out.println(queryBuilder.explainText());

    } finally {
      queryBuilder.sql("drop table if exists %s", tableName).run();
    }
  }

  /*
  Original:
00-00    Screen
00-01      Project(**=[$0])
00-02        Project(T0¦¦**=[$0])
00-03          SelectionVectorRemover
00-04            Filter(condition=[=($1, 1992-01-01)])
00-05              Project(T0¦¦**=[$0], o_orderdate=[$1])
00-06                Scan(groupscan=[ParquetGroupScan [entries=[ReadEntryWithPath [path=D:/git_repo/drill/exec/java-exec/target/org.apache.drill.exec.store.parquet.TestPushdownAndPruningWithStarItem/dfsTestTmp/1516802494290-0/order_ctas/t1/0_0_0.parquet]], selectionRoot=file:/D:/git_repo/drill/exec/java-exec/target/org.apache.drill.exec.store.parquet.TestPushdownAndPruningWithStarItem/dfsTestTmp/1516802494290-0/order_ctas, numFiles=1, numRowGroups=1, usedMetadataFile=false, columns=[`*`]]])

00-00    Screen
00-01      Project(**=[$0])
00-02        SelectionVectorRemover
00-03          Filter(condition=[=(ITEM($0, 'o_orderdate'), 1992-01-01)])
00-04            Scan(groupscan=[ParquetGroupScan [entries=[ReadEntryWithPath [path=file:/D:/git_repo/drill/exec/java-exec/target/org.apache.drill.exec.store.parquet.TestPushdownAndPruningWithStarItem/dfsTestTmp/1516802530132-0/order_ctas/t1/0_0_0.parquet], ReadEntryWithPath [path=file:/D:/git_repo/drill/exec/java-exec/target/org.apache.drill.exec.store.parquet.TestPushdownAndPruningWithStarItem/dfsTestTmp/1516802530132-0/order_ctas/t2/0_0_0.parquet], ReadEntryWithPath [path=file:/D:/git_repo/drill/exec/java-exec/target/org.apache.drill.exec.store.parquet.TestPushdownAndPruningWithStarItem/dfsTestTmp/1516802530132-0/order_ctas/t3/0_0_0.parquet]], selectionRoot=file:/D:/git_repo/drill/exec/java-exec/target/org.apache.drill.exec.store.parquet.TestPushdownAndPruningWithStarItem/dfsTestTmp/1516802530132-0/order_ctas, numFiles=3, numRowGroups=3, usedMetadataFile=false, columns=[`*`]]])



   */

  @Test
  public void testFailure() throws Exception {
    String query = " select n.n_regionkey, count(*) as cnt \n" +
        " from   (select * from cp.`tpch/nation.parquet`) n  \n" +
        "   join (select * from cp.`tpch/region.parquet`) r  \n" +
        " on n.n_regionkey = r.r_regionkey \n" +
        " where n.n_nationkey > 10 \n" +
        " group by n.n_regionkey \n" +
        " order by n.n_regionkey";

    String query1 = "SELECT SUM(n_nationkey) OVER w as s\n" +
        "FROM (SELECT * FROM cp.`tpch/nation.parquet`) subQry\n" +
        "WINDOW w AS (PARTITION BY REGION ORDER BY n_nationkey)\n" +
        "limit 1";

    QueryBuilder queryBuilder = client.queryBuilder();

    queryBuilder.sql(query1);
    queryBuilder.printCsv();
    System.out.println(queryBuilder.explainText());

  }

}
