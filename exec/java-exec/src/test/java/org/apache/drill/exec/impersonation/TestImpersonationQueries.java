/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.impersonation;

import com.google.common.collect.Maps;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.store.avro.AvroTestUtil;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertNull;

/**
 * Test queries involving direct impersonation and multilevel impersonation including join queries where each side is
 * a nested view.
 */
public class TestImpersonationQueries extends BaseTestImpersonation {
  @BeforeClass
  public static void setup() throws Exception {
    startMiniDfsCluster(TestImpersonationQueries.class.getSimpleName());
    startDrillCluster(true);
    addMiniDfsBasedStorage(createTestWorkspaces());
    createTestData();
  }

  private static void createTestData() throws Exception {
    // Create test tables/views

    // Create copy of "lineitem" table in /user/user0_1 owned by user0_1:group0_1 with permissions 750. Only user0_1
    // has access to data in created "lineitem" table.
    createTestTable(org1Users[0], org1Groups[0], "lineitem");

    // Create copy of "orders" table in /user/user0_2 owned by user0_2:group0_2 with permissions 750. Only user0_2
    // has access to data in created "orders" table.
    createTestTable(org2Users[0], org2Groups[0], "orders");

    createNestedTestViewsOnLineItem();
    createNestedTestViewsOnOrders();
    createRecordReadersData(org1Users[0], org1Groups[0]);
  }

  private static Map<String, WorkspaceConfig> createTestWorkspaces() throws Exception {
    // Create "/tmp" folder and set permissions to "777"
    final Path tmpPath = new Path("/tmp");
    fs.delete(tmpPath, true);
    FileSystem.mkdirs(fs, tmpPath, new FsPermission((short)0777));

    Map<String, WorkspaceConfig> workspaces = Maps.newHashMap();

    // create user directory (ex. "/user/user0_1", with ownership "user0_1:group0_1" and perms 755) for every user.
    for (int i = 0; i < org1Users.length; i++) {
      final String user = org1Users[i];
      final String group = org1Groups[i];
      createAndAddWorkspace(user, getUserHome(user), (short)0755, user, group, workspaces);
    }

    // create user directory (ex. "/user/user0_2", with ownership "user0_2:group0_2" and perms 755) for every user.
    for (int i = 0; i < org2Users.length; i++) {
      final String user = org2Users[i];
      final String group = org2Groups[i];
      createAndAddWorkspace(user, getUserHome(user), (short)0755, user, group, workspaces);
    }

    return workspaces;
  }

  private static void createTestTable(String user, String group, String tableName) throws Exception {
    updateClient(user);
    test("USE " + getWSSchema(user));
    test(String.format("CREATE TABLE %s as SELECT * FROM cp.`tpch/%s.parquet`;", tableName, tableName));

    // Change the ownership and permissions manually. Currently there is no option to specify the default permissions
    // and ownership for new tables.
    final Path tablePath = new Path(getUserHome(user), tableName);

    fs.setOwner(tablePath, user, group);
    fs.setPermission(tablePath, new FsPermission((short)0750));
  }

  private static void createNestedTestViewsOnLineItem() throws Exception {
    // Input table "lineitem"
    // /user/user0_1     lineitem      750    user0_1:group0_1

    // Create a view on top of lineitem table
    // /user/user1_1    u1_lineitem    750    user1_1:group1_1
    createView(org1Users[1], org1Groups[1], (short)0750, "u1_lineitem", getWSSchema(org1Users[0]), "lineitem");

    // Create a view on top of u1_lineitem view
    // /user/user2_1    u2_lineitem    750    user2_1:group2_1
    createView(org1Users[2], org1Groups[2], (short) 0750, "u2_lineitem", getWSSchema(org1Users[1]), "u1_lineitem");

    // Create a view on top of u2_lineitem view
    // /user/user2_1    u22_lineitem    750    user2_1:group2_1
    createView(org1Users[2], org1Groups[2], (short) 0750, "u22_lineitem", getWSSchema(org1Users[2]), "u2_lineitem");

    // Create a view on top of u22_lineitem view
    // /user/user3_1    u3_lineitem    750    user3_1:group3_1
    createView(org1Users[3], org1Groups[3], (short)0750, "u3_lineitem", getWSSchema(org1Users[2]), "u22_lineitem");

    // Create a view on top of u3_lineitem view
    // /user/user4_1    u4_lineitem    755    user4_1:group4_1
    createView(org1Users[4], org1Groups[4], (short) 0755, "u4_lineitem", getWSSchema(org1Users[3]), "u3_lineitem");
  }

  private static void createNestedTestViewsOnOrders() throws Exception {
    // Input table "orders"
    // /user/user0_2     orders      750    user0_2:group0_2

    // Create a view on top of orders table
    // /user/user1_2    u1_orders    750    user1_2:group1_2
    createView(org2Users[1], org2Groups[1], (short)0750, "u1_orders", getWSSchema(org2Users[0]), "orders");

    // Create a view on top of u1_orders view
    // /user/user2_2    u2_orders    750    user2_2:group2_2
    createView(org2Users[2], org2Groups[2], (short)0750, "u2_orders", getWSSchema(org2Users[1]), "u1_orders");

    // Create a view on top of u2_orders view
    // /user/user2_2    u22_orders    750    user2_2:group2_2
    createView(org2Users[2], org2Groups[2], (short)0750, "u22_orders", getWSSchema(org2Users[2]), "u2_orders");

    // Create a view on top of u22_orders view (permissions of this view (755) are different from permissions of the
    // corresponding view in "lineitem" nested views to have a join query of "lineitem" and "orders" nested views
    // without exceeding the maximum number of allowed user hops in chained impersonation.
    // /user/user3_2    u3_orders    750    user3_2:group3_2
    createView(org2Users[3], org2Groups[3], (short)0755, "u3_orders", getWSSchema(org2Users[2]), "u22_orders");

    // Create a view on top of u3_orders view
    // /user/user4_2    u4_orders    755    user4_2:group4_2
    createView(org2Users[4], org2Groups[4], (short)0755, "u4_orders", getWSSchema(org2Users[3]), "u3_orders");
  }

  private static void createRecordReadersData(String user, String group) throws Exception {
    // copy sequence file
    updateClient(user);
    Path localFile = new Path(FileUtils.getResourceAsFile("/sequencefiles/simple.seq").toURI().toString());
    Path dfsFile = new Path(getUserHome(user), "simple.seq");
    fs.copyFromLocalFile(localFile, dfsFile);
    fs.setOwner(dfsFile, user, group);
    fs.setPermission(dfsFile, new FsPermission((short) 0700));

    localFile = new Path(AvroTestUtil.generateSimplePrimitiveSchema_NoNullValues().getFilePath());
    dfsFile = new Path(getUserHome(user), "simple.avro");
    fs.copyFromLocalFile(localFile, dfsFile);
    fs.setOwner(dfsFile, user, group);
    fs.setPermission(dfsFile, new FsPermission((short) 0700));
  }

  @Test
  public void testBug() throws Exception {
    Path dfsPath = new Path("/tmp/arina/");
    Path localPath = new Path("/home/osboxes/files/dataset/");
    fs.copyFromLocalFile(localPath, dfsPath);
    fs.copyFromLocalFile(new Path("/home/osboxes/files/decimal_test.tbl"), dfsPath);
    fs.copyFromLocalFile(new Path("/home/osboxes/files/decimal_test.dat"), dfsPath);

    fs.setOwner(dfsPath, processUser, processUser);
    fs.setPermission(dfsPath, new FsPermission((short) 0777));
    updateClient(processUser);

/*    final RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(dfsPath, true);
    while (locatedFileStatusRemoteIterator.hasNext()) {
      System.out.println(locatedFileStatusRemoteIterator.next().getPath());
    }*/
    
    // hdfs://localhost:35209/tmp/arina/store_sales/store_sales.dat
    
    // create views
    test("create or replace view %s.arina.store_sales as select " +
        "case when (columns[0]='') then cast(null as integer) else cast(columns[0] as integer) end as ss_sold_date_sk, " +
        "case when (columns[1]='') then cast(null as integer) else cast(columns[1] as integer) end as ss_sold_time_sk, " +
        "case when (columns[2]='') then cast(null as integer) else cast(columns[2] as integer) end as ss_item_sk, " +
        "case when (columns[3]='') then cast(null as integer) else cast(columns[3] as integer) end as ss_customer_sk, " +
        "case when (columns[4]='') then cast(null as integer) else cast(columns[4] as integer) end as ss_cdemo_sk, " +
        "case when (columns[5]='') then cast(null as integer) else cast(columns[5] as integer) end as ss_hdemo_sk, " +
        "case when (columns[6]='') then cast(null as integer) else cast(columns[6] as integer) end as ss_addr_sk, " +
        "case when (columns[7]='') then cast(null as integer) else cast(columns[7] as integer) end as ss_store_sk, " +
        "case when (columns[8]='') then cast(null as integer) else cast(columns[8] as integer) end as ss_promo_sk, " +
        "case when (columns[9]='') then cast(null as integer) else cast(columns[9] as integer) end as ss_ticket_number, " +
        "case when (columns[10]='') then cast(null as integer) else cast(columns[10] as integer) end as ss_quantity, " +
        "case when (columns[11]='') then cast(null as double) else cast(columns[11] as double) end as ss_wholesale_cost, " +
        "case when (columns[12]='') then cast(null as double) else cast(columns[12] as double) end as ss_list_price, " +
        "case when (columns[13]='') then cast(null as double) else cast(columns[13] as double) end as ss_sales_price, " +
        "case when (columns[14]='') then cast(null as double) else cast(columns[14] as double) end as ss_ext_discount_amt, " +
        "case when (columns[15]='') then cast(null as double) else cast(columns[15] as double) end as ss_ext_sales_price, " +
        "case when (columns[16]='') then cast(null as double) else cast(columns[16] as double) end as ss_ext_wholesale_cost, " +
        "case when (columns[17]='') then cast(null as double) else cast(columns[17] as double) end as ss_ext_list_price, " +
        "case when (columns[18]='') then cast(null as double) else cast(columns[18] as double) end as ss_ext_tax, " +
        "case when (columns[19]='') then cast(null as double) else cast(columns[19] as double) end as ss_coupon_amt, " +
        "case when (columns[20]='') then cast(null as double) else cast(columns[20] as double) end as ss_net_paid, " +
        "case when (columns[21]='') then cast(null as double) else cast(columns[21] as double) end as ss_net_paid_inc_tax, " +
        "case when (columns[22]='') then cast(null as double) else cast(columns[22] as double) end as ss_net_profit " +
        "from %s.arina.`/arina/store_sales/store_sales.dat`", MINIDFS_STORAGE_PLUGIN_NAME, MINIDFS_STORAGE_PLUGIN_NAME);

    //test("select * from %s.arina.store_sales", MINIDFS_STORAGE_PLUGIN_NAME);
    
    test("create or replace view %s.arina.date_dim as select " +
        "case when (columns[0]='') then cast(null as integer) else cast(columns[0] as integer) end as d_date_sk, " +
        "case when (columns[1]='') then cast(null as varchar(200)) else cast(columns[1] as varchar(200)) end as d_date_id, " +
        "case when (columns[2]='') then cast(null as date) else cast(columns[2] as date) end as d_date, " +
        "case when (columns[3]='') then cast(null as integer) else cast(columns[3] as integer) end as d_month_seq, " +
        "case when (columns[4]='') then cast(null as integer) else cast(columns[4] as integer) end as d_week_seq, " +
        "case when (columns[5]='') then cast(null as integer) else cast(columns[5] as integer) end as d_quarter_seq, " +
        "case when (columns[6]='') then cast(null as integer) else cast(columns[6] as integer) end as d_year, " +
        "case when (columns[7]='') then cast(null as integer) else cast(columns[7] as integer) end as d_dow, " +
        "case when (columns[8]='') then cast(null as integer) else cast(columns[8] as integer) end as d_moy, " +
        "case when (columns[9]='') then cast(null as integer) else cast(columns[9] as integer) end as d_dom, " +
        "case when (columns[10]='') then cast(null as integer) else cast(columns[10] as integer) end as d_qoy, " +
        "case when (columns[11]='') then cast(null as integer) else cast(columns[11] as integer) end as d_fy_year, " +
        "case when (columns[12]='') then cast(null as integer) else cast(columns[12] as integer) end as d_fy_quarter_seq, " +
        "case when (columns[13]='') then cast(null as integer) else cast(columns[13] as integer) end as s_fy_week_seq, " +
        "case when (columns[14]='') then cast(null as varchar(200)) else cast(columns[14] as varchar(200)) end as d_day_name, " +
        "case when (columns[15]='') then cast(null as varchar(200)) else cast(columns[15] as varchar(200)) end as d_quarter_name, " +
        "case when (columns[16]='') then cast(null as varchar(200)) else cast(columns[16] as varchar(200)) end as d_holiday, " +
        "case when (columns[17]='') then cast(null as varchar(200)) else cast(columns[17] as varchar(200)) end as d_weekend, " +
        "case when (columns[18]='') then cast(null as varchar(200)) else cast(columns[18] as varchar(200)) end as d_following_holiday, " +
        "case when (columns[19]='') then cast(null as integer) else cast(columns[19] as integer) end as d_first_dom, " +
        "case when (columns[20]='') then cast(null as integer) else cast(columns[20] as integer) end as d_last_dom, " +
        "case when (columns[21]='') then cast(null as integer) else cast(columns[21] as integer) end as d_same_day_ly, " +
        "case when (columns[22]='') then cast(null as integer) else cast(columns[22] as integer) end as d_same_day_lq, " +
        "case when (columns[23]='') then cast(null as varchar(200)) else cast(columns[23] as varchar(200)) end as d_current_day, " +
        "case when (columns[24]='') then cast(null as varchar(200)) else cast(columns[24] as varchar(200)) end as d_current_week, " +
        "case when (columns[25]='') then cast(null as varchar(200)) else cast(columns[25] as varchar(200)) end as d_current_month, " +
        "case when (columns[26]='') then cast(null as varchar(200)) else cast(columns[26] as varchar(200)) end as d_current_quarter, " +
        "case when (columns[27]='') then cast(null as varchar(200)) else cast(columns[27] as varchar(200)) end as d_current_year " +
        "from %s.arina.`/arina/date_dim/date_dim.dat`", MINIDFS_STORAGE_PLUGIN_NAME, MINIDFS_STORAGE_PLUGIN_NAME);

    test("create or replace view %s.arina.store as select " +
        "case when (columns[0]='') then cast(null as integer) else cast(columns[0] as integer) end as s_store_sk, " +
        "case when (columns[1]='') then cast(null as varchar(200)) else cast(columns[1] as varchar(200)) end as s_store_id, " +
        "case when (columns[2]='') then cast(null as date) else cast(columns[2] as date) end as s_rec_start_date, " +
        "case when (columns[3]='') then cast(null as date) else cast(columns[3] as date) end as s_rec_end_date, " +
        "case when (columns[4]='') then cast(null as integer) else cast(columns[4] as integer) end as s_closed_date_sk, " +
        "case when (columns[5]='') then cast(null as varchar(200)) else cast(columns[5] as varchar(200)) end as s_store_name, " +
        "case when (columns[6]='') then cast(null as integer) else cast(columns[6] as integer) end as s_number_employees, " +
        "case when (columns[7]='') then cast(null as integer) else cast(columns[7] as integer) end as s_floor_space, " +
        "case when (columns[8]='') then cast(null as varchar(200)) else cast(columns[8] as varchar(200)) end as s_hours," +
        "case when (columns[9]='') then cast(null as varchar(200)) else cast(columns[9] as varchar(200)) end as s_manager, " +
        "case when (columns[10]='') then cast(null as integer) else cast(columns[10] as integer) end as s_market_id, " +
        "case when (columns[11]='') then cast(null as varchar(200)) else cast(columns[11] as varchar(200)) end as s_geography_class, " +
        "case when (columns[12]='') then cast(null as varchar(200)) else cast(columns[12] as varchar(200)) end as s_market_desc, " +
        "case when (columns[13]='') then cast(null as varchar(200)) else cast(columns[13] as varchar(200)) end as s_market_manager, " +
        "case when (columns[14]='') then cast(null as integer) else cast(columns[14] as integer) end as s_division_id, " +
        "case when (columns[15]='') then cast(null as varchar(200)) else cast(columns[15] as varchar(200)) end as s_division_name, " +
        "case when (columns[16]='') then cast(null as integer) else cast(columns[16] as integer) end as s_company_id, " +
        "case when (columns[17]='') then cast(null as varchar(200)) else cast(columns[17] as varchar(200)) end as s_company_name, " +
        "case when (columns[18]='') then cast(null as varchar(200)) else cast(columns[18] as varchar(200)) end as s_street_number, " +
        "case when (columns[19]='') then cast(null as varchar(200)) else cast(columns[19] as varchar(200)) end as s_street_name, " +
        "case when (columns[20]='') then cast(null as varchar(200)) else cast(columns[20] as varchar(200)) end as s_street_type, " +
        "case when (columns[21]='') then cast(null as varchar(200)) else cast(columns[21] as varchar(200)) end as s_suite_number, " +
        "case when (columns[22]='') then cast(null as varchar(200)) else cast(columns[22] as varchar(200)) end as s_city, " +
        "case when (columns[23]='') then cast(null as varchar(200)) else cast(columns[23] as varchar(200)) end as s_county, " +
        "case when (columns[24]='') then cast(null as varchar(200)) else cast(columns[24] as varchar(200)) end as s_state, " +
        "case when (columns[25]='') then cast(null as varchar(200)) else cast(columns[25] as varchar(200)) end as s_zip, " +
        "case when (columns[26]='') then cast(null as varchar(200)) else cast(columns[26] as varchar(200)) end as s_country, " +
        "case when (columns[27]='') then cast(null as double) else cast(columns[27] as double) end as s_gmt_offset, " +
        "case when (columns[28]='') then cast(null as double) else cast(columns[28] as double) end as s_tax_precentage " +
        "from %s.arina.`/arina/store/store.dat`", MINIDFS_STORAGE_PLUGIN_NAME, MINIDFS_STORAGE_PLUGIN_NAME);

 /*   testBuilder()
        .sqlQuery("select sum(cast(columns[1] as double)) as price from miniDfsPlugin.arina.`/arina/decimal_test.tbl`")
        .unOrdered()
        .baselineColumns("price")
        .baselineValues(176.73000000000002)
        .go();

    testBuilder()
        .sqlQuery("select cast(columns[1] as double) as price from miniDfsPlugin.arina.`/arina/decimal_test.tbl` where columns[0] = 'arina'")
        .unOrdered()
        .baselineColumns("price")
        .baselineValues(120.56)
        .go();

    test("create or replace view miniDfsPlugin.arina.decimal_test as select " +
        "cast(columns[0] as varchar(200)) as name, cast(columns[1] as double) as price from miniDfsPlugin" +
        ".arina.`/arina/decimal_test.dat`");

    testBuilder()
        .sqlQuery("select sum(price) as price from miniDfsPlugin.arina.`decimal_test`")
        .unOrdered()
        .baselineColumns("price")
        .baselineValues(176.73000000000002)
        .go();*/

/*    testBuilder()
        .sqlQuery("select sum(ss_sales_price) as price from miniDfsPlugin.arina.store_sales")
        .unOrdered()
        .baselineColumns("price")
        .baselineValues(1.0423193558999118E8)
        //  returned 1.0423193558999895E8
        .go();*/
    
    testBuilder()
        .sqlQuery(String.format("select" +
            "  s.s_store_name," +
            "  s.s_store_id," +
            "  sum(case when (d.d_day_name = 'Sunday') then ss.ss_sales_price else null end) sun_sales," +
            "  sum(case when (d.d_day_name = 'Monday') then ss.ss_sales_price else null end) mon_sales," +
            "  sum(case when (d.d_day_name = 'Tuesday') then ss.ss_sales_price else null end) tue_sales," +
            "  sum(case when (d.d_day_name = 'Wednesday') then ss.ss_sales_price else null end) wed_sales," +
            "  sum(case when (d.d_day_name = 'Thursday') then ss.ss_sales_price else null end) thu_sales," +
            "  sum(case when (d.d_day_name = 'Friday') then ss.ss_sales_price else null end) fri_sales," +
            "  sum(case when (d.d_day_name = 'Saturday') then ss.ss_sales_price else null end) sat_sales" +
            " from" +
            "  %s.arina.date_dim as d," +
            "  %s.arina.store_sales as ss," +
            "  %s.arina.store as s" +
            " where" +
            "  d.d_date_sk = ss.ss_sold_date_sk" +
            "  and s.s_store_sk = ss.ss_store_sk" +
            "  and s.s_gmt_offset = -5" +
            "  and d.d_year = 1998" +
            "  and ss.ss_sold_date_sk between 2450816 and 2451179" +
            " group by" +
            "  s.s_store_name," +
            "  s.s_store_id" +
            " order by" +
            "  s.s_store_name," +
            "  s.s_store_id," +
            "  sun_sales," +
            "  mon_sales," +
            "  tue_sales," +
            "  wed_sales," +
            "  thu_sales," +
            "  fri_sales," +
            "  sat_sales" +
        " limit 100", MINIDFS_STORAGE_PLUGIN_NAME, MINIDFS_STORAGE_PLUGIN_NAME, MINIDFS_STORAGE_PLUGIN_NAME))
    .ordered()
    .baselineColumns("s_store_name", "s_store_id", "sun_sales", "mon_sales", "tue_sales", "wed_sales", "thu_sales", "fri_sales", "sat_sales")
    .baselineValues("able","AAAAAAAACAAAAAAA",459564.3499999986,492832.2100000004,472234.89000000147,513998.9199999991,480718.60999999876,480887.2999999979,482390.9299999994)
    .baselineValues("ation","AAAAAAAAHAAAAAAA",468604.3999999996,467302.1000000005,470821.48999999993,510779.71000000066,481377.21000000107,473673.48999999923,471487.62000000075)
    .baselineValues("bar","AAAAAAAAKAAAAAAA",495341.9799999992,461667.2800000013,476791.3800000002,498368.240000001,475762.4300000013,476690.979999999,462227.2200000022)
    .baselineValues("eing","AAAAAAAAIAAAAAAA",487998.3200000025,460740.3699999989,486454.05999999976,447624.8700000004,468239.159999999,468815.85999999876,502495.15999999957)
    .baselineValues("ese","AAAAAAAAEAAAAAAA",466373.15000000113,477718.0200000016,486363.5899999991,490049.12000000017,483203.2500000001,470427.7700000023,497738.0200000024)
    .baselineValues("ought", "AAAAAAAABAAAAAAA", 442199.1699999991, 483945.97, 497270.79000000027, 483466.9200000013, 496373.0899999972, 491362.2499999975, 462524.8699999985)
    .go();
  }

  @Test
  public void testBugLocally() throws Exception {
    test("create or replace view %s.tmp.store_sales as select " +
        "case when (columns[0]='') then cast(null as integer) else cast(columns[0] as integer) end as ss_sold_date_sk, " +
        "case when (columns[1]='') then cast(null as integer) else cast(columns[1] as integer) end as ss_sold_time_sk, " +
        "case when (columns[2]='') then cast(null as integer) else cast(columns[2] as integer) end as ss_item_sk, " +
        "case when (columns[3]='') then cast(null as integer) else cast(columns[3] as integer) end as ss_customer_sk, " +
        "case when (columns[4]='') then cast(null as integer) else cast(columns[4] as integer) end as ss_cdemo_sk, " +
        "case when (columns[5]='') then cast(null as integer) else cast(columns[5] as integer) end as ss_hdemo_sk, " +
        "case when (columns[6]='') then cast(null as integer) else cast(columns[6] as integer) end as ss_addr_sk, " +
        "case when (columns[7]='') then cast(null as integer) else cast(columns[7] as integer) end as ss_store_sk, " +
        "case when (columns[8]='') then cast(null as integer) else cast(columns[8] as integer) end as ss_promo_sk, " +
        "case when (columns[9]='') then cast(null as integer) else cast(columns[9] as integer) end as ss_ticket_number, " +
        "case when (columns[10]='') then cast(null as integer) else cast(columns[10] as integer) end as ss_quantity, " +
        "case when (columns[11]='') then cast(null as double) else cast(columns[11] as double) end as ss_wholesale_cost, " +
        "case when (columns[12]='') then cast(null as double) else cast(columns[12] as double) end as ss_list_price, " +
        "case when (columns[13]='') then cast(null as double) else cast(columns[13] as double) end as ss_sales_price, " +
        "case when (columns[14]='') then cast(null as double) else cast(columns[14] as double) end as ss_ext_discount_amt, " +
        "case when (columns[15]='') then cast(null as double) else cast(columns[15] as double) end as ss_ext_sales_price, " +
        "case when (columns[16]='') then cast(null as double) else cast(columns[16] as double) end as ss_ext_wholesale_cost, " +
        "case when (columns[17]='') then cast(null as double) else cast(columns[17] as double) end as ss_ext_list_price, " +
        "case when (columns[18]='') then cast(null as double) else cast(columns[18] as double) end as ss_ext_tax, " +
        "case when (columns[19]='') then cast(null as double) else cast(columns[19] as double) end as ss_coupon_amt, " +
        "case when (columns[20]='') then cast(null as double) else cast(columns[20] as double) end as ss_net_paid, " +
        "case when (columns[21]='') then cast(null as double) else cast(columns[21] as double) end as ss_net_paid_inc_tax, " +
        "case when (columns[22]='') then cast(null as double) else cast(columns[22] as double) end as ss_net_profit " +
        "from dfs_test.`/home/osboxes/files/dataset/store_sales/store_sales.tbl`", "dfs_test");

    // test("select * from dfs_test.tmp.store_sales");

    test("create or replace view %s.tmp.date_dim as select " +
        "case when (columns[0]='') then cast(null as integer) else cast(columns[0] as integer) end as d_date_sk, " +
        "case when (columns[1]='') then cast(null as varchar(200)) else cast(columns[1] as varchar(200)) end as d_date_id, " +
        "case when (columns[2]='') then cast(null as date) else cast(columns[2] as date) end as d_date, " +
        "case when (columns[3]='') then cast(null as integer) else cast(columns[3] as integer) end as d_month_seq, " +
        "case when (columns[4]='') then cast(null as integer) else cast(columns[4] as integer) end as d_week_seq, " +
        "case when (columns[5]='') then cast(null as integer) else cast(columns[5] as integer) end as d_quarter_seq, " +
        "case when (columns[6]='') then cast(null as integer) else cast(columns[6] as integer) end as d_year, " +
        "case when (columns[7]='') then cast(null as integer) else cast(columns[7] as integer) end as d_dow, " +
        "case when (columns[8]='') then cast(null as integer) else cast(columns[8] as integer) end as d_moy, " +
        "case when (columns[9]='') then cast(null as integer) else cast(columns[9] as integer) end as d_dom, " +
        "case when (columns[10]='') then cast(null as integer) else cast(columns[10] as integer) end as d_qoy, " +
        "case when (columns[11]='') then cast(null as integer) else cast(columns[11] as integer) end as d_fy_year, " +
        "case when (columns[12]='') then cast(null as integer) else cast(columns[12] as integer) end as d_fy_quarter_seq, " +
        "case when (columns[13]='') then cast(null as integer) else cast(columns[13] as integer) end as s_fy_week_seq, " +
        "case when (columns[14]='') then cast(null as varchar(200)) else cast(columns[14] as varchar(200)) end as d_day_name, " +
        "case when (columns[15]='') then cast(null as varchar(200)) else cast(columns[15] as varchar(200)) end as d_quarter_name, " +
        "case when (columns[16]='') then cast(null as varchar(200)) else cast(columns[16] as varchar(200)) end as d_holiday, " +
        "case when (columns[17]='') then cast(null as varchar(200)) else cast(columns[17] as varchar(200)) end as d_weekend, " +
        "case when (columns[18]='') then cast(null as varchar(200)) else cast(columns[18] as varchar(200)) end as d_following_holiday, " +
        "case when (columns[19]='') then cast(null as integer) else cast(columns[19] as integer) end as d_first_dom, " +
        "case when (columns[20]='') then cast(null as integer) else cast(columns[20] as integer) end as d_last_dom, " +
        "case when (columns[21]='') then cast(null as integer) else cast(columns[21] as integer) end as d_same_day_ly, " +
        "case when (columns[22]='') then cast(null as integer) else cast(columns[22] as integer) end as d_same_day_lq, " +
        "case when (columns[23]='') then cast(null as varchar(200)) else cast(columns[23] as varchar(200)) end as d_current_day, " +
        "case when (columns[24]='') then cast(null as varchar(200)) else cast(columns[24] as varchar(200)) end as d_current_week, " +
        "case when (columns[25]='') then cast(null as varchar(200)) else cast(columns[25] as varchar(200)) end as d_current_month, " +
        "case when (columns[26]='') then cast(null as varchar(200)) else cast(columns[26] as varchar(200)) end as d_current_quarter, " +
        "case when (columns[27]='') then cast(null as varchar(200)) else cast(columns[27] as varchar(200)) end as d_current_year " +
        "from dfs_test.`/home/osboxes/files/dataset/date_dim/date_dim.tbl`", "dfs_test");

    test("create or replace view %s.tmp.store as select " +
        "case when (columns[0]='') then cast(null as integer) else cast(columns[0] as integer) end as s_store_sk, " +
        "case when (columns[1]='') then cast(null as varchar(200)) else cast(columns[1] as varchar(200)) end as s_store_id, " +
        "case when (columns[2]='') then cast(null as date) else cast(columns[2] as date) end as s_rec_start_date, " +
        "case when (columns[3]='') then cast(null as date) else cast(columns[3] as date) end as s_rec_end_date, " +
        "case when (columns[4]='') then cast(null as integer) else cast(columns[4] as integer) end as s_closed_date_sk, " +
        "case when (columns[5]='') then cast(null as varchar(200)) else cast(columns[5] as varchar(200)) end as s_store_name, " +
        "case when (columns[6]='') then cast(null as integer) else cast(columns[6] as integer) end as s_number_employees, " +
        "case when (columns[7]='') then cast(null as integer) else cast(columns[7] as integer) end as s_floor_space, " +
        "case when (columns[8]='') then cast(null as varchar(200)) else cast(columns[8] as varchar(200)) end as s_hours," +
        "case when (columns[9]='') then cast(null as varchar(200)) else cast(columns[9] as varchar(200)) end as s_manager, " +
        "case when (columns[10]='') then cast(null as integer) else cast(columns[10] as integer) end as s_market_id, " +
        "case when (columns[11]='') then cast(null as varchar(200)) else cast(columns[11] as varchar(200)) end as s_geography_class, " +
        "case when (columns[12]='') then cast(null as varchar(200)) else cast(columns[12] as varchar(200)) end as s_market_desc, " +
        "case when (columns[13]='') then cast(null as varchar(200)) else cast(columns[13] as varchar(200)) end as s_market_manager, " +
        "case when (columns[14]='') then cast(null as integer) else cast(columns[14] as integer) end as s_division_id, " +
        "case when (columns[15]='') then cast(null as varchar(200)) else cast(columns[15] as varchar(200)) end as s_division_name, " +
        "case when (columns[16]='') then cast(null as integer) else cast(columns[16] as integer) end as s_company_id, " +
        "case when (columns[17]='') then cast(null as varchar(200)) else cast(columns[17] as varchar(200)) end as s_company_name, " +
        "case when (columns[18]='') then cast(null as varchar(200)) else cast(columns[18] as varchar(200)) end as s_street_number, " +
        "case when (columns[19]='') then cast(null as varchar(200)) else cast(columns[19] as varchar(200)) end as s_street_name, " +
        "case when (columns[20]='') then cast(null as varchar(200)) else cast(columns[20] as varchar(200)) end as s_street_type, " +
        "case when (columns[21]='') then cast(null as varchar(200)) else cast(columns[21] as varchar(200)) end as s_suite_number, " +
        "case when (columns[22]='') then cast(null as varchar(200)) else cast(columns[22] as varchar(200)) end as s_city, " +
        "case when (columns[23]='') then cast(null as varchar(200)) else cast(columns[23] as varchar(200)) end as s_county, " +
        "case when (columns[24]='') then cast(null as varchar(200)) else cast(columns[24] as varchar(200)) end as s_state, " +
        "case when (columns[25]='') then cast(null as varchar(200)) else cast(columns[25] as varchar(200)) end as s_zip, " +
        "case when (columns[26]='') then cast(null as varchar(200)) else cast(columns[26] as varchar(200)) end as s_country, " +
        "case when (columns[27]='') then cast(null as double) else cast(columns[27] as double) end as s_gmt_offset, " +
        "case when (columns[28]='') then cast(null as double) else cast(columns[28] as double) end as s_tax_precentage " +
        "from dfs_test.`/home/osboxes/files/dataset/store/store.tbl`", "dfs_test");

    testBuilder()
        .sqlQuery(String.format("select" +
            "  s.s_store_name," +
            "  s.s_store_id," +
            "  sum(case when (d.d_day_name = 'Sunday') then ss.ss_sales_price else null end) sun_sales," +
            "  sum(case when (d.d_day_name = 'Monday') then ss.ss_sales_price else null end) mon_sales," +
            "  sum(case when (d.d_day_name = 'Tuesday') then ss.ss_sales_price else null end) tue_sales," +
            "  sum(case when (d.d_day_name = 'Wednesday') then ss.ss_sales_price else null end) wed_sales," +
            "  sum(case when (d.d_day_name = 'Thursday') then ss.ss_sales_price else null end) thu_sales," +
            "  sum(case when (d.d_day_name = 'Friday') then ss.ss_sales_price else null end) fri_sales," +
            "  sum(case when (d.d_day_name = 'Saturday') then ss.ss_sales_price else null end) sat_sales" +
            " from" +
            "  %s.tmp.date_dim as d," +
            "  %s.tmp.store_sales as ss," +
            "  %s.tmp.store as s" +
            " where" +
            "  d.d_date_sk = ss.ss_sold_date_sk" +
            "  and s.s_store_sk = ss.ss_store_sk" +
            "  and s.s_gmt_offset = -5" +
            "  and d.d_year = 1998" +
            "  and ss.ss_sold_date_sk between 2450816 and 2451179" +
            " group by" +
            "  s.s_store_name," +
            "  s.s_store_id" +
            " order by" +
            "  s.s_store_name," +
            "  s.s_store_id," +
            "  sun_sales," +
            "  mon_sales," +
            "  tue_sales," +
            "  wed_sales," +
            "  thu_sales," +
            "  fri_sales," +
            "  sat_sales" +
        " limit 100", "dfs_test", "dfs_test", "dfs_test"))
        .ordered()
        .baselineColumns("s_store_name", "s_store_id", "sun_sales", "mon_sales", "tue_sales", "wed_sales", "thu_sales", "fri_sales", "sat_sales")
        .baselineValues("able","AAAAAAAACAAAAAAA",459564.3499999986,492832.2100000004,472234.89000000147,513998.9199999991,480718.60999999876,480887.2999999979,482390.9299999994)
        .baselineValues("ation","AAAAAAAAHAAAAAAA",468604.3999999996,467302.1000000005,470821.48999999993,510779.71000000066,481377.21000000107,473673.48999999923,471487.62000000075)
        .baselineValues("bar","AAAAAAAAKAAAAAAA",495341.9799999992,461667.2800000013,476791.3800000002,498368.240000001,475762.4300000013,476690.979999999,462227.2200000022)
        .baselineValues("eing","AAAAAAAAIAAAAAAA",487998.3200000025,460740.3699999989,486454.05999999976,447624.8700000004,468239.159999999,468815.85999999876,502495.15999999957)
        .baselineValues("ese","AAAAAAAAEAAAAAAA",466373.15000000113,477718.0200000016,486363.5899999991,490049.12000000017,483203.2500000001,470427.7700000023,497738.0200000024)
        .baselineValues("ought", "AAAAAAAABAAAAAAA", 442199.1699999991, 483945.97, 497270.79000000027, 483466.9200000013, 496373.0899999972, 491362.2499999975, 462524.8699999985)
        .go();

    testBuilder()
        .sqlQuery("select sum(ss_sales_price) as price from dfs_test.tmp.store_sales")
        .unOrdered()
        .baselineColumns("price")
        .baselineValues(1.0423193558999118E8)
        .go();

    testBuilder()
        .sqlQuery("select sum(cast(columns[1] as double)) as price from dfs_test.`/home/osboxes/files/decimal_test.tbl`")
        .unOrdered()
        .baselineColumns("price")
        .baselineValues(176.73000000000002)
        .go();

    testBuilder()
        .sqlQuery("select cast(columns[1] as double) as price from dfs_test.`/home/osboxes/files/decimal_test.tbl` where columns[0] = 'arina'")
        .unOrdered()
        .baselineColumns("price")
        .baselineValues(120.56)
        .go();
  }

  @Test
  public void compareRemoteAndLocalResults() throws Exception {
    Path dfsPath = new Path("/tmp/arina/");
    Path localPath = new Path("/home/osboxes/files/dataset/");
    fs.copyFromLocalFile(localPath, dfsPath);
    fs.copyFromLocalFile(new Path("/home/osboxes/files/decimal_large.tbl"), dfsPath);
    fs.copyFromLocalFile(new Path("/home/osboxes/files/decimal_large.dat"), dfsPath);

    fs.setOwner(dfsPath, processUser, processUser);
    fs.setPermission(dfsPath, new FsPermission((short) 0777));
    updateClient(processUser);

    testBuilder()
        .sqlQuery("select sum(cast(columns[1] as double)) from miniDfsPlugin.arina.`/arina/decimal_large.dat`")
        .unOrdered()
        .sqlBaselineQuery("select sum(cast(columns[1] as double)) from dfs_test.`/home/osboxes/files/decimal_large.tbl`")
        .go();

    /*
    test("create or replace view miniDfsPlugin.arina.decimal_view as select case when (columns[13]='') then cast(null as double) else cast(columns[13] as double) end as " +
        "price from " +
        "miniDfsPlugin.arina.`/arina/store_sales/store_sales.dat`");
    test("create or replace view dfs_test.tmp.decimal_view as select case when (columns[13]='') then cast(null as double) else cast(columns[13] as double) end as price from " +
        "dfs_test.`/home/osboxes/files/dataset/store_sales/store_sales.tbl`");
        */

/*    testBuilder()
        .sqlQuery("select sum(case when (columns[13]='') then cast(null as double) else cast(columns[13] as double) end) from miniDfsPlugin.arina.`/arina/store_sales/store_sales" +
            ".dat`")
        .unOrdered()
        .sqlBaselineQuery("select sum(case when (columns[13]='') then cast(null as double) else cast(columns[13] as double) end) from dfs_test" +
            ".`/home/osboxes/files/dataset/store_sales/store_sales.tbl`")
        .go();*/

    testBuilder()
        .sqlQuery("select sum(case when (columns[13]='') then cast(null as double) else cast(columns[13] as double) end) from miniDfsPlugin.arina.`/arina/store_sales/store_sales" +
            ".dat` limit 1")
        .unOrdered()
        .sqlBaselineQuery("select sum(case when (columns[13]='') then cast(null as double) else cast(columns[13] as double) end) from dfs_test" +
            ".`/home/osboxes/files/dataset/store_sales/store_sales.tbl` limit 1")
        .go();


  }

  @Test
  public void testDirectImpersonation_HasUserReadPermissions() throws Exception {
    // Table lineitem is owned by "user0_1:group0_1" with permissions 750. Try to read the table as "user0_1". We
    // shouldn't expect any errors.
    updateClient(org1Users[0]);
    test(String.format("SELECT * FROM %s.lineitem ORDER BY l_orderkey LIMIT 1", getWSSchema(org1Users[0])));
  }

  @Test
  public void testDirectImpersonation_HasGroupReadPermissions() throws Exception {
    // Table lineitem is owned by "user0_1:group0_1" with permissions 750. Try to read the table as "user1_1". We
    // shouldn't expect any errors as "user1_1" is part of the "group0_1"
    updateClient(org1Users[1]);
    test(String.format("SELECT * FROM %s.lineitem ORDER BY l_orderkey LIMIT 1", getWSSchema(org1Users[0])));
  }

  @Test
  public void testDirectImpersonation_NoReadPermissions() throws Exception {
    UserRemoteException ex = null;
    try {
      // Table lineitem is owned by "user0_1:group0_1" with permissions 750. Now try to read the table as "user2_1". We
      // should expect a permission denied error as "user2_1" is not part of the "group0_1"
      updateClient(org1Users[2]);
      test(String.format("SELECT * FROM %s.lineitem ORDER BY l_orderkey LIMIT 1", getWSSchema(org1Users[0])));
    } catch(UserRemoteException e) {
      ex = e;
    }

    assertNotNull("UserRemoteException is expected", ex);
    assertThat(ex.getMessage(), containsString("PERMISSION ERROR: " +
      String.format("Not authorized to read table [lineitem] in schema [%s.user0_1]",
        MINIDFS_STORAGE_PLUGIN_NAME)));
  }

  @Test
  public void testMultiLevelImpersonationEqualToMaxUserHops() throws Exception {
    updateClient(org1Users[4]);
    test(String.format("SELECT * from %s.u4_lineitem LIMIT 1;", getWSSchema(org1Users[4])));
  }

  @Test
  public void testMultiLevelImpersonationExceedsMaxUserHops() throws Exception {
    UserRemoteException ex = null;

    try {
      updateClient(org1Users[5]);
      test(String.format("SELECT * from %s.u4_lineitem LIMIT 1;", getWSSchema(org1Users[4])));
    } catch (UserRemoteException e) {
      ex = e;
    }

    assertNotNull("UserRemoteException is expected", ex);
    assertThat(ex.getMessage(),
      containsString("Cannot issue token for view expansion as issuing the token exceeds the maximum allowed number " +
        "of user hops (3) in chained impersonation"));
  }

  @Test
  public void testMultiLevelImpersonationJoinEachSideReachesMaxUserHops() throws Exception {
    updateClient(org1Users[4]);
    test(String.format("SELECT * from %s.u4_lineitem l JOIN %s.u3_orders o ON l.l_orderkey = o.o_orderkey LIMIT 1;",
      getWSSchema(org1Users[4]), getWSSchema(org2Users[3])));
  }

  @Test
  public void testMultiLevelImpersonationJoinOneSideExceedsMaxUserHops() throws Exception {
    UserRemoteException ex = null;

    try {
      updateClient(org1Users[4]);
      test(String.format("SELECT * from %s.u4_lineitem l JOIN %s.u4_orders o ON l.l_orderkey = o.o_orderkey LIMIT 1;",
          getWSSchema(org1Users[4]), getWSSchema(org2Users[4])));
    } catch(UserRemoteException e) {
      ex = e;
    }

    assertNotNull("UserRemoteException is expected", ex);
    assertThat(ex.getMessage(),
      containsString("Cannot issue token for view expansion as issuing the token exceeds the maximum allowed number " +
        "of user hops (3) in chained impersonation"));
  }

  @Test
  public void sequenceFileChainedImpersonationWithView() throws Exception {
    // create a view named "simple_seq_view" on "simple.seq". View is owned by user0:group0 and has permissions 750
    createView(org1Users[0], org1Groups[0], "simple_seq_view",
      String.format("SELECT convert_from(t.binary_key, 'UTF8') as k FROM %s.`%s` t", MINIDFS_STORAGE_PLUGIN_NAME,
        new Path(getUserHome(org1Users[0]), "simple.seq")));
    try {
      updateClient(org1Users[1]);
      test("SELECT k FROM %s.%s.%s", MINIDFS_STORAGE_PLUGIN_NAME, "tmp", "simple_seq_view");
    } catch (UserRemoteException e) {
      assertNull("This test should pass.", e);
    }
    createView(org1Users[1], org1Groups[1], "simple_seq_view_2",
      String.format("SELECT k FROM %s.%s.%s", MINIDFS_STORAGE_PLUGIN_NAME, "tmp", "simple_seq_view"));
    try {
      updateClient(org1Users[2]);
      test("SELECT k FROM %s.%s.%s", MINIDFS_STORAGE_PLUGIN_NAME, "tmp", "simple_seq_view_2");
    } catch (UserRemoteException e) {
      assertNull("This test should pass.", e);
    }
  }

  @Test
  public void avroChainedImpersonationWithView() throws Exception {
    createView(org1Users[0], org1Groups[0], "simple_avro_view",
      String.format("SELECT h_boolean, e_double FROM %s.`%s` t", MINIDFS_STORAGE_PLUGIN_NAME,
        new Path(getUserHome(org1Users[0]), "simple.avro")));
    try {
      updateClient(org1Users[1]);
      test("SELECT h_boolean FROM %s.%s.%s", MINIDFS_STORAGE_PLUGIN_NAME, "tmp", "simple_avro_view");
    } catch (UserRemoteException e) {
      assertNull("This test should pass.", e);
    }
  }

  @AfterClass
  public static void removeMiniDfsBasedStorage() throws Exception {
    getDrillbitContext().getStorage().deletePlugin(MINIDFS_STORAGE_PLUGIN_NAME);
    stopMiniDfsCluster();
  }
}
