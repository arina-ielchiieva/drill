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
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestParquetFilterPushDownForDateTimeCasts extends PlanTestBase {

  private static final String TABLE_NAME = "dateTimeCasts";

  @BeforeClass
  public static void copyData() {
    dirTestWatcher.copyResourceToRoot(Paths.get("parquet", "voter_hive"));
    dirTestWatcher.copyResourceToRoot(Paths.get("parquet", "impala"));
    dirTestWatcher.copyResourceToRoot(Paths.get("parquet", "int96.parquet"));
  }

  //@BeforeClass
  public static void init() throws Exception {
    test("use dfs.tmp");
    test("create table `%s/p1` as\n" + "select timestamp '2017-01-01 00:00:00' as col_timestamp, date '2017-01-01' as col_date, time '00:00:00' as col_time from (values(1)) union all\n" + "select timestamp '2017-01-02 00:00:00' as col_timestamp, date '2017-01-02' as col_date, time '00:00:00' as col_time from (values(1)) union all\n" + "select timestamp '2017-01-02 21:01:15' as col_timestamp, date '2017-01-02' as col_date, time '21:01:15' as col_time from (values(1))", TABLE_NAME);

    test("create table `%s/p2` as\n" + "select timestamp '2017-01-03 08:50:00' as col_timestamp, date '2017-01-03' as col_date, time '08:50:00' as col_time from (values(1)) union all\n" + "select timestamp '2017-01-04 15:25:00' as col_timestamp, date '2017-01-04' as col_date, time '15:25:00' as col_time from (values(1)) union all\n" + "select timestamp '2017-01-04 22:14:29' as col_timestamp, date '2017-01-04' as col_date, time '22:14:29' as col_time from (values(1))", TABLE_NAME);

    test("create table `%s/p3` as\n" + "select timestamp '2017-01-05 05:46:11' as col_timestamp, date '2017-01-05' as col_date, time '05:46:11' as col_time from (values(1)) union all\n" + "select timestamp '2017-01-06 06:17:59' as col_timestamp, date '2017-01-06' as col_date, time '06:17:59' as col_time from (values(1)) union all\n" + "select timestamp '2017-01-06 06:17:59' as col_timestamp, date '2017-01-06' as col_date, time '06:17:59' as col_time from (values(1)) union all\n" + "select cast(null as timestamp) as col_timestamp, cast(null as date) as col_date, cast(null as time) as col_time from (values(1))", TABLE_NAME);
  }

  //@AfterClass
  public static void tearDown() throws Exception {
    test("drop table if exists `%s`", TABLE_NAME);
  }

  @Test
  public void testCastTimestampVarchar() throws Exception {
    testParquetFilterPushDown("col_timestamp = '2017-01-05 05:46:11'", TABLE_NAME, 1, 1);
    testParquetFilterPushDown("col_timestamp = cast('2017-01-05 05:46:11' as varchar)", TABLE_NAME, 1, 1);
    testParquetFilterPushDown("col_timestamp = cast('2017-01-05 05:46:11' as timestamp)", TABLE_NAME, 1, 1);
    testParquetFilterPushDown("col_timestamp > '2017-01-02 00:00:00'", TABLE_NAME, 7, 3);
    testParquetFilterPushDown("col_timestamp between '2017-01-03 21:01:15' and '2017-01-06 05:46:11'", TABLE_NAME, 3, 2);
    testParquetFilterPushDown("col_timestamp between '2017-01-03' and '2017-01-06'", TABLE_NAME, 4, 2);
  }

  @Test
  public void testCastTimestampDate() throws Exception {
    testParquetFilterPushDown("col_timestamp = date '2017-01-02'", TABLE_NAME, 1, 1);
    testParquetFilterPushDown("col_timestamp = cast(date '2017-01-02' as timestamp)", TABLE_NAME, 1, 1);
    testParquetFilterPushDown("col_timestamp > date '2017-01-02'", TABLE_NAME, 7, 3);
    testParquetFilterPushDown("col_timestamp between date '2017-01-03' and date '2017-01-06'", TABLE_NAME, 4, 2);
  }

  @Test
  public void testCastDateVarchar() throws Exception {
    testParquetFilterPushDown("col_date = '2017-01-02'", TABLE_NAME, 2, 1);
    testParquetFilterPushDown("col_date = cast('2017-01-02' as varchar)", TABLE_NAME, 2, 1);
    testParquetFilterPushDown("col_date = cast('2017-01-02' as date)", TABLE_NAME, 2, 1);
    testParquetFilterPushDown("col_date > '2017-01-02'", TABLE_NAME, 6, 2);
    testParquetFilterPushDown("col_date between '2017-01-02' and '2017-01-04'", TABLE_NAME, 5, 2);
  }

  @Test
  public void testCastDateTimestamp() throws Exception {
    testParquetFilterPushDown("col_date = timestamp '2017-01-02 00:00:00'", TABLE_NAME, 2, 1);
    testParquetFilterPushDown("col_date = cast(timestamp '2017-01-02 00:00:00' as date)", TABLE_NAME, 2, 1);
    testParquetFilterPushDown("col_date > timestamp '2017-01-02 21:01:15'", TABLE_NAME, 6, 2);
    testParquetFilterPushDown("col_date between timestamp '2017-01-03 08:50:00' and timestamp '2017-01-06 06:17:59'", TABLE_NAME, 5, 2);
  }

  @Test
  public void testCastTimeVarchar() throws Exception {
    testParquetFilterPushDown("col_time = '00:00:00'", TABLE_NAME, 2, 1);
    testParquetFilterPushDown("col_time = cast('00:00:00' as varchar)", TABLE_NAME, 2, 1);
    testParquetFilterPushDown("col_time = cast('00:00:00' as time)", TABLE_NAME, 2, 1);
    testParquetFilterPushDown("col_time > '15:25:00'", TABLE_NAME, 2, 2);
    testParquetFilterPushDown("col_time between '08:00:00' and '23:00:00'", TABLE_NAME, 4, 2);
  }

  @Test
  public void testCastTimeTimestamp() throws Exception {
    testParquetFilterPushDown("col_time = timestamp '2017-01-01 05:46:11'", TABLE_NAME, 1, 2);
    testParquetFilterPushDown("col_time = cast(timestamp '2017-01-01 05:46:11' as time)", TABLE_NAME, 1, 2);
    testParquetFilterPushDown("col_time = timestamp '2017-01-01 00:00:00'", TABLE_NAME, 2, 1);
    testParquetFilterPushDown("col_time > timestamp '2017-01-01 15:25:00'", TABLE_NAME, 2, 2);
    testParquetFilterPushDown("col_time between timestamp '2017-01-01 08:00:00' and timestamp '2017-01-01 23:00:00'", TABLE_NAME, 4, 2);
  }

  @Test
  public void testCastTimeDate() throws Exception {
    testParquetFilterPushDown("col_time = date '2017-01-01'", TABLE_NAME, 2, 1);
    testParquetFilterPushDown("col_time = cast(date '2017-01-01' as time)", TABLE_NAME, 2, 1);
    testParquetFilterPushDown("col_time > date '2017-01-01'", TABLE_NAME, 7, 3);
    testParquetFilterPushDown("col_time between date '2017-01-01' and date '2017-01-02'", TABLE_NAME, 2, 1);
  }

  @Test
  public void testHiveTimestamp() throws Exception {
    /*
      voter_1.parquet => 2017-03-23 17:17:05, 2016-10-15 11:31:17, 2016-12-29 18:30:19
      voter_2.parquet => 2017-01-29 14:44:37, 2017-03-18 15:36:04
      voter_3.parquet => 2016-12-07 20:10:48, null, 2016-04-14 10:26:17
     */
    try {
      test("alter session set `store.parquet.reader.int96_as_timestamp` = true");

      String tableName = "dfs.`parquet/voter_hive`";
      //testParquetFilterPushDown("create_timestamp = '2017-03-23 17:17:05'", tableName, 1, 1);
      testParquetFilterPushDown("create_timestamp > '2017-01-29 00:00:00'", tableName, 3, 2); //todo lost value from first file
      //testParquetFilterPushDown("create_timestamp between '2016-01-01 00:00:00' and '2017-01-01 00:00:00'", tableName, 4, 2);
    } finally {
      test("alter session reset `store.parquet.reader.int96_as_timestamp`");
    }
  }

  @Test
  public void testImpala() throws Exception {
    //test("alter session set `planner.enable_decimal_data_type` = true");
    test("alter session set `store.parquet.reader.int96_as_timestamp` = true");
    //String query = "select * from dfs.`parquet/voter_hive` where create_timestamp > '2017-01-29 00:00:00'"; // 2017-02-03 01:51:25
    //String query = "select * from dfs.`parquet/impala` where create_timestamp > '2017-01-29 00:00:00'"; // 2017-02-03 01:51:25
    String query = "select *, filename from dfs.`parquet/impala`"; // 2017-02-03 01:51:25
    //String query = "select *, filename from dfs.`parquet/int96.parquet`"; // 2017-02-03 01:51:25
    //String query = "select * from dfs.`parquet/impala` where `create_timestamp` = '2017-01-29 03:27:19.0'"; // 2017-02-03 01:51:25
    // 2016-07-09 17:47:21 - does not exist ... full scan when date was not be able to determine
//    String query = "select * from (select dir0, min(create_timestamp) as mn, max(create_timestamp) as mx" +
//        " from dfs.`parquet/impala/voter_parquet_part` group by dir0 order by dir0) t" +
//        " where mn <= '2017-01-29 03:27:19.0' and mx >= '2017-01-29 03:27:19.0'"; // result 20 rows
    // total = 144 files

    setColumnWidths(new int[]{40});
    List<QueryDataBatch> res = testSqlWithResults(query);
    printResult(res);

    String plan = PlanTestBase.getPlanInString("explain plan for " + query, PlanTestBase.OPTIQ_FORMAT);
    System.out.println(plan);


    // get parquet file schema
    ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(new Configuration(), new Path
        ("D:\\drill\\files\\voter_parquet_part_hive_2_1_2\\voter_parquet_part\\create_date=__HIVE_DEFAULT_PARTITION__\\000000_0"), ParquetMetadataConverter.NO_FILTER);
    //ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(new Configuration(), new Path
        //("D:\\drill\\files\\voter_parquet_part_hive_1_2\\create_date=__HIVE_DEFAULT_PARTITION__\\000000_0"), ParquetMetadataConverter.NO_FILTER);
    MessageType schema = parquetMetadata.getFileMetaData().getSchema();

    System.out.println(parquetMetadata.getFileMetaData().getCreatedBy());
    System.out.println(schema);

  }

  @Test
  public void createParquetFile() throws Exception {
    Path file = new Path("D:\\tmp\\files", "int96.parquet");
    Configuration conf = new Configuration();
    String schemaText = "message test { " + "required int96 int96_field; " + "} ";
    MessageType schema = MessageTypeParser.parseMessageType(schemaText);
    GroupWriteSupport.setSchema(schema, conf);

    ParquetWriter<Group> writer = ExampleParquetWriter.builder(file).withType(schema).withConf(conf).build();
/*
    Timestamp timestamp = new Timestamp(new Date().getTime());
    NanoTime nanoTime = NanoTimeUtils.getNanoTime(timestamp, true);

    ByteBuffer buf = ByteBuffer.allocate(12);
    buf.order(ByteOrder.LITTLE_ENDIAN);
    buf.putLong(nanoTime.getTimeOfDayNanos());
    buf.putInt(nanoTime.getJulianDay());
    buf.flip();
    org.apache.parquet.io.api.Binary binary = org.apache.parquet.io.api.Binary.fromReusedByteBuffer(buf);

    SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
    writer.write(
        groupFactory.newGroup()
            .append("int96_field", binary));
    writer.close();*/

  }


  private void testParquetFilterPushDown(String predicate, String tableName, int expectedRowCount, int expectedFilesNumber) throws Exception {
    String query = String.format("select * from %s where %s", tableName, predicate);

    int actualRowCount = testSql(query);
    assertEquals("Expected and actual row count should match", expectedRowCount, actualRowCount);

    String numFilesPattern = "numFiles=" + expectedFilesNumber;
    testPlanMatchingPatterns(query, new String[]{numFilesPattern}, new String[]{});
  }

}

