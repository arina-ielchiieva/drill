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
package org.apache.drill;

import org.apache.drill.categories.SqlTest;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Category(SqlTest.class)
public class TestCreateSchema extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    startCluster(builder);
  }

  @Test
  public void create() throws Exception {
    queryBuilder().sql("create table dfs.tmp.t as select * from sys.version").run();
    queryBuilder().sql(" create table schema " +
      "(col1 int, " +
      "col2 varchar( 20 ,  5 ), `col4(x\\)`(500) int) " +
      //"for dfs.tmp.t " +
      "path '(9)' " +
      "properties ( 'k1'='v1', 'k2'='v2', 'k3'='v3' )").printCsv();
  }

  @Test
  public void drop() throws Exception {
    //queryBuilder().sql("create temporary table t as select * from sys.version").run();
    queryBuilder().sql("create table dfs.tmp.t as select * from sys.version").run();
    queryBuilder().sql("select * from dfs.tmp.`t/0_0_0.parquet`").printCsv();
    queryBuilder().sql("drop table schema " +
      //"if exists " +
      "for dfs.tmp.`t/0_0_0.parquet`").printCsv();
  }

  @Test
  public void testWordCount() {
    String word = "abcdeab";
    String[] letters = word.split("");
    Map<String, Long> result =
      Arrays.stream(letters)
        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    System.out.println(result);
  }

}
