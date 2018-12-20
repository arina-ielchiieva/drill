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
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.planner.sql.handlers.TableSchemaHandler;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

@Category(SqlTest.class)
public class TestCreateSchema extends ClusterTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

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
      "path '(9)' "
      + "properties ( 'k1'='v1', 'k2'='v2', 'k3'='v3' )"
    ).printCsv();
  }

  @Test
  public void testCreateWithoutSchema() throws Exception {
    thrown.expect(UserException.class);
    thrown.expectMessage("PARSE ERROR: Lexical error");

    queryBuilder().sql("create table schema for tbl").run();
  }

  @Test
  public void testCreateWithForAndPath() throws Exception {
    thrown.expect(UserException.class);
    thrown.expectMessage("PARSE ERROR: Encountered \"path\"");

    queryBuilder().sql("create table schema (col1 int, col2 int) for tbl path '/tmp/schema.file'").run();
  }

  @Test
  public void testCreateWithPathAndOrReplace() throws Exception {
    thrown.expect(UserException.class);
    thrown.expectMessage("PARSE ERROR: <OR REPLACE> cannot be used with <PATH> property");

    queryBuilder().sql("create or replace table schema (col1 int, col2 int) path '/tmp/schema.file'").run();
  }

  @Test
  public void testCreateForMissingTable() throws Exception {

  }

  @Test
  public void testCreateForTemporaryTable() throws Exception {

  }

  @Test
  public void testCreateForImmutableSchema() throws Exception {

  }

  @Test
  public void testDropWithoutTable() throws Exception {
    thrown.expect(UserException.class);
    thrown.expectMessage("PARSE ERROR: Encountered \"<EOF>\"");

    queryBuilder().sql("drop table schema").run();
  }

  @Test
  public void testDropForMissingTable() throws Exception {
    thrown.expect(UserException.class);
    thrown.expectMessage("VALIDATION ERROR: Table [t] was not found.");

    queryBuilder().sql("drop table schema for dfs.t").run();
  }

  @Test
  public void testDropForTemporaryTable() throws Exception {
    String tableName = "temp_drop";
    try {
      queryBuilder().sql(String.format("create temporary table %s as select 'a' as c from (values(1))", tableName)).run();
      thrown.expect(UserException.class);
      thrown.expectMessage(String.format("VALIDATION ERROR: Indicated table [%s] is temporary table.", tableName));

      queryBuilder().sql(String.format("drop table schema for %s", tableName)).run();
    } finally {
      queryBuilder().sql(String.format("drop table if exists %s", tableName)).run();
    }
  }

  @Test
  public void testDropForImmutableSchema() throws Exception {

  }

  @Test
  public void testDropForMissingSchema() throws Exception {
    String tableName = "dfs.tmp.`table_with_missing_schema`";
    try {
      queryBuilder().sql(String.format("create table %s as select 'a' as c from (values(1))", tableName)).run();
      thrown.expect(UserException.class);
      thrown.expectMessage(String.format("VALIDATION ERROR: Schema file [%s] " +
        "does not exist in table [%s] root directory", TableSchemaHandler.DEFAULT_SCHEMA_NAME, tableName));

      queryBuilder().sql(String.format("drop table schema for %s", tableName)).run();
    } finally {
      queryBuilder().sql(String.format("drop table if exists %s", tableName)).run();
    }
  }

  @Test
  public void testDropForMissingSchemaIfExists() throws Exception {
    String tableName = "dfs.tmp.`table_with_missing_schema_if_exists`";
    try {
      queryBuilder().sql(String.format("create table %s as select 'a' as c from (values(1))", tableName)).run();

      client.testBuilder()
        .sqlQuery("drop table schema if exists for %s", tableName)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, String.format("Schema file [%s] does not exist in table [%s] root directory",
          TableSchemaHandler.DEFAULT_SCHEMA_NAME, tableName))
        .go();
    } finally {
      queryBuilder().sql(String.format("drop table if exists %s", tableName)).run();
    }
  }

  @Test
  public void testSuccessfulDrop() throws Exception {
    //todo need first to implement file creation
  }



}
