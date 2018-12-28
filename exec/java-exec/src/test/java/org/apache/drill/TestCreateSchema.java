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
import org.apache.drill.exec.record.metadata.schema.TableSchemaProvider;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.nio.file.Paths;

import static org.junit.Assert.assertTrue;


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
  public void createForPath() throws Exception {
    String query = "create table schema (col1 int, col2 int) path '/tmp/schema.json' " +
      "properties ('key1' = 'val1', 'key2' = 'val2')";
    queryBuilder().sql(query).printCsv();

    // why we started the second time? -> because of the registry sync
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
    String table = "dfs.tmp.tbl";
    thrown.expect(UserException.class);
    thrown.expectMessage("VALIDATION ERROR: Table [tbl] was not found");

    queryBuilder().sql(String.format("create table schema (col1 int, col2 int) for %s", table)).run();
  }

  @Test
  public void testCreateForTemporaryTable() throws Exception {
    String table = "temp_create";
    try {
      queryBuilder().sql(String.format("create temporary table %s as select 'a' as c from (values(1))", table)).run();
      thrown.expect(UserException.class);
      thrown.expectMessage(String.format("VALIDATION ERROR: Indicated table [%s] is temporary table", table));

      queryBuilder().sql(String.format("create table schema (col1 int, col2 int) for %s", table)).run();
    } finally {
      queryBuilder().sql(String.format("drop table if exists %s", table)).run();
    }
  }

  @Test
  public void testCreateForImmutableSchema() throws Exception {
    String table = "sys.version";
    thrown.expect(UserException.class);
    thrown.expectMessage("VALIDATION ERROR: Unable to create or drop objects. Schema [sys] is immutable");

    queryBuilder().sql(String.format("create table schema (col1 int, col2 int) for %s", table)).run();
  }

  @Test
  public void testMissingParentDirectory() throws Exception {
    File tmpDir = dirTestWatcher.getTmpDir();
    File schema = Paths.get(tmpDir.toURI().getPath(), "missing_parent_directory", "file.schema").toFile();

    thrown.expect(UserException.class);
    thrown.expectMessage(String.format("RESOURCE ERROR: Error while preparing / creating schema file path for [%s]", schema.toURI().getPath()));

    queryBuilder().sql(String.format("create table schema (col1 int, col2 int) path '%s'", schema.toURI().getPath())).run();
  }

  @Test
  public void testCreateSimpleForPathWithExistingSchema() throws Exception {
    File tmpDir = dirTestWatcher.getTmpDir();
    File schema = new File(tmpDir, "simple_for_path.schema");
    assertTrue(schema.createNewFile());

    thrown.expect(UserException.class);
    thrown.expectMessage(String.format("Schema file already exists for [%s]", schema.toURI().getPath()));

    try {
      queryBuilder().sql(String.format("create table schema (col1 int, col2 int) path '%s'", schema.toURI().getPath())).run();
    } finally {
      assertTrue(schema.delete());
    }
  }

  @Test
  public void testCreateIfNotExistsForPath() throws Exception {
    File tmpDir = dirTestWatcher.getTmpDir();
    File schema = new File(tmpDir, "if_not_exists_for_path.schema");
    assertTrue(schema.createNewFile());

    try {
      client.testBuilder()
        .sqlQuery("create table schema if not exists (col1 int, col2 int) path '%s'", schema.toURI().getPath())
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, String.format("Schema file already exists for [%s]", schema.toURI().getPath()))
        .go();
    } finally {
      assertTrue(schema.delete());
    }
  }

  @Test
  public void testCreateSimpleForTableWithExistingSchema() throws Exception {

  }

  @Test
  public void testCreateIfNotExistsForTableWithExistingSchema() throws Exception {

  }

  @Test
  public void testCreateXXX() throws Exception {
    // or replace
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
    thrown.expectMessage("VALIDATION ERROR: Table [t] was not found");

    queryBuilder().sql("drop table schema for dfs.t").run();
  }

  @Test
  public void testDropForTemporaryTable() throws Exception {
    String table = "temp_drop";
    try {
      queryBuilder().sql(String.format("create temporary table %s as select 'a' as c from (values(1))", table)).run();
      thrown.expect(UserException.class);
      thrown.expectMessage(String.format("VALIDATION ERROR: Indicated table [%s] is temporary table", table));

      queryBuilder().sql(String.format("drop table schema for %s", table)).run();
    } finally {
      queryBuilder().sql(String.format("drop table if exists %s", table)).run();
    }
  }

  @Test
  public void testDropForImmutableSchema() throws Exception {
    String table = "sys.version";
    thrown.expect(UserException.class);
    thrown.expectMessage("VALIDATION ERROR: Unable to create or drop objects. Schema [sys] is immutable");

    queryBuilder().sql(String.format("drop table schema for %s", table)).run();
  }

  @Test
  public void testDropForMissingSchema() throws Exception {
    String table = "dfs.tmp.table_with_missing_schema";
    try {
      queryBuilder().sql(String.format("create table %s as select 'a' as c from (values(1))", table)).run();
      thrown.expect(UserException.class);
      thrown.expectMessage(String.format("VALIDATION ERROR: Schema file [%s] " +
        "does not exist in table [%s] root directory", TableSchemaProvider.DEFAULT_SCHEMA_NAME, table));

      queryBuilder().sql(String.format("drop table schema for %s", table)).run();
    } finally {
      queryBuilder().sql(String.format("drop table if exists %s", table)).run();
    }
  }

  @Test
  public void testDropForMissingSchemaIfExists() throws Exception {
    String table = "dfs.tmp.table_with_missing_schema_if_exists";
    try {
      queryBuilder().sql(String.format("create table %s as select 'a' as c from (values(1))", table)).run();

      client.testBuilder()
        .sqlQuery("drop table schema if exists for %s", table)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, String.format("Schema file [%s] does not exist in table [%s] root directory",
          TableSchemaProvider.DEFAULT_SCHEMA_NAME, table))
        .go();
    } finally {
      queryBuilder().sql(String.format("drop table if exists %s", table)).run();
    }
  }

  @Test
  public void testSuccessfulDrop() throws Exception {
    //todo need first to implement file creation
  }



}
