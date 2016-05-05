/**
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

import static java.lang.String.format;
import static org.apache.drill.TestBuilder.listOf;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.drill.exec.store.dfs.WorkspaceSchemaFactory;
import org.apache.drill.exec.util.JsonStringArrayList;
import org.apache.drill.exec.util.Text;
import org.junit.Ignore;
import org.junit.Test;

public class TestSelectWithOption extends BaseTestQuery {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WorkspaceSchemaFactory.class);

  private File genCSVFile(String name, String... rows) throws IOException {
    File file = new File(format("target/%s_%s.csv", this.getClass().getName(), name));
    try (FileWriter fw = new FileWriter(file)) {
      for (int i = 0; i < rows.length; i++) {
        fw.append(rows[i] + "\n");
      }
    }
    return file;
  }

  private String genCSVTable(String name, String... rows) throws IOException {
    File f = genCSVFile(name, rows);
    return format("dfs.`${WORKING_PATH}/%s`", f.getPath());
  }

  private void testWithResult(String query, Object... expectedResult) throws Exception {
    TestBuilder builder = testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("columns");
    for (Object o : expectedResult) {
      builder = builder.baselineValues(o);
    }
    builder.build().run();
  }

  @Test
  public void testTextFieldDelimiter() throws Exception {
    String tableName = genCSVTable("testTextFieldDelimiter",
        "\"b\"|\"0\"",
        "\"b\"|\"1\"",
        "\"b\"|\"2\"");

    String queryTemplate =
        "select columns from table(%s (type => 'TeXT', fieldDelimiter => '%s'))";
    testWithResult(format(queryTemplate, tableName, ","),
        listOf("b\"|\"0"),
        listOf("b\"|\"1"),
        listOf("b\"|\"2")
      );
    testWithResult(format(queryTemplate, tableName, "|"),
        listOf("b", "0"),
        listOf("b", "1"),
        listOf("b", "2")
      );
  }

  @Test @Ignore // It does not look like lineDelimiter is working
  public void testTextLineDelimiter() throws Exception {
    String tableName = genCSVTable("testTextLineDelimiter",
        "\"b\"|\"0\"",
        "\"b\"|\"1\"",
        "\"b\"|\"2\"");

    testWithResult(format("select columns from table(%s(type => 'TeXT', lineDelimiter => '|'))", tableName),
        listOf("\"b\""),
        listOf("\"0\"", "\"b\""),
        listOf("\"1\"", "\"b\""),
        listOf("\"2\"")
      );
  }

  @Test
  public void testTextQuote() throws Exception {
    String tableName = genCSVTable("testTextQuote",
        "\"b\"|\"0\"",
        "\"b\"|\"1\"",
        "\"b\"|\"2\"");

    testWithResult(format("select columns from table(%s(type => 'TeXT', fieldDelimiter => '|', quote => '@'))", tableName),
        listOf("\"b\"", "\"0\""),
        listOf("\"b\"", "\"1\""),
        listOf("\"b\"", "\"2\"")
        );

    String quoteTableName = genCSVTable("testTextQuote2",
        "@b@|@0@",
        "@b$@c@|@1@");
    // It seems that a parameter can not be called "escape"
    testWithResult(format("select columns from table(%s(`escape` => '$', type => 'TeXT', fieldDelimiter => '|', quote => '@'))", quoteTableName),
        listOf("b", "0"),
        listOf("b$@c", "1") // shouldn't $ be removed here?
        );
  }

  @Test
  public void testTextComment() throws Exception {
      String commentTableName = genCSVTable("testTextComment",
          "b|0",
          "@ this is a comment",
          "b|1");
      testWithResult(format("select columns from table(%s(type => 'TeXT', fieldDelimiter => '|', comment => '@'))", commentTableName),
          listOf("b", "0"),
          listOf("b", "1")
          );
  }

  @Test
  public void testTextHeader() throws Exception {
    String headerTableName = genCSVTable("testTextHeader",
        "b|a",
        "b|0",
        "b|1");
    testWithResult(format("select columns from table(%s(type => 'TeXT', fieldDelimiter => '|', skipFirstLine => true))", headerTableName),
        listOf("b", "0"),
        listOf("b", "1")
        );

    testBuilder()
        .sqlQuery(format("select a, b from table(%s(type => 'TeXT', fieldDelimiter => '|', extractHeader => true))", headerTableName))
        .ordered()
        .baselineColumns("b", "a")
        .baselineValues("b", "0")
        .baselineValues("b", "1")
        .build().run();
  }

  @Test
  public void testVariationsCSV() throws Exception {
    String csvTableName = genCSVTable("testVariationsCSV",
        "a,b",
        "c|d");
    // Using the defaults in TextFormatConfig (the field delimiter is neither "," not "|")
    String[] csvQueries = {
//        format("select columns from %s ('TeXT')", csvTableName),
//        format("select columns from %s('TeXT')", csvTableName),
        format("select columns from table(%s ('TeXT'))", csvTableName),
        format("select columns from table(%s (type => 'TeXT'))", csvTableName),
//        format("select columns from %s (type => 'TeXT')", csvTableName)
    };
    for (String csvQuery : csvQueries) {
      testWithResult(csvQuery,
          listOf("a,b"),
          listOf("c|d"));
    }
    // the drill config file binds .csv to "," delimited
    testWithResult(format("select columns from %s", csvTableName),
          listOf("a", "b"),
          listOf("c|d"));
    // setting the delimiter
    testWithResult(format("select columns from table(%s (type => 'TeXT', fieldDelimiter => ','))", csvTableName),
        listOf("a", "b"),
        listOf("c|d"));
    testWithResult(format("select columns from table(%s (type => 'TeXT', fieldDelimiter => '|'))", csvTableName),
        listOf("a,b"),
        listOf("c", "d"));
  }

  @Test
  public void testVariationsJSON() throws Exception {
    String jsonTableName = genCSVTable("testVariationsJSON",
        "{\"columns\": [\"f\",\"g\"]}");
    // the extension is actually csv
    testWithResult(format("select columns from %s", jsonTableName),
        listOf("{\"columns\": [\"f\"", "g\"]}\n")
        );
    String[] jsonQueries = {
        format("select columns from table(%s ('JSON'))", jsonTableName),
        format("select columns from table(%s(type => 'JSON'))", jsonTableName),
//        format("select columns from %s ('JSON')", jsonTableName),
//        format("select columns from %s (type => 'JSON')", jsonTableName),
//        format("select columns from %s(type => 'JSON')", jsonTableName),
        // we can use named format plugin configurations too!
        format("select columns from table(%s(type => 'Named', name => 'json'))", jsonTableName),
    };
    for (String jsonQuery : jsonQueries) {
      testWithResult(jsonQuery, listOf("f","g"));
    }
  }

  @Test
  public void testUse() throws Exception {
    File f = genCSVFile("testUse",
        "{\"columns\": [\"f\",\"g\"]}");
    String jsonTableName = format("`${WORKING_PATH}/%s`", f.getPath());
    // the extension is actually csv
    test("use dfs");
    try {
      String[] jsonQueries = {
          format("select columns from table(%s ('JSON'))", jsonTableName),
          format("select columns from table(%s(type => 'JSON'))", jsonTableName),
      };
      for (String jsonQuery : jsonQueries) {
        testWithResult(jsonQuery, listOf("f","g"));
      }

      testWithResult(format("select length(columns[0]) as columns from table(%s ('JSON'))", jsonTableName), 1L);
    } finally {
      test("use sys");
    }
  }

  @Test
  public void testLineDelimiter() throws Exception {

    JsonStringArrayList<Text> value1 = new JsonStringArrayList<>();

    value1.add(new Text("1"));
    value1.add(new Text("1990"));
    value1.add(new Text("Q3"));

    JsonStringArrayList<Text> value2 = new JsonStringArrayList<>();

    value2.add(new Text("2"));
    value2.add(new Text("1990"));
    value2.add(new Text("Q3"));

    JsonStringArrayList<Text> value3 = new JsonStringArrayList<>();

    value3.add(new Text("3"));
    value3.add(new Text("1990"));
    value3.add(new Text("Q3"));

    testBuilder()
        .sqlQuery("select columns from table(dfs.`F:\\drill\\files\\dirN\\1990\\1990_Q1_3.csv`(type=>'text',lineDelimiter=>'\r\n',fieldDelimiter=>','))")
        .unOrdered()
        .baselineColumns("columns")
        .baselineValues(value1)
        .baselineValues(value2)
        .baselineValues(value3)
        .go();
  }

  @Test
  public void testLineDelimiterMulti() throws Exception {

    JsonStringArrayList<Text> value1 = new JsonStringArrayList<>();
    value1.add(new Text("1"));
    value1.add(new Text(""));

    JsonStringArrayList<Text> value2 = new JsonStringArrayList<>();
    value2.add(new Text("2"));
    value2.add(new Text(""));

    JsonStringArrayList<Text> value3 = new JsonStringArrayList<>();
    value3.add(new Text("3"));
    value3.add(new Text(""));

    JsonStringArrayList<Text> values = new JsonStringArrayList<>();
    values.add(new Text("90"));
    values.add(new Text("Q3\r"));

    testBuilder()
        .sqlQuery("select columns from table(dfs.`F:\\drill\\files\\dirN\\1990\\1990_Q1_3.csv`(type=>'text',lineDelimiter=>'19',fieldDelimiter=>','))")
        .unOrdered()
        .baselineColumns("columns")
        .baselineValues(value1)
        .baselineValues(values)
        .baselineValues(value2)
        .baselineValues(values)
        .baselineValues(value3)
        .baselineValues(values)
        .go();
  }

  @Test
  public void testLineDelimiterMultiWithNewLineInTheEnd() throws Exception {

    JsonStringArrayList<Text> value1 = new JsonStringArrayList<>();
    value1.add(new Text("1"));
    value1.add(new Text("1990"));
    value1.add(new Text("Q"));

    JsonStringArrayList<Text> value2 = new JsonStringArrayList<>();
    value2.add(new Text("2"));
    value2.add(new Text("1990"));
    value2.add(new Text("Q"));

    JsonStringArrayList<Text> value3 = new JsonStringArrayList<>();
    value3.add(new Text("3"));
    value3.add(new Text("1990"));
    value3.add(new Text("Q"));

    testBuilder()
        .sqlQuery("select columns from table(dfs.`F:\\drill\\files\\dirN\\1990\\1990_Q1_4.csv`(type=>'text',lineDelimiter=>'\r\n',fieldDelimiter=>','))")
        .unOrdered()
        .baselineColumns("columns")
        .baselineValues(value1)
        .baselineValues(value2)
        .baselineValues(value3)
        .go();
  }

  @Test
  public void testLineDelimiterMultiCorrect() throws Exception {

    JsonStringArrayList<Text> value1 = new JsonStringArrayList<>();
    value1.add(new Text("1"));
    value1.add(new Text("1"));

    JsonStringArrayList<Text> values = new JsonStringArrayList<>();
    values.add(new Text("0"));
    values.add(new Text("Q5\r"));

    testBuilder()
        .sqlQuery("select columns from table(dfs.`F:\\drill\\files\\dirN\\1990\\1990_Q1_5.csv`(type=>'text',lineDelimiter=>'99',fieldDelimiter=>','))")
        .unOrdered()
        .baselineColumns("columns")
        .baselineValues(value1)
        .baselineValues(values)
        .go();
  }

  @Test
  public void testLargeCount() throws Exception {
    testBuilder()
        .sqlQuery("select count(*) as cnt from dfs.`F:\\drill\\files\\hive_5005500.csv`")
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(5005500L)
        .go();
  }

  @Test
  public void testErrorWithThreeDelimiters() throws Exception {
    test("select columns from table(dfs.`F:\\drill\\files\\hive_10.csv`(type=>'text',lineDelimiter=>'key',fieldDelimiter=>','))");
  }

  @Test
  public void testDfsAndCarriageReturn() throws Exception {
    JsonStringArrayList<Text> value = new JsonStringArrayList<>();
    value.add(new Text("1"));
    value.add(new Text("key_1\r"));
    testBuilder()
        .sqlQuery("select * from table(dfs.`F:\\drill\\files\\hive_1.csv`(type=>'text',lineDelimiter=>'k'))")
        .unOrdered()
        .baselineColumns("columns")
        .baselineValues(value)
        .go();
  }

}
