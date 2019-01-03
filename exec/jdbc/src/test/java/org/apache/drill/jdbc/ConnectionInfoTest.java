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
package org.apache.drill.jdbc;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.drill.categories.JdbcTest;
import org.apache.drill.categories.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Test for Drill's Properties in the JDBC URL connection string
 */
@Category({SlowTest.class, JdbcTest.class})
public class ConnectionInfoTest extends JdbcTestBase {
  private static Connection connection;
  private static DatabaseMetaData dbmd;

  @Test
  public void testQuotingIdentifiersProperty() throws SQLException {
    try {
      // Test DoubleQuotes for the DrillProperty#QUOTING_IDENTIFIERS in connection URL
      connection = connect("jdbc:drill:zk=local;quoting_identifiers='\"'");
      dbmd = connection.getMetaData();
      assertThat(dbmd.getIdentifierQuoteString(), equalTo(Quoting.DOUBLE_QUOTE.string));
      reset();

      // Test Brackets for the DrillProperty#QUOTING_IDENTIFIERS in connection URL
      connection = connect("jdbc:drill:zk=local;quoting_identifiers=[");
      dbmd = connection.getMetaData();
      assertThat(dbmd.getIdentifierQuoteString(), equalTo(Quoting.BRACKET.string));
    } finally {
      reset();
    }
  }

  @Test(expected = SQLException.class)
  public void testIncorrectCharacterForQuotingIdentifiers() throws SQLException {
    try {
      connection = connect("jdbc:drill:zk=local;quoting_identifiers=&");
    }
    catch (SQLException e) {
      // Check exception text message
      assertThat(e.getMessage(), containsString("Option planner.parser.quoting_identifiers " +
          "must be one of: [`, \", []"));
      throw e;
    } finally {
      reset();
    }
  }

  @Test
  public void testSchema() throws Exception {
    try {
      connection = connect("jdbc:drill:zk=local");
      assertNull(connection.getSchema());

      connection.setSchema("dfs.tmp");
      assertEquals("dfs.tmp", connection.getSchema());

      try {
        connection.setSchema("ABC");
        fail();
      } catch (SQLException e) {
        //todo check root cause
        System.out.println(e);
      }

      reset();

      connection = connect("jdbc:drill:zk=local;schema=sys");
      assertEquals("sys", connection.getSchema());
    } finally {
      reset();
    }
  }

}
