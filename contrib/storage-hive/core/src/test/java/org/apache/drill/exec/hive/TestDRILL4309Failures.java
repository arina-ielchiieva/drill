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
package org.apache.drill.exec.hive;

import org.apache.drill.exec.ExecConstants;
import org.junit.Test;

public class TestDRILL4309Failures extends HiveTestBase {

  @Test
  public void testMaxMinQuery() throws Exception {
    try {
      test(String.format("alter session set `%s` = true", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
      testBuilder()
          //.sqlQuery("select min(create_date) as mn, max(create_date) as mx from hive.voter_parquet")
          .sqlQuery("select min(CREATE_TIMESTAMP) as mn, max(CREATE_TIMESTAMP) as mx from hive.voter_parquet")
          .unOrdered()
          .baselineColumns("mn", "mx")
          .baselineValues("2016-04-15", "2017-04-12")
          .go();
      // true -> `mn` : -11348-01-31T00:00:00.000Z, `mx` : -11347-01-27T00:00:00.000Z,
      //  349-04-26	 348-04-23
      // false -> `mn` : 2016-04-15T00:00:00.000Z, `mx` : 2017-04-12T00:00:00.000Z,

    } finally {
      test(String.format("alter session set `%s` = true", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
    }
  }

  @Test
  public void testDateColumnSelect() throws Exception {
    try {
      test(String.format("alter session set `%s` = false", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
      testBuilder()
          .sqlQuery("select create_date as d from hive.voter_parquet where voter_id = 1")
          .unOrdered()
          .baselineColumns("d")
          .baselineValues("2016-07-05")
          .go();
      // true -> `d` : -11348-04-21T00:00:00.000Z
      // false -> `d` : 2016-07-05T00:00:00.000Z
    } finally {
      test(String.format("alter session set `%s` = true", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
    }
  }

}
