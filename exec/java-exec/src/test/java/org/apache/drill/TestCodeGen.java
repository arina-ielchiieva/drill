/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to you under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
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

import org.apache.drill.common.config.DrillConfig;
import org.junit.Test;

import java.util.Properties;

public class TestCodeGen extends BaseTestQuery {

  @Test
  public void test() throws Exception {
    Properties overrideProps = new Properties();
    overrideProps.setProperty("drill.exec.compile.save_source", "true");
    overrideProps.setProperty("drill.exec.compile.code_dir", "/home/arina/temp");
    updateTestCluster(1, DrillConfig.create(overrideProps));

    testBuilder()
            .sqlQuery("select 'A' as a from (values(1))")
            .unOrdered()
            .baselineColumns("a")
            .baselineValues("A")
            .build()
            .run();
  }


}
