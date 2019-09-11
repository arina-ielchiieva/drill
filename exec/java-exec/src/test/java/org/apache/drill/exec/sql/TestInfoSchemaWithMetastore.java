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
package org.apache.drill.exec.sql;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.metastore.Metastore;
import org.apache.drill.metastore.MetastoreRegistry;
import org.apache.drill.metastore.components.tables.TableMetadataUnit;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestInfoSchemaWithMetastore extends ClusterTest {
  @ClassRule
  public static TemporaryFolder root = new TemporaryFolder();

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    builder.configProperty(ExecConstants.ZK_ROOT, root.getRoot().toString());
    builder.sessionOption(ExecConstants.METASTORE_ENABLED, true);
    startCluster(builder);
  }

  @Test
  public void t() throws Exception {
    MetastoreRegistry metastoreRegistry = client.cluster().drillbit().getContext().getMetastoreRegistry();
    Metastore metastore = metastoreRegistry.get();
    System.out.println("Current Metastore version: " + metastore.tables().metadata().version());

    metastore.tables().modify().overwrite(
      TableMetadataUnit.builder()
        .storagePlugin("dfs")
        .workspace("tmp")
        .tableName("nation")
        .metadataKey("GENERAL_INFO")
        .metadataType("TABLE")
        .build(),
      TableMetadataUnit.builder()
        .storagePlugin("dfs")
        .workspace("tmp")
        .tableName("region")
        .metadataKey("GENERAL_INFO")
        .metadataType("TABLE")
        .build(),
      TableMetadataUnit.builder()
        .storagePlugin("dfs")
        .workspace("default")
        .tableName("test")
        .metadataKey("GENERAL_INFO")
        .metadataType("TABLE")
        .build())
      .execute();

    queryBuilder().sql("select * from information_schema.`tables`").print();
  }
}
