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
package org.apache.drill.metastore.iceberg.transform;

import org.apache.drill.metastore.MetadataUnit;
import org.apache.hadoop.fs.Path;

import java.util.Objects;

/**
 * Is used to uniquely identify Drill table based on storage plugin, workspace and table name.
 */
public class DrillTableKey {

  private final String storagePlugin;
  private final String workspaceName;
  private final String tableName;

  public DrillTableKey(String storagePlugin, String workspaceName, String tableName) {
    this.storagePlugin = storagePlugin;
    this.workspaceName = workspaceName;
    this.tableName = tableName;
  }

  public static DrillTableKey of(MetadataUnit unit) {
    return new DrillTableKey(unit.storagePlugin(), unit.workspace(), unit.tableName());
  }

  public String storagePlugin() {
    return storagePlugin;
  }

  public String workspace() {
    return workspaceName;
  }

  public String tableName() {
    return tableName;
  }

  /**
   * Constructs table location based on given Iceberg table location.
   * For example, metadata for the table dfs.tmp.nation will be stored in
   * [METASTORE_ROOT_DIRECTORY]/dfs/tmp/nation folder.
   *
   * @param base Iceberg table location
   * @return table location
   */
  public String toLocation(String base) {
    Path path = new Path(base);
    path = new Path(path, storagePlugin);
    path = new Path(path, workspaceName);
    path = new Path(path, tableName);
    return path.toUri().getPath();
  }

  @Override
  public int hashCode() {
    return Objects.hash(storagePlugin, workspaceName, tableName);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DrillTableKey that = (DrillTableKey) o;
    return Objects.equals(storagePlugin, that.storagePlugin)
      && Objects.equals(workspaceName, that.workspaceName)
      && Objects.equals(tableName, that.tableName);
  }
}
