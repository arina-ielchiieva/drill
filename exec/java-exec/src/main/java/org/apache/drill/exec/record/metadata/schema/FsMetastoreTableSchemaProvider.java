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
package org.apache.drill.exec.record.metadata.schema;

import org.apache.drill.exec.store.dfs.WorkspaceSchemaFactory;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.LinkedHashMap;

public class FsMetastoreTableSchemaProvider extends PathTableSchemaProvider {

  private final String tableName;

  public FsMetastoreTableSchemaProvider(WorkspaceSchemaFactory.WorkspaceSchema wsSchema, String tableName) throws IOException {
    super(wsSchema.getFS(), generatePath(wsSchema, tableName));
    this.tableName = String.format("%s.`%s`", wsSchema.getFullSchemaName(), tableName);
  }

  private static Path generatePath(WorkspaceSchemaFactory.WorkspaceSchema wsSchema, String tableName) throws IOException {
    Path tablePath = new Path(wsSchema.getDefaultLocation(), tableName);
    Path schemaParentPath = wsSchema.getFS().isFile(tablePath) ? tablePath.getParent() : tablePath;
    return new Path(schemaParentPath, DEFAULT_SCHEMA_NAME);
  }

  @Override
  protected TableSchema createTableSchema(String schema, LinkedHashMap<String, String> properties) {
    return new TableSchema(tableName, schema, properties);
  }
}
