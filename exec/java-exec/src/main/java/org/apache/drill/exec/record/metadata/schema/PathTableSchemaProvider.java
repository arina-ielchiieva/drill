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

import org.apache.drill.exec.store.StorageStrategy;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedHashMap;

public class PathTableSchemaProvider implements TableSchemaProvider {

  private final Path path;
  private final FileSystem fs;

  public PathTableSchemaProvider(Path path) throws IOException {
    this(createFsFromPath(path), path);
  }

  public PathTableSchemaProvider(FileSystem fs, Path path) throws IOException {
    this.fs = ImpersonationUtil.createFileSystem(ImpersonationUtil.getProcessUserName(), fs.getConf());

    if (!fs.exists(path.getParent())) {
      throw new IOException(String.format("Parent path for schema file [%s] does not exist", path.toUri().getPath()));
    }

    this.path = path;
  }

  private static FileSystem createFsFromPath(Path path) throws IOException {
    Configuration conf = new Configuration();
    String scheme = path.toUri().getScheme();
    if (scheme != null) {
      conf.set(FileSystem.FS_DEFAULT_NAME_KEY, scheme);
    }
    return path.getFileSystem(conf);
    //conf.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
    //todo check manually how it works on dfs
  }

  @Override
  public void delete() throws IOException {
    try {
      if (!fs.delete(path, false)) {
        throw new IOException(String.format("Error while deleting schema file - [%s]", path.toUri().getPath()));
      }
    } catch (IOException e1) {
      // re-check file existence to cover concurrent deletion case
      try {
        if (exists()) {
          throw e1;
        }
      } catch (IOException e2) {
        // ignore new exception and throw original one
        throw e1;
      }
    }
  }

  @Override
  public void store(String schema, LinkedHashMap<String, String> properties) throws IOException {
    TableSchema tableSchema = createTableSchema(schema, properties);

    try (OutputStream stream = fs.create(path)) {
      MAPPER.writeValue(stream, tableSchema);
    }
    //new StorageStrategy(context.getOption(ExecConstants.PERSISTENT_TABLE_UMASK).string_val, false);
    StorageStrategy storageStrategy = StorageStrategy.DEFAULT;
    storageStrategy.applyToFile(fs, path);
  }

  @Override
  public TableSchema read() throws IOException {
    try (InputStream stream = fs.open(path)) {
      return MAPPER.readValue(stream, TableSchema.class);
    }
  }

  @Override
  public boolean exists() throws IOException {
    return fs.exists(path);
  }

  protected TableSchema createTableSchema(String schema, LinkedHashMap<String, String> properties) {
    return new TableSchema(null, schema, properties);
  }

}

