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
package org.apache.drill.metastore.iceberg.metadata;

import org.apache.drill.metastore.Metastore;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import java.util.Map;

/**
 * Implementation of {@link Metastore.Metadata} interface.
 * Provides information about current Metastore version and its properties.
 */
public class IcebergMetadata implements Metastore.Metadata {

  private final Table metastore;

  private IcebergMetadata(Table metastore) {
    this.metastore = metastore;
  }

  public static IcebergMetadata init(Table metastore) {
    return new IcebergMetadata(metastore);
  }

  @Override
  public boolean supportsVersioning() {
    return true;
  }

  @Override
  public long version() {
    Snapshot snapshot = metastore.currentSnapshot();
    return snapshot == null ? 0 : snapshot.snapshotId();
  }

  @Override
  public Map<String, String> properties() {
    return metastore.properties();
  }
}
