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
package org.apache.drill.exec.store.hive;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.schema.Schema.TableType;

import org.apache.drill.exec.store.hive.HiveTableWrapper.HivePartitionWrapper;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class HiveReadEntry {

  private final HiveTableWrapper table;
  private final List<HivePartitionWrapper> partitions;
  private final List<HivePartition> partitionsUnwrapped;

  @JsonCreator
  public HiveReadEntry(@JsonProperty("table") HiveTableWrapper table,
                       @JsonProperty("partitions") List<HivePartitionWrapper> partitions) {
    this.table = table;
    this.partitions = partitions;
    this.partitionsUnwrapped = new ArrayList<>();
    if (partitions != null) {
      partitionsUnwrapped.addAll(partitions.stream()
          .map(HivePartitionWrapper::getPartition)
          .collect(Collectors.toList()));
    }
  }

  @JsonProperty("table")
  public HiveTableWrapper getTableWrapper() {
    return table;
  }

  @JsonProperty("partitions")
  public List<HivePartitionWrapper> getPartitionWrappers() {
    return partitions;
  }

  @JsonIgnore
  public HiveTableWithColumnCache getTable() {
    return table.getTable();
  }

  @JsonIgnore
  public List<HivePartition> getPartitions() {
    return partitionsUnwrapped;
  }

  @JsonIgnore
  public TableType getJdbcTableType() {
    if (table.getTable().getTableType().equals(org.apache.hadoop.hive.metastore.TableType.VIRTUAL_VIEW.toString())) {
      return TableType.VIEW;
    }

    return TableType.TABLE;
  }

}

