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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HivePartitionHolder<T> {

  private final Map<T, Integer> filePathToIndexMapper;
  private final List<List<String>> partitionValues;

  @JsonCreator
  public HivePartitionHolder(@JsonProperty("filePathToIndexMapper") Map<T, Integer> filePathToIndexMapper,
                             @JsonProperty("partitionValues") List<List<String>> partitionValues) {
    this.filePathToIndexMapper = filePathToIndexMapper;
    this.partitionValues = partitionValues;
  }

  public HivePartitionHolder() {
    this.filePathToIndexMapper = new HashMap<>();
    this.partitionValues = new ArrayList<>();
  }

  @JsonProperty
  public Map<T, Integer> getFilePathToIndexMapper() {
    return filePathToIndexMapper;
  }

  @JsonProperty
  public List<List<String>> getPartitionValues() {
    return partitionValues;
  }

  public void add(T path, List<String> values) {
    int index = partitionValues.indexOf(values);
    if (index == -1) {
      index = partitionValues.size();
      partitionValues.add(values);

    }
    filePathToIndexMapper.put(path, index);
  }

  public boolean hasPartitions() {
    return !partitionValues.isEmpty();
  }

  public List<String> get(T path) {
    Integer index = filePathToIndexMapper.get(path);
    assert index != null;
    return partitionValues.get(index);
  }

}
