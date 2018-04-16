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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper class that stores partition values per key.
 * Key to index mapper contains key and index corresponding to partition values position in partition values list.
 * Since several keys may have that same partition values, such structure is optimized to save memory usage.
 * Partition values are stored in list of consecutive values.
 */
public class HivePartitionHolder {

  private final Map<String, Integer> keyToIndexMapper;
  private final List<List<String>> partitionValues;

  @JsonCreator
  public HivePartitionHolder(@JsonProperty("keyToIndexMapper") Map<String, Integer> keyToIndexMapper,
                             @JsonProperty("partitionValues") List<List<String>> partitionValues) {
    this.keyToIndexMapper = keyToIndexMapper;
    this.partitionValues = partitionValues;
  }

  public HivePartitionHolder() {
    this.keyToIndexMapper = new HashMap<>();
    this.partitionValues = new ArrayList<>();
  }

  @JsonProperty
  public Map<String, Integer> getKeyToIndexMapper() {
    return keyToIndexMapper;
  }

  @JsonProperty
  public List<List<String>> getPartitionValues() {
    return partitionValues;
  }

  public void add(String path, List<String> values) {
    int index = partitionValues.indexOf(values);
    if (index == -1) {
      index = partitionValues.size();
      partitionValues.add(values);

    }
    keyToIndexMapper.put(path, index);
  }

  public List<String> get(String path) {
    Integer index = keyToIndexMapper.get(path);
    if (index == null) {
      return Collections.emptyList();
    }
    return partitionValues.get(index);
  }

}
