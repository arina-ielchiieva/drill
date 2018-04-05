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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.StdConverter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HivePartitionHolder<T> {

  private final Map<T, Integer> keyToIndexMapper;
  private final List<List<String>> partitionValues;

  @JsonCreator
  public HivePartitionHolder(@JsonProperty("keyToIndexMapper") Map<T, Integer> keyToIndexMapper,
                             @JsonProperty("partitionValues") List<List<String>> partitionValues) {
    this.keyToIndexMapper = keyToIndexMapper;
    this.partitionValues = partitionValues;
  }

  public HivePartitionHolder() {
    this.keyToIndexMapper = new HashMap<>();
    this.partitionValues = new ArrayList<>();
  }

  @JsonProperty
  public Map<T, Integer> getKeyToIndexMapper() {
    return keyToIndexMapper;
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
    keyToIndexMapper.put(path, index);
  }

  public List<String> get(T path) {
    Integer index = keyToIndexMapper.get(path);
    assert index != null;
    return partitionValues.get(index);
  }

}
