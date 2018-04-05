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

  public List<String> get(T path) {
    Integer index = filePathToIndexMapper.get(path);
    assert index != null;
    return partitionValues.get(index);
  }

  /**
   * An utility class that converts from {@link com.fasterxml.jackson.databind.JsonNode}
   * to DynamicPojoRecordReader during physical plan fragment deserialization.
   */
/*
  public static class Converter extends StdConverter<JsonNode, HivePartitionHolder> {
*/
/*    private static final TypeReference<LinkedHashMap<String, Class<?>>> schemaType =
      new TypeReference<LinkedHashMap<String, Class<?>>>() {};*//*


    private final ObjectMapper mapper;

    public Converter(ObjectMapper mapper)
    {
      this.mapper = mapper;
    }

    @Override
    public HivePartitionHolder convert(JsonNode value) {
      //LinkedHashMap<String, Class<?>> schema = mapper.convertValue(value.get("schema"), schemaType);

      //ArrayList records = new ArrayList(schema.size());
      //final Iterator<JsonNode> recordsIterator = value.get("records").get(0).elements();
      //for (Class<?> fieldType : schema.values()) {
        //records.add(mapper.convertValue(recordsIterator.next(), fieldType));
      //}
      //int maxRecordsToRead = value.get("recordsPerBatch").asInt();
      //return new DynamicPojoRecordReader(schema, Collections.singletonList(records), maxRecordsToRead);
    }
  }
*/

}
