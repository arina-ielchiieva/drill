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
package org.apache.drill.exec.store;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.PARAM;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface PartitionExtractor<T> {

  List<String> getValues(T key);

  class DirectoryBasedPartitionExtractor implements PartitionExtractor<String> {

    private final String root;

    public DirectoryBasedPartitionExtractor(String root) {
      this.root = root;
    }

    @Override
    public List<String> getValues(String filePath) {
      return listDiffDirectoryNames(filePath, root);
    }

    /**
     * Compares root and file path to determine directories
     * that are present in the file path but absent in root.
     * Example: root - a/b/c, filePath - a/b/c/d/e, result - d/e.
     * Stores different directory names in the list in successive order.
     *
     *
     * @param filePath file path
     * @param root root directory
     * @return list of directory names
     */
    private List<String> listDiffDirectoryNames(String filePath, String root) {
      if (filePath == null || root == null) {
        return Collections.emptyList();
      }

      int rootDepth = new Path(root).depth();
      Path path = new Path(filePath);
      int pathDepth = path.depth();

      int diffCount = pathDepth - rootDepth;

      if (diffCount < 0) {
        return Collections.emptyList();
      }

      String[] diffDirectoryNames = new String[diffCount];

      // start filling in array from the end
      for (int i = rootDepth; pathDepth > i; i++) {
        path = path.getParent();
        // place in the end of array
        diffDirectoryNames[pathDepth - i - 1] = path.getName();
      }

      return Arrays.asList(diffDirectoryNames);
    }

  }

  class MapperBasedParitionExtractor<T> implements PartitionExtractor<T> {

    private final PartitionMapper<T> partitionMapper;

    public MapperBasedParitionExtractor(PartitionMapper<T> partitionMapper) {
      this.partitionMapper = partitionMapper;
    }

    @Override
    public List<String> getValues(T key) {
      return partitionMapper.get(key);
    }
  }

  class PartitionMapper<T> {

    private final Map<T, Integer> keyToIndexMapper;
    private final List<List<String>> partitionValues;

    @JsonCreator
    public PartitionMapper(@JsonProperty("keyToIndexMapper") Map<T, Integer> keyToIndexMapper,
                               @JsonProperty("partitionValues") List<List<String>> partitionValues) {
      this.keyToIndexMapper = keyToIndexMapper;
      this.partitionValues = partitionValues;
    }

    public PartitionMapper() {
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
      if (index == null) {
        return Collections.emptyList();
      }
      return partitionValues.get(index);
    }

  }

}
