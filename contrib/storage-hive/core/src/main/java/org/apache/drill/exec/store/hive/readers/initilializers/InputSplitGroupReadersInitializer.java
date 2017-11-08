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
package org.apache.drill.exec.store.hive.readers.initilializers;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.hive.HivePartition;
import org.apache.drill.exec.store.hive.HiveSubScan;
import org.apache.drill.exec.store.hive.readers.HiveAbstractReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Create readers for each input split group. Input split group is determined by unique file.
 * When reading from distributed file system, one file can be split into several input splits.
 * For correct skip header / footer logic application these input splits should be processed by one reader.
 */
public class InputSplitGroupReadersInitializer extends AbstractReadersInitializer {

  public InputSplitGroupReadersInitializer(FragmentContext context, HiveSubScan config, Class<? extends HiveAbstractReader> readerClass) {
    super(context, config, readerClass);
  }

  @Override
  public List<RecordReader> init() {
    Multimap<Path, InputSplit> inputSplitGroups = transformInputSplits(config.getInputSplits());
    Map<String, HivePartition> partitions = transformPartitions(config.getPartitions());

    List<RecordReader> readers = new ArrayList<>();
    Constructor<? extends HiveAbstractReader> readerConstructor = createReaderConstructor(Collection.class);
    for (Map.Entry<Path, Collection<InputSplit>> entry : inputSplitGroups.asMap().entrySet()) {
      Collection<InputSplit> value = entry.getValue();
      HivePartition partition = partitions.get(entry.getKey().getParent().toUri().toString());
      readers.add(createReader(readerConstructor, partition, value));
    }
    return readers;
  }

  /**
   * Transforms input splits into groups by file path.
   * Key is file path and value is list of inputs splits that belong to the same file.
   *
   * @param inputSplitsList list of input splits
   * @return map with transformed input splits
   */
  private Multimap<Path, InputSplit> transformInputSplits(List<InputSplit> inputSplitsList) {
    Multimap<Path, InputSplit> inputSplitGroups = ArrayListMultimap.create();
    for (InputSplit inputSplit : inputSplitsList) {
      inputSplitGroups.put(((FileSplit) inputSplit).getPath(), inputSplit);
    }
    return inputSplitGroups;
  }

  /**
   * Transforms partitions into map by partition location.
   * Key is partition location and value is corresponding partition.
   * If partition location already exists will override it
   * since the same partition can be used for all the files in that particular location.
   * If no partitions are present, returns empty map.
   *
   * @param partitionsList list of partitions
   * @return map with transformed partitions
   */
  private Map<String, HivePartition> transformPartitions(List<HivePartition> partitionsList) {
    Map<String, HivePartition> partitions = new HashMap<>();
    if (partitionsList != null) {
      for (HivePartition partition : partitionsList) {
        partitions.put(partition.getSd().getLocation(), partition);
      }
    }
    return partitions;
  }

}
