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
import java.util.List;

/**
 * Create readers for each input split group. Input split group is determined by unique file.
 * When reading from distributed file system, one file can be split into several input splits.
 * For correct skip header / footer logic application these input splits should be processed in one reader.
 */
public class InputSplitGroupReadersInitializer extends AbstractReadersInitializer {

  public InputSplitGroupReadersInitializer(FragmentContext context, HiveSubScan config, Class<? extends HiveAbstractReader> readerClass) {
    super(context, config, readerClass);
  }

  @Override
  public List<RecordReader> init() {
    List<RecordReader> readers = new ArrayList<>();
    List<InputSplit> inputSplits = config.getInputSplits();
    List<HivePartition> partitions = config.getPartitions();
    Constructor<? extends HiveAbstractReader> readerConstructor = createReaderConstructor(List.class);

    boolean hasPartitions = hasPartitions(partitions);

    // prepare input splits groups by file name
    List<InputSplit> inputSplitGroup = new ArrayList<>();
    Path previousPath = null;
    for (int i = 0 ; i < inputSplits.size(); i++) {
      FileSplit fileSplit = (FileSplit) inputSplits.get(i);
      Path currentPath = fileSplit.getPath();
      if (previousPath == null) {
        previousPath = currentPath;
      }

      if (!previousPath.equals(currentPath)) {
        // path is not equal, create reader for current input split group
        readers.add(createReader(readerConstructor, hasPartitions ? partitions.get(i - 1) : null, inputSplitGroup));
        // update the path, start new input split group
        previousPath = currentPath;
        inputSplitGroup = new ArrayList<>();
      }

      inputSplitGroup.add(fileSplit);
    }

    // add leftovers if any
    if (!inputSplitGroup.isEmpty()) {
      readers.add(createReader(readerConstructor, hasPartitions ? partitions.get(partitions.size() - 1) : null, inputSplitGroup));
    }

    return readers;
  }
}
