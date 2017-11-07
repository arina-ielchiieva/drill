/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.hive.readers.inspectors;

import org.apache.hadoop.mapred.RecordReader;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * To implement skip footer logic this records inspector will buffer N number of incoming read records in queue
 * and make sure they are skipped when input is fully processed. FIFO method of queuing is used for these purposes.
 */
public class SkipFooterRecordsInspector extends AbstractRecordsInspector {

  private final int footerCount;
  private Queue<Object> footerBuffer;
  private final List<Object> valueHolders;
  private int holderIndex;
  private int tempRecordCount;

  public SkipFooterRecordsInspector(int footerCount) {
    this.footerCount = footerCount;
    this.footerBuffer = new LinkedList<>();
    this.valueHolders = new ArrayList<>(footerCount + 1);
    this.holderIndex = -1;
    this.tempRecordCount = 0;
  }

  /**
   * Returns next available value holder where value should be written.
   * If value holder was not initialized, creates it and stores to future re-use.
   *
   * @return value holder
   */
  @Override
  public Object getValueHolder(RecordReader reader) {
    Object valueHolder = getCurrentValueHolder();
    if (valueHolder == null) {
      valueHolder = reader.createValue();
      valueHolders.set(holderIndex, valueHolder);
    }
    return valueHolder;
  }

  /**
   * Buffers current value holder with written value
   * and returns last buffered value if number of buffered values exceeds N records to skip.
   *
   * @return next available value holder with written value, null otherwise
   */
  @Override
  public Object getNextValue() {
    footerBuffer.add(getCurrentValueHolder());
    tempRecordCount++;
    if (footerBuffer.size() <= footerCount) {
      return null;
    }
    return footerBuffer.poll();
  }

  /**
   * resets number of temporary
   */
  @Override
  public void reset() {
    super.reset();
    tempRecordCount = holderIndex + 1;
  }

  /**
   * Returns current value holder. Position is determined based on
   *
   * @return value holder
   */
  private Object getCurrentValueHolder() {
    holderIndex = tempRecordCount % valueHolders.size();
    return valueHolders.get(holderIndex);
  }

}

/*  LEGACY

   * To take into account Hive "skip.header.lines.count" property first N values from file are skipped.
   * Since file can be read in batches (depends on TARGET_RECORD_COUNT), additional checks are made
   * to determine if it's new file or continuance.
   *
   * To take into account Hive "skip.footer.lines.count" property values are buffered in queue
   * until queue size exceeds number of footer lines to skip, then first value in queue is retrieved.
   * Buffer of value objects is used to re-use value objects in order to reduce number of created value objects.
   * For each new file queue is cleared to drop footer lines from previous file.

 */