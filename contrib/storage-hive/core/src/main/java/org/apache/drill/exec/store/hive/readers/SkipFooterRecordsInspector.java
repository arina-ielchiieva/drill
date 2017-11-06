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
package org.apache.drill.exec.store.hive.readers;

import org.apache.hadoop.mapred.RecordReader;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class SkipFooterRecordsInspector extends AbstractRecordsInspector {

  private final int footerCount;
  private Queue<Object> footerBuffer;
  private final List<Object> valueHolders;
  private int holderIndex;
  private int tempRecordCount;


  public SkipFooterRecordsInspector(RecordReader reader, int footerCount) {
    this.footerCount = footerCount;
    this.footerBuffer = new LinkedList<>();
    this.valueHolders = initializeValueHolders(reader, footerCount);
    this.holderIndex = -1;
    this.tempRecordCount = 0;
  }

  @Override
  public Object getValueHolder() {
    holderIndex = tempRecordCount % valueHolders.size();
    return valueHolders.get(holderIndex);
  }

  @Override
  public Object getNextValue() {
    footerBuffer.add(getValueHolder());
    tempRecordCount++;
    if (footerBuffer.size() <= footerCount) {
      return null;
    }
    return footerBuffer.poll();
  }

  @Override
  public void reset() {
    super.reset();
    tempRecordCount = holderIndex + 1;
  }


  /**
   * Creates buffer of objects to be used as values, so these values can be re-used.
   * Objects number depends on number of lines to skip in the end of the file plus one object.
   *
   * @param reader          RecordReader to return value object
   * @param skipFooterLines number of lines to skip at the end of the file
   * @return list of objects to be used as values
   */
  private List<Object> initializeValueHolders(RecordReader reader, int skipFooterLines) {
    List<Object> valueHolder = new ArrayList<>(skipFooterLines + 1);
    for (int i = 0; i <= skipFooterLines; i++) {
      valueHolder.add(reader.createValue());
    }
    return valueHolder;
  }

}
