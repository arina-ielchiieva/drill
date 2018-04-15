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
package org.apache.drill.exec.store.hive;

import com.google.common.base.Preconditions;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.parquet.AbstractParquetScanBatchCreator;
import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveDrillNativeParquetScanBatchCreator extends AbstractParquetScanBatchCreator implements BatchCreator<HiveDrillNativeParquetRowGroupScan> {

  @Override
  public CloseableRecordBatch getBatch(ExecutorFragmentContext context, HiveDrillNativeParquetRowGroupScan rowGroupScan, List<RecordBatch> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    OperatorContext oContext = context.newOperatorContext(rowGroupScan);
    return getBatch(context, rowGroupScan, oContext);
  }

  @Override
  protected AbstractDrillFileSystemCreator getDrillFileSystemCreator(OperatorContext operatorContext, boolean useAsyncPageReader) {
    return new HiveDrillNativeParquetDrillFileSystemCreator(operatorContext, useAsyncPageReader);
  }

  private class HiveDrillNativeParquetDrillFileSystemCreator extends AbstractDrillFileSystemCreator {

    private Map<String, DrillFileSystem> fileSystems;

    HiveDrillNativeParquetDrillFileSystemCreator(OperatorContext operatorContext, boolean useAsyncPageReader) {
      super(operatorContext, useAsyncPageReader);
      this.fileSystems = new HashMap<>();
    }

    @Override
    protected DrillFileSystem getDrillFileSystem(Configuration config, String path) throws ExecutionSetupException {
      DrillFileSystem fs = fileSystems.get(path);
      if (fs == null) {
        fs = createDrillFileSystem(config);
        fileSystems.put(path, fs);
      }
      return fs;
    }
  }

}
