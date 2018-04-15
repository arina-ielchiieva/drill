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
package org.apache.drill.exec.store.parquet;

import com.google.common.base.Functions;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.common.Stopwatch;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.store.ColumnExplorer;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.parquet.columnreaders.ParquetRecordReader;
import org.apache.drill.exec.store.parquet2.DrillParquetReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class AbstractParquetScanBatchCreator {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractParquetScanBatchCreator.class);

  private static final String ENABLE_BYTES_READ_COUNTER = "parquet.benchmark.bytes.read";
  private static final String ENABLE_BYTES_TOTAL_COUNTER = "parquet.benchmark.bytes.total";
  private static final String ENABLE_TIME_READ_COUNTER = "parquet.benchmark.time.read";

  protected ScanBatch getBatch(ExecutorFragmentContext context, AbstractParquetRowGroupScan rowGroupScan, OperatorContext oContext) throws ExecutionSetupException {
    final ColumnExplorer columnExplorer = new ColumnExplorer(context.getOptions(), rowGroupScan.getColumns());

    if (!columnExplorer.isStarQuery()) {
      rowGroupScan = rowGroupScan.copy(columnExplorer.getTableColumns());
      rowGroupScan.setOperatorId(rowGroupScan.getOperatorId());
    }

    boolean useAsyncPageReader =
        context.getOptions().getOption(ExecConstants.PARQUET_PAGEREADER_ASYNC).bool_val;

    // keep footers in a map to avoid re-reading them
    Map<String, Pair<ParquetMetadata, DrillFileSystem>> footers = new HashMap<>();
    List<RecordReader> readers = new LinkedList<>();
    List<Map<String, String>> implicitColumns = new ArrayList<>();
    Map<String, String> mapWithMaxColumns = new LinkedHashMap<>();
    for(RowGroupReadEntry rowGroup : rowGroupScan.getRowGroupReadEntries()) {
      /*
      Here we could store a map from file names to footers, to prevent re-reading the footer for each row group in a file
      TODO - to prevent reading the footer again in the parquet record reader (it is read earlier in the ParquetStorageEngine)
      we should add more information to the RowGroupInfo that will be populated upon the first read to
      provide the reader with all of th file meta-data it needs
      These fields will be added to the constructor below
      */
      try {
        Stopwatch timer = Stopwatch.createUnstarted(logger.isDebugEnabled());
        if (!footers.containsKey(rowGroup.getPath())) {
          timer.start();
          DrillFileSystem fs = createDrillFileSystem(oContext, useAsyncPageReader, rowGroupScan.getFsConf(rowGroup));
          ParquetMetadata footer = readerFooter(fs.getConf(), rowGroup.getPath());
          long timeToRead = timer.elapsed(TimeUnit.MICROSECONDS);
          logger.trace("ParquetTrace,Read Footer,{},{},{},{},{},{},{}", "", rowGroup.getPath(), "", 0, 0, 0, timeToRead);
          footers.put(rowGroup.getPath(), new ImmutablePair<>(footer, fs));
        }
        Pair<ParquetMetadata, DrillFileSystem> pair = footers.get(rowGroup.getPath());
        ParquetMetadata footer = pair.getKey();
        DrillFileSystem fs = pair.getValue();

        boolean autoCorrectCorruptDates = rowGroupScan.areCorruptDatesAutoCorrected();
        ParquetReaderUtility.DateCorruptionStatus containsCorruptDates = ParquetReaderUtility.detectCorruptDates(footer, rowGroupScan.getColumns(),
            autoCorrectCorruptDates);
        logger.debug("Contains corrupt dates: {}", containsCorruptDates);

        if (!context.getOptions().getBoolean(ExecConstants.PARQUET_NEW_RECORD_READER) && !isComplex(footer)) {
          readers.add(new ParquetRecordReader(context,
              rowGroup.getPath(),
              rowGroup.getRowGroupIndex(),
              rowGroup.getNumRecordsToRead(),
              fs,
              CodecFactory.createDirectCodecFactory(fs.getConf(), new ParquetDirectByteBufferAllocator(oContext.getAllocator()), 0),
              footer,
              rowGroupScan.getColumns(),
              containsCorruptDates));
        } else {
          readers.add(new DrillParquetReader(context,
              footer,
              rowGroup,
              columnExplorer.getTableColumns(),
              fs,
              containsCorruptDates));
        }

        List<String> partitionValues = rowGroupScan.getPartitionValues(rowGroup);
        Map<String, String> implicitValues = columnExplorer.populateImplicitColumns(rowGroup.getPath(), partitionValues, rowGroupScan.supportsFileImplicitColumns());
        implicitColumns.add(implicitValues);
        if (implicitValues.size() > mapWithMaxColumns.size()) {
          mapWithMaxColumns = implicitValues;
        }

      } catch (IOException e1) {
        throw new ExecutionSetupException(e1);
      }
    }

    // all readers should have the same number of implicit columns, add missing ones with value null
    Map<String, String> diff = Maps.transformValues(mapWithMaxColumns, Functions.constant((String) null));
    for (Map<String, String> map : implicitColumns) {
      map.putAll(Maps.difference(map, diff).entriesOnlyOnRight());
    }

    return new ScanBatch(context, oContext, readers, implicitColumns);
  }

  private DrillFileSystem createDrillFileSystem(OperatorContext oContext, boolean useAsyncPageReader, Configuration conf) throws ExecutionSetupException {
    DrillFileSystem fs;
    try {
      if (useAsyncPageReader) {
        fs = oContext.newNonTrackingFileSystem(conf);
      } else {
        fs = oContext.newFileSystem(conf);
      }
    } catch (IOException e) {
      throw new ExecutionSetupException(String.format("Failed to create DrillFileSystem: %s", e.getMessage()), e);
    }
    return fs;
  }

  private ParquetMetadata readerFooter(Configuration conf, String path) throws IOException {
    Configuration newConf = new Configuration(conf);
    conf.setBoolean(ENABLE_BYTES_READ_COUNTER, false);
    conf.setBoolean(ENABLE_BYTES_TOTAL_COUNTER, false);
    conf.setBoolean(ENABLE_TIME_READ_COUNTER, false);
    return ParquetFileReader.readFooter(newConf, new Path(path), ParquetMetadataConverter.NO_FILTER);
  }

  private boolean isComplex(ParquetMetadata footer) {
    MessageType schema = footer.getFileMetaData().getSchema();

    for (Type type : schema.getFields()) {
      if (!type.isPrimitive()) {
        return true;
      }
    }
    for (ColumnDescriptor col : schema.getColumns()) {
      if (col.getMaxRepetitionLevel() > 0) {
        return true;
      }
    }
    return false;
  }

}
