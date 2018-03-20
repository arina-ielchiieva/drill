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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.ReadEntryWithPath;
import org.apache.drill.exec.store.hive.HiveMetadataProvider.LogicalInputSplit;
import org.apache.drill.exec.store.parquet.AbstractParquetGroupScan;
import org.apache.drill.exec.store.parquet.metadata.Metadata;
import org.apache.drill.exec.store.parquet.ParquetFormatConfig;
import org.apache.drill.exec.store.parquet.RowGroupInfo;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

@JsonTypeName("hive-drill-native-parquet-scan")
public class HiveDrillNativeParquetScan2 extends AbstractParquetGroupScan {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveDrillNativeParquetScan2.class);

  private final HiveStoragePlugin hiveStoragePlugin;

  @JsonCreator
  public HiveDrillNativeParquetScan2(@JacksonInject StoragePluginRegistry engineRegistry,
                           @JsonProperty("userName") String userName,
                           @JsonProperty("hiveStoragePluginConfig") HiveStoragePluginConfig hiveStoragePluginConfig,
                           @JsonProperty("entries") List<ReadEntryWithPath> entries,
                           @JsonProperty("columns") List<SchemaPath> columns,
                           @JsonProperty("filter") LogicalExpression filter) throws IOException, ExecutionSetupException {
    super(ImpersonationUtil.resolveUserName(userName), columns, entries, filter);
    this.hiveStoragePlugin = (HiveStoragePlugin) engineRegistry.getPlugin(hiveStoragePluginConfig);
    init();
  }

  public HiveDrillNativeParquetScan2(String userName,
                                     List<SchemaPath> columns,
                                     HiveReadEntry hiveReadEntry,
                                     HiveStoragePlugin hiveStoragePlugin,
                                     HiveMetadataProvider hiveMetadataProvider) throws IOException {
    this(userName, columns, hiveReadEntry, hiveStoragePlugin, hiveMetadataProvider, ValueExpressions.BooleanExpression.TRUE);
  }

  public HiveDrillNativeParquetScan2(String userName,
                                     List<SchemaPath> columns,
                                     HiveReadEntry hiveReadEntry,
                                     HiveStoragePlugin hiveStoragePlugin,
                                     HiveMetadataProvider hiveMetadataProvider,
                                     LogicalExpression filter) throws IOException {
    super(userName, columns, new ArrayList<>(), filter);

    this.hiveStoragePlugin = hiveStoragePlugin;

    if (hiveMetadataProvider == null) {
      hiveMetadataProvider = new HiveMetadataProvider(userName, hiveReadEntry, hiveStoragePlugin.getHiveConf());
    }

    // transform hiveReadEntry
    List<LogicalInputSplit> logicalInputSplits = hiveMetadataProvider.getInputSplits(hiveReadEntry);

    // logical input split contains list of splits by files
    // we need to read only one to get file path
    for (LogicalInputSplit logicalInputSplit : logicalInputSplits) {
      Iterator<InputSplit> iterator = logicalInputSplit.getInputSplits().iterator();
      if (iterator.hasNext()) {
        InputSplit split = iterator.next();
        final FileSplit fileSplit = (FileSplit) split; //todo
        final Path finalPath = fileSplit.getPath();
        entries.add(new ReadEntryWithPath(Path.getPathWithoutSchemeAndAuthority(finalPath).toString()));
      }
    }

    init();
  }

  private HiveDrillNativeParquetScan2(HiveDrillNativeParquetScan2 that) {
    super(that);
    this.hiveStoragePlugin = that.hiveStoragePlugin;
  }

  @JsonProperty
  public HiveStoragePluginConfig getHiveStoragePluginConfig() {
    return hiveStoragePlugin.getConfig();
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) {
    return new HiveDrillNativeParquetRowGroupScan(getUserName(), hiveStoragePlugin, getReadEntries(minorFragmentId), columns, filter);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new HiveDrillNativeParquetScan2(this);
  }

  @Override
  public HiveDrillNativeParquetScan2 clone(FileSelection selection) throws IOException {
    HiveDrillNativeParquetScan2 newScan = new HiveDrillNativeParquetScan2(this);
    newScan.modifyFileSelection(selection);
    newScan.init();
    return newScan;
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    HiveDrillNativeParquetScan2 newScan = new HiveDrillNativeParquetScan2(this);
    newScan.columns = columns;
    return newScan;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("HiveDrillNativeParquetScan [");
    builder.append("entries=").append(entries);
    builder.append(", numFiles=").append(getEntries().size());
    builder.append(", numRowGroups=").append(rowGroupInfos.size());

    String filterString = getFilterString();
    if (!filterString.isEmpty()) {
      builder.append(", filter=").append(filterString);
    }

    builder.append(", columns=").append(columns);
    builder.append("]");

    return builder.toString();
  }

  @Override
  protected void initInternal() throws IOException {
    //todo we need to get file system like in Hive with new ProjectionPusher().pushProjectionsAndFilters
    /*
          final JobConf cloneJob;
      try {
        cloneJob = new ProjectionPusher().pushProjectionsAndFilters(
            new JobConf(rowGroupScan.getHiveStoragePlugin().getHiveConf()), new Path(e.getPath()).getParent());
      } catch (IOException e1) {
        throw new RuntimeException(e1);
      }
     */

/*    Map<FileStatus, FileSystem> map = new HashMap<>();
    for (ReadEntryWithPath entry : entries) {
      Path path = new Path(entry.getPath());
      JobConf jobConf = new ProjectionPusher().pushProjectionsAndFilters(
          new JobConf(hiveStoragePlugin.getHiveConf()),
          path.getParent());
      FileSystem fs = FileSystem.get(jobConf);
      FileStatus fileStatus = fs.getFileStatus(Path.getPathWithoutSchemeAndAuthority(path));
      map.put(fileStatus, fs);
    }*/


    FileSystem fs = FileSystem.get(new JobConf(hiveStoragePlugin.getHiveConf()));
    ParquetFormatConfig formatConfig = new ParquetFormatConfig();
    List<FileStatus> fileStatuses = new ArrayList<>();
    for (ReadEntryWithPath entry : entries) {
      fileStatuses.add(fs.getFileStatus(Path.getPathWithoutSchemeAndAuthority(new Path(entry.getPath()))));
    }

    //todo for each file we should take separate file system, needs some re-writing
    //todo what if we skip projection pusher???
    parquetTableMetadata = Metadata.getParquetTableMetadata(fs, fileStatuses, formatConfig);
  }

  @Override
  protected Collection<CoordinationProtos.DrillbitEndpoint> getDrillbits() {
    return hiveStoragePlugin.getContext().getBits();
  }

  @Override
  protected String getSelectionRoot(RowGroupInfo rowGroup) {
    return rowGroup.getPath();
  }

  @Override
  protected AbstractParquetGroupScan cloneWithFileSelection(Collection<String> filePaths) throws IOException {
    FileSelection newSelection = new FileSelection(null, new ArrayList<>(filePaths), null, null, false);
    return clone(newSelection);
  }

}
