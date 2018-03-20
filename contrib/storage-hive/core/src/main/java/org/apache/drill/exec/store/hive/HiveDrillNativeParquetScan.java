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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.parquet.ProjectionPusher;

import java.util.List;

/**
 * Extension of {@link HiveScan} which support reading Hive tables using Drill's native parquet reader.
 */
@JsonTypeName("hive-drill-native-parquet-scan")
public abstract class HiveDrillNativeParquetScan extends HiveScan {
  public HiveDrillNativeParquetScan(@JsonProperty("userName") String userName, @JsonProperty("hiveReadEntry") HiveReadEntry hiveReadEntry, @JsonProperty("hiveStoragePluginConfig") HiveStoragePluginConfig hiveStoragePluginConfig, @JsonProperty("columns") List<SchemaPath> columns, @JacksonInject StoragePluginRegistry pluginRegistry) throws ExecutionSetupException {
    super(userName, hiveReadEntry, hiveStoragePluginConfig, columns, pluginRegistry);
  }
/*
  @JsonCreator
  public HiveDrillNativeParquetScan(@JsonProperty("userName") String userName,
                                    @JsonProperty("hiveReadEntry") HiveReadEntry hiveReadEntry,
                                    @JsonProperty("hiveStoragePluginConfig") HiveStoragePluginConfig hiveStoragePluginConfig,
                                    @JsonProperty("columns") List<SchemaPath> columns,
                                    @JacksonInject StoragePluginRegistry pluginRegistry) throws ExecutionSetupException {
    super(userName, hiveReadEntry, hiveStoragePluginConfig, columns, pluginRegistry);
    List<HiveMetadataProvider.LogicalInputSplit> inputSplits = getInputSplits();
    HiveMetadataProvider.LogicalInputSplit logicalInputSplit = inputSplits.get(0);
    Collection<InputSplit> inputSplits1 = logicalInputSplit.getInputSplits();
    // List<List<InputSplit> = // collect from LogicalInputSplits - needed when file is split into several parts,
    // not sure if this will be applicable for Drill native reader

  }

  *//*
  Implementation thoughts:
   1. Define files (not splits) - List<ReadEntryWithPath>
   2. Pass HiveStoragePluginConfig, userName, columns, pluginRegistry
   3. Check for what hiveReadEntry is used for and how this info can be transferred

based on ParquetGroupScan
    +  @JsonProperty("userName") String userName,
    --todo transform input splits into read entries (list of files?) --  @JsonProperty("entries") List<ReadEntryWithPath> entries,//
     -- we don't need this -- @JsonProperty("storage") StoragePluginConfig storageConfig, //
     -- HiveStoragePluginConfig -- @JsonProperty("format") FormatPluginConfig formatConfig, //
    +  @JacksonInject StoragePluginRegistry engineRegistry, //
   +   @JsonProperty("columns") List<SchemaPath> columns, //
    --todo Hive can have multiple different directories, need to adjust --  @JsonProperty("selectionRoot") String selectionRoot, //
    //todo can we add partition to the table outside of tables folder? -- need to check in Hive
     -- we don't need that -- @JsonProperty("cacheFileRoot") String cacheFileRoot, //
    +  @JsonProperty("filter") LogicalExpression filter

   *//*



      *//*
  rowGroupInfos +
  parquetTableMetadata +
  selectionRoot
  cacheFileRoot

      ParquetGroupScan clonegroupscan = this.clone(newSelection);
      clonegroupscan.rowGroupInfos = qualifiedRGs;
      clonegroupscan.updatePartitionColTypeMap();
   *//*

  String selectionRoot;
  String cacheFileRoot = null;

  Metadata.ParquetTableMetadata_v3 parquetTableMetadata;
  //List<ParquetGroupScan.RowGroupInfo> rowGroupInfos;

  public HiveDrillNativeParquetScan(String userName, HiveReadEntry hiveReadEntry, HiveStoragePlugin hiveStoragePlugin,
      List<SchemaPath> columns, HiveMetadataProvider metadataProvider) throws ExecutionSetupException {
    // table location - hiveReadEntry.table.sd.location

    super(userName, hiveReadEntry, hiveStoragePlugin, columns, metadataProvider);
    //todo difference between hive partitioned table that partitions can be in totally different location?
    this.selectionRoot = hiveReadEntry.getTable().getSd().getLocation();

    //todo try to get parquet metadata from footer
    HiveConf conf = getHiveConf();
    //JobConf cloneJob = new ProjectionPusher().pushProjectionsAndFilters(new JobConf(hiveConf), finalPath.getParent());

    List<HiveMetadataProvider.LogicalInputSplit> logicalInputSplits = getMetadataProvider().getInputSplits(hiveReadEntry);

    final Set<FileStatus> fileStatuses = new HashSet<>();
    FileSystem fs = null;

    for (HiveMetadataProvider.LogicalInputSplit logicalInputSplit : logicalInputSplits) {
      Collection<InputSplit> inputSplits = logicalInputSplit.getInputSplits();
      for (InputSplit split : inputSplits) {
        final FileSplit fileSplit = (FileSplit) split;
        final Path finalPath = fileSplit.getPath();
        final JobConf cloneJob;
        try {
          cloneJob = new ProjectionPusher().pushProjectionsAndFilters(new JobConf(conf), finalPath.getParent());
          if (fs == null) {
            fs = finalPath.getFileSystem(cloneJob);
          }
          ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(cloneJob, finalPath, ParquetMetadataConverter.NO_FILTER);
          List<FileStatus> fStatuses = DrillFileSystemUtil.listFiles(fs, Path.getPathWithoutSchemeAndAuthority(finalPath), true);
          fileStatuses.addAll(fStatuses);

        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    List<ReadEntryWithPath> entries = new ArrayList<>();
    for (FileStatus status: fileStatuses) {
      entries.add(new ReadEntryWithPath(status.getPath().toUri().getPath()));
    }

    try {
      //parquetTableMetadata = Metadata.getParquetTableMetadata(fs, new ArrayList<>(fileStatuses), new ParquetFormatConfig());


      Map<String, CoordinationProtos.DrillbitEndpoint> hostEndpointMap = new HashMap<>();
      for (CoordinationProtos.DrillbitEndpoint endpoint : hiveStoragePlugin.getContext().getBits()) {
        hostEndpointMap.put(endpoint.getAddress(), endpoint);
      }

*//*      rowGroupInfos = new ArrayList<>();
      for (Metadata.ParquetFileMetadata file : parquetTableMetadata.getFiles()) {
        int rgIndex = 0;
        for (Metadata.RowGroupMetadata rg : file.getRowGroups()) {
          ParquetGroupScan.RowGroupInfo rowGroupInfo = new ParquetGroupScan.RowGroupInfo(file.getPath(), rg.getStart(), rg.getLength(), rgIndex, rg.getRowCount());
          EndpointByteMap endpointByteMap = new EndpointByteMapImpl();
          for (String host : rg.getHostAffinity().keySet()) {
            if (hostEndpointMap.containsKey(host)) {
              endpointByteMap.add(hostEndpointMap.get(host), (long) (rg.getHostAffinity().get(host) * rg.getLength()));
            }
          }
          rowGroupInfo.setEndpointByteMap(endpointByteMap); //todo empty...
          rowGroupInfo.setColumns(rg.getColumns());
          rgIndex++;
          rowGroupInfos.add(rowGroupInfo);
        }
      }

      List<EndpointAffinity> affinityMap =
          AffinityCreator.getAffinityMap(rowGroupInfos); //todo empty

      System.out.println("ARINA");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }*//*

    *//*
            final List<FileStatus> fileStatuses = Lists.newArrayList();
        for (ReadEntryWithPath entry : entries) {
          fileStatuses.addAll(DrillFileSystemUtil.listFiles(fs, Path.getPathWithoutSchemeAndAuthority(new Path(entry.getPath())), true));
        }
        parquetTableMetadata = Metadata.getParquetTableMetadata(fs, fileStatuses, formatConfig);
     *//*

    *//*
      public static ParquetTableMetadata_v3 getParquetTableMetadata(FileSystem fs,
      List<FileStatus> fileStatuses, ParquetFormatConfig formatConfig) throws IOException {
    Metadata metadata = new Metadata(fs, formatConfig);
    return metadata.getParquetTableMetadata(fileStatuses);
  }
     *//*

  }

  public HiveDrillNativeParquetScan(final HiveScan hiveScan) {
    super(hiveScan);
  }

  @Override
  public ScanStats getScanStats() {
    final ScanStats nativeHiveScanStats = super.getScanStats();

    // As Drill's native parquet record reader is faster and memory efficient. Divide the CPU cost
    // by a factor to let the planner choose HiveDrillNativeScan over HiveScan with SerDes.
    return new ScanStats(
        nativeHiveScanStats.getGroupScanProperty(),
        nativeHiveScanStats.getRecordCount(),
        nativeHiveScanStats.getCpuCost()/getSerDeOverheadFactor(),
        nativeHiveScanStats.getDiskCost());
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
    try {
      return new HiveDrillNativeParquetSubScan((HiveSubScan)super.getSpecificScan(minorFragmentId));
    } catch (IOException | ReflectiveOperationException e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public boolean isNativeReader() {
    return true;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    return new HiveDrillNativeParquetScan(this);
  }

  @Override
  public HiveScan clone(HiveReadEntry hiveReadEntry) throws ExecutionSetupException {
    return new HiveDrillNativeParquetScan(getUserName(), hiveReadEntry, getStoragePlugin(), getColumns(), getMetadataProvider());
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    final HiveDrillNativeParquetScan scan = new HiveDrillNativeParquetScan(this);
    scan.columns = columns;
    return scan;
  }

  @Override
  public String toString() {
    final List<HivePartitionWrapper> partitions = getHiveReadEntry().getHivePartitionWrappers();
    int numPartitions = partitions == null ? 0 : partitions.size();
    return "HiveDrillNativeParquetScan [table=" + getHiveReadEntry().getHiveTableWrapper()
        + ", columns=" + getColumns()
        + ", numPartitions=" + numPartitions
        + ", partitions= " + partitions
        + ", inputDirectories=" + getMetadataProvider().getInputDirectories(getHiveReadEntry()) + "]";
  }

  @Override
  public LogicalExpression getFilter() {
    return ValueExpressions.BooleanExpression.TRUE;
  }

  @Override
  public GroupScan applyFilter(LogicalExpression filterExpr, UdfUtilities udfUtilities,
                               FunctionImplementationRegistry functionImplementationRegistry, OptionManager optionManager) {
    return null;

  *//*  if (rowGroupInfos.size() == 1 ||
        ! (parquetTableMetadata.isRowGroupPrunable()) ||
        rowGroupInfos.size() > optionManager.getOption(PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD)
        ) {
      // Stop pruning for 3 cases:
      //    -  1 single parquet file,
      //    -  metadata does not have proper format to support row group level filter pruning,
      //    -  # of row groups is beyond PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD.
      return null;
    }

    final Set<SchemaPath> schemaPathsInExpr = filterExpr.accept(new ParquetRGFilterEvaluator.FieldReferenceFinder(), null);

    final List<ParquetGroupScan.RowGroupInfo> qualifiedRGs = new ArrayList<>(rowGroupInfos.size());
    Set<String> qualifiedFileNames = new HashSet<>(); // HashSet keeps a fileName unique.

    ParquetFilterPredicate filterPredicate = null;

    for (ParquetGroupScan.RowGroupInfo rowGroup : rowGroupInfos) {
      final ColumnExplorer columnExplorer = new ColumnExplorer(optionManager, this.columns);
      Map<String, String> implicitColValues = columnExplorer.populateImplicitColumns(rowGroup.getPath(), selectionRoot);

      ParquetMetaStatCollector statCollector = new ParquetMetaStatCollector(
          parquetTableMetadata,
          rowGroup.getColumns(),
          implicitColValues);

      Map<SchemaPath, ColumnStatistics> columnStatisticsMap = statCollector.collectColStat(schemaPathsInExpr);

      if (filterPredicate == null) {
        ErrorCollector errorCollector = new ErrorCollectorImpl();
        LogicalExpression materializedFilter = ExpressionTreeMaterializer.materializeFilterExpr(
            filterExpr, columnStatisticsMap, errorCollector, functionImplementationRegistry);

        if (errorCollector.hasErrors()) {
          //logger.error("{} error(s) encountered when materialize filter expression : {}", errorCollector.getErrorCount(), errorCollector.toErrorString());
          return null;
        }
        //    logger.debug("materializedFilter : {}", ExpressionStringBuilder.toString(materializedFilter));

        Set<LogicalExpression> constantBoundaries = ConstantExpressionIdentifier.getConstantExpressionSet(materializedFilter);
        filterPredicate = (ParquetFilterPredicate) ParquetFilterBuilder.buildParquetFilterPredicate(
            materializedFilter, constantBoundaries, udfUtilities);

        if (filterPredicate == null) {
          return null;
        }
      }

      if (ParquetRGFilterEvaluator.canDrop(filterPredicate, columnStatisticsMap, rowGroup.getRowCount())) {
        continue;
      }

      qualifiedRGs.add(rowGroup);
      qualifiedFileNames.add(rowGroup.getPath());  // TODO : optimize when 1 file contains m row groups.
    }


    if (qualifiedRGs.size() == rowGroupInfos.size() ) {
      // There is no reduction of rowGroups. Return the original groupScan.
      //logger.debug("applyFilter does not have any pruning!");
      return null;
    } else if (qualifiedFileNames.size() == 0) {
      //logger.warn("All rowgroups have been filtered out. Add back one to get schema from scannner");
      ParquetGroupScan.RowGroupInfo rg = rowGroupInfos.iterator().next();
      qualifiedFileNames.add(rg.getPath());
      qualifiedRGs.add(rg);
    }

    try {
      //FileSelection newSelection = new FileSelection(null, Lists.newArrayList(qualifiedFileNames), getSelectionRoot(), cacheFileRoot, false);
      //logger.info("applyFilter {} reduce parquet rowgroup # from {} to {}", ExpressionStringBuilder.toString(filterExpr), rowGroupInfos.size(), qualifiedRGs.size());
      //todoParquetGroupScan clonegroupscan = this.clone(newSelection);
      //clonegroupscan.rowGroupInfos = qualifiedRGs;
      //clonegroupscan.updatePartitionColTypeMap();
      //return clonegroupscan;
      return null; //todo

    } catch (Exception e) {
      //logger.warn("Could not apply filter prune due to Exception : {}", e);
      return null;
    }*//*
  }*/
}
