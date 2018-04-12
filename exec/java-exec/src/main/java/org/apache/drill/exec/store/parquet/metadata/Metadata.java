/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.parquet.metadata;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.Stopwatch;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.util.DrillVersionInfo;
import org.apache.drill.exec.store.TimedRunnable;
import org.apache.drill.exec.store.parquet.ParquetFormatConfig;
import org.apache.drill.exec.store.parquet.ParquetReaderUtility;
import org.apache.drill.exec.util.DrillFileSystemUtil;
import org.apache.drill.exec.store.dfs.MetadataContext;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.commons.lang3.tuple.Pair;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import javax.annotation.Nullable;

import static org.apache.drill.exec.store.parquet.metadata.MetadataVersion.Constants.SUPPORTED_VERSIONS;

public class Metadata {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Metadata.class);

  public static final String[] OLD_METADATA_FILENAMES = {".drill.parquet_metadata.v2"};
  public static final String METADATA_FILENAME = ".drill.parquet_metadata";
  public static final String METADATA_DIRECTORIES_FILENAME = ".drill.parquet_metadata_directories";

  private final FileSystem fs;
  private final ParquetFormatConfig formatConfig;

  private MetadataBase.ParquetTableMetadataBase parquetTableMetadata;
  private ParquetTableMetadataDirs parquetTableMetadataDirs;

  /**
   * Create the parquet metadata file for the directory at the given path, and for any subdirectories.
   *
   * @param fs file system
   * @param path path
   */
  public static void createMeta(FileSystem fs, String path, ParquetFormatConfig formatConfig) throws IOException {
    Metadata metadata = new Metadata(fs, formatConfig);
    metadata.createMetaFilesRecursively(path);
  }

  /**
   * Get the parquet metadata for the parquet files in the given directory, including those in subdirectories.
   *
   * @param fs file system
   * @param path path
   * @return parquet table metadata
   */
  public static Metadata_V3.ParquetTableMetadata_v3 getParquetTableMetadata(FileSystem fs, String path, ParquetFormatConfig formatConfig)
      throws IOException {
    Metadata metadata = new Metadata(fs, formatConfig);
    return metadata.getParquetTableMetadata(path);
  }

  /**
   * Get the parquet metadata for a list of parquet files.
   *
   * @param fs file system
   * @param fileStatuses file statuses
   * @return parquet table metadata
   */
  public static Metadata_V3.ParquetTableMetadata_v3 getParquetTableMetadata(FileSystem fs,
                                                                            List<FileStatus> fileStatuses, ParquetFormatConfig formatConfig) throws IOException {
    Metadata metadata = new Metadata(fs, formatConfig);
    return metadata.getParquetTableMetadata(fileStatuses);
  }

  /**
   * Get the parquet metadata for the table by reading the metadata file
   *
   * @param fs current file system
   * @param path The path to the metadata file, located in the directory that contains the parquet files
   * @param metaContext metadata context
   * @param formatConfig parquet format plugin configs
   * @return parquet table metadata. Null if metadata cache is missing, unsupported or corrupted
   */
  public static @Nullable
  MetadataBase.ParquetTableMetadataBase readBlockMeta(FileSystem fs, Path path, MetadataContext metaContext,
                                                      ParquetFormatConfig formatConfig) {
    if (ignoreReadingMetadata(metaContext, path)) {
      return null;
    }
    Metadata metadata = new Metadata(fs, formatConfig);
    metadata.readBlockMeta(path, false, metaContext);
    return metadata.parquetTableMetadata;
  }

  /**
   * Get the parquet metadata for all subdirectories by reading the metadata file
   *
   * @param fs current file system
   * @param path The path to the metadata file, located in the directory that contains the parquet files
   * @param metaContext metadata context
   * @param formatConfig parquet format plugin configs
   * @return parquet metadata for a directory. Null if metadata cache is missing, unsupported or corrupted
   */
  public static @Nullable ParquetTableMetadataDirs readMetadataDirs(FileSystem fs, Path path,
      MetadataContext metaContext, ParquetFormatConfig formatConfig) {
    if (ignoreReadingMetadata(metaContext, path)) {
      return null;
    }
    Metadata metadata = new Metadata(fs, formatConfig);
    metadata.readBlockMeta(path, true, metaContext);
    return metadata.parquetTableMetadataDirs;
  }

  /**
   * Ignore reading metadata files, if metadata is missing, unsupported or corrupted
   *
   * @param metaContext Metadata context
   * @param path The path to the metadata file, located in the directory that contains the parquet files
   * @return true if parquet metadata is missing or corrupted, false otherwise
   */
  private static boolean ignoreReadingMetadata(MetadataContext metaContext, Path path) {
    if (metaContext.isMetadataCacheCorrupted()) {
      logger.warn("Ignoring of reading '{}' metadata file. Parquet metadata cache files are unsupported or corrupted. " +
          "Query performance may be slow. Make sure the cache files are up-to-date by running the 'REFRESH TABLE " +
          "METADATA' command", path);
      return true;
    }
    return false;
  }

  private Metadata(FileSystem fs, ParquetFormatConfig formatConfig) {
    this.fs = ImpersonationUtil.createFileSystem(ImpersonationUtil.getProcessUserName(), fs.getConf());
    this.formatConfig = formatConfig;
  }

  /**
   * Create the parquet metadata files for the directory at the given path and for any subdirectories.
   * Metadata cache files written to the disk contain relative paths. Returned Pair of metadata contains absolute paths.
   *
   * @param path to the directory of the parquet table
   * @return Pair of parquet metadata. The left one is a parquet metadata for the table. The right one of the Pair is
   *         a metadata for all subdirectories (if they are present and there are no any parquet files in the
   *         {@code path} directory).
   * @throws IOException if parquet metadata can't be serialized and written to the json file
   */
  private Pair<Metadata_V3.ParquetTableMetadata_v3, ParquetTableMetadataDirs> createMetaFilesRecursively(final String path) throws IOException {
    Stopwatch timer = Stopwatch.createStarted(logger.isDebugEnabled());
    List<Metadata_V3.ParquetFileMetadata_v3> metaDataList = Lists.newArrayList();
    List<String> directoryList = Lists.newArrayList();
    ConcurrentHashMap<Metadata_V3.ColumnTypeMetadata_v3.Key, Metadata_V3.ColumnTypeMetadata_v3> columnTypeInfoSet =
        new ConcurrentHashMap<>();
    Path p = new Path(path);
    FileStatus fileStatus = fs.getFileStatus(p);
    assert fileStatus.isDirectory() : "Expected directory";

    final List<FileStatus> childFiles = Lists.newArrayList();

    for (final FileStatus file : DrillFileSystemUtil.listAll(fs, p, false)) {
      if (file.isDirectory()) {
        Metadata_V3.ParquetTableMetadata_v3 subTableMetadata = (createMetaFilesRecursively(file.getPath().toString())).getLeft();
        metaDataList.addAll(subTableMetadata.files);
        directoryList.addAll(subTableMetadata.directories);
        directoryList.add(file.getPath().toString());
        // Merge the schema from the child level into the current level
        //TODO: We need a merge method that merges two columns with the same name but different types
        columnTypeInfoSet.putAll(subTableMetadata.columnTypeInfo);
      } else {
        childFiles.add(file);
      }
    }
    Metadata_V3.ParquetTableMetadata_v3 parquetTableMetadata = new Metadata_V3.ParquetTableMetadata_v3(SUPPORTED_VERSIONS.last().toString(),
                                                                                DrillVersionInfo.getVersion());
    if (childFiles.size() > 0) {
      List<Metadata_V3.ParquetFileMetadata_v3> childFilesMetadata =
          getParquetFileMetadata_v3(parquetTableMetadata, childFiles);
      metaDataList.addAll(childFilesMetadata);
      // Note that we do not need to merge the columnInfo at this point. The columnInfo is already added
      // to the parquetTableMetadata.
    }

    parquetTableMetadata.directories = directoryList;
    parquetTableMetadata.files = metaDataList;
    // TODO: We need a merge method that merges two columns with the same name but different types
    if (parquetTableMetadata.columnTypeInfo == null) {
      parquetTableMetadata.columnTypeInfo = new ConcurrentHashMap<>();
    }
    parquetTableMetadata.columnTypeInfo.putAll(columnTypeInfoSet);

    for (String oldName : OLD_METADATA_FILENAMES) {
      fs.delete(new Path(p, oldName), false);
    }
    //  relative paths in the metadata are only necessary for meta cache files.
    Metadata_V3.ParquetTableMetadata_v3 metadataTableWithRelativePaths =
        MetadataPathUtils.createMetadataWithRelativePaths(parquetTableMetadata, path);
    writeFile(metadataTableWithRelativePaths, new Path(p, METADATA_FILENAME));

    if (directoryList.size() > 0 && childFiles.size() == 0) {
      ParquetTableMetadataDirs parquetTableMetadataDirsRelativePaths =
          new ParquetTableMetadataDirs(metadataTableWithRelativePaths.directories);
      writeFile(parquetTableMetadataDirsRelativePaths, new Path(p, METADATA_DIRECTORIES_FILENAME));
      logger.debug("Creating metadata files recursively took {} ms", timer.elapsed(TimeUnit.MILLISECONDS));
      ParquetTableMetadataDirs parquetTableMetadataDirs = new ParquetTableMetadataDirs(directoryList);
      return Pair.of(parquetTableMetadata, parquetTableMetadataDirs);
    }
    List<String> emptyDirList = Lists.newArrayList();
    logger.debug("Creating metadata files recursively took {} ms", timer.elapsed(TimeUnit.MILLISECONDS));
    timer.stop();
    return Pair.of(parquetTableMetadata, new ParquetTableMetadataDirs(emptyDirList));
  }

  /**
   * Get the parquet metadata for the parquet files in a directory.
   *
   * @param path the path of the directory
   * @return metadata object for an entire parquet directory structure
   * @throws IOException in case of problems during accessing files
   */
  private Metadata_V3.ParquetTableMetadata_v3 getParquetTableMetadata(String path) throws IOException {
    Path p = new Path(path);
    FileStatus fileStatus = fs.getFileStatus(p);
    final Stopwatch watch = Stopwatch.createStarted(logger.isDebugEnabled());
    List<FileStatus> fileStatuses = new ArrayList<>();
    if (fileStatus.isFile()) {
      fileStatuses.add(fileStatus);
    } else {
      fileStatuses.addAll(DrillFileSystemUtil.listFiles(fs, p, true));
    }
    logger.debug("Took {} ms to get file statuses", watch.elapsed(TimeUnit.MILLISECONDS));
    watch.reset();
    watch.start();
    Metadata_V3.ParquetTableMetadata_v3 metadata_v3 = getParquetTableMetadata(fileStatuses);
    logger.debug("Took {} ms to read file metadata", watch.elapsed(TimeUnit.MILLISECONDS));
    return metadata_v3;
  }

  /**
   * Get the parquet metadata for a list of parquet files
   *
   * @param fileStatuses List of file statuses
   * @return parquet table metadata object
   * @throws IOException if parquet file metadata can't be obtained
   */
  private Metadata_V3.ParquetTableMetadata_v3 getParquetTableMetadata(List<FileStatus> fileStatuses)
      throws IOException {
    Metadata_V3.ParquetTableMetadata_v3 tableMetadata = new Metadata_V3.ParquetTableMetadata_v3(SUPPORTED_VERSIONS.last().toString(),
                                                                        DrillVersionInfo.getVersion());
    tableMetadata.files = getParquetFileMetadata_v3(tableMetadata, fileStatuses);
    tableMetadata.directories = new ArrayList<>();
    return tableMetadata;
  }

  /**
   * Get a list of file metadata for a list of parquet files
   *
   * @param parquetTableMetadata_v3 can store column schema info from all the files and row groups
   * @param fileStatuses list of the parquet files statuses
   *
   * @return list of the parquet file metadata with absolute paths
   * @throws IOException is thrown in case of issues while executing the list of runnables
   */
  private List<Metadata_V3.ParquetFileMetadata_v3> getParquetFileMetadata_v3(
    Metadata_V3.ParquetTableMetadata_v3 parquetTableMetadata_v3, List<FileStatus> fileStatuses) throws IOException {
    List<TimedRunnable<Metadata_V3.ParquetFileMetadata_v3>> gatherers = Lists.newArrayList();
    for (FileStatus file : fileStatuses) {
      gatherers.add(new MetadataGatherer(parquetTableMetadata_v3, file));
    }

    List<Metadata_V3.ParquetFileMetadata_v3> metaDataList = Lists.newArrayList();
    metaDataList.addAll(TimedRunnable.run("Fetch parquet metadata", logger, gatherers, 16));
    return metaDataList;
  }

  /**
   * TimedRunnable that reads the footer from parquet and collects file metadata
   */
  private class MetadataGatherer extends TimedRunnable<Metadata_V3.ParquetFileMetadata_v3> {

    private FileStatus fileStatus;
    private Metadata_V3.ParquetTableMetadata_v3 parquetTableMetadata;

    MetadataGatherer(Metadata_V3.ParquetTableMetadata_v3 parquetTableMetadata, FileStatus fileStatus) {
      this.fileStatus = fileStatus;
      this.parquetTableMetadata = parquetTableMetadata;
    }

    @Override
    protected Metadata_V3.ParquetFileMetadata_v3 runInner() throws Exception {
      return getParquetFileMetadata_v3(parquetTableMetadata, fileStatus);
    }

    @Override
    protected IOException convertToIOException(Exception e) {
      if (e instanceof IOException) {
        return (IOException) e;
      } else {
        return new IOException(e);
      }
    }
  }

  private OriginalType getOriginalType(Type type, String[] path, int depth) {
    if (type.isPrimitive()) {
      return type.getOriginalType();
    }
    Type t = ((GroupType) type).getType(path[depth]);
    return getOriginalType(t, path, depth + 1);
  }

  private ColTypeInfo getColTypeInfo(MessageType schema, Type type, String[] path, int depth) {
    if (type.isPrimitive()) {
      PrimitiveType primitiveType = (PrimitiveType) type;
      int precision = 0;
      int scale = 0;
      if (primitiveType.getDecimalMetadata() != null) {
        precision = primitiveType.getDecimalMetadata().getPrecision();
        scale = primitiveType.getDecimalMetadata().getScale();
      }

      int repetitionLevel = schema.getMaxRepetitionLevel(path);
      int definitionLevel = schema.getMaxDefinitionLevel(path);

      return new ColTypeInfo(type.getOriginalType(), precision, scale, repetitionLevel, definitionLevel);
    }
    Type t = ((GroupType) type).getType(path[depth]);
    return getColTypeInfo(schema, t, path, depth + 1);
  }

  private class ColTypeInfo {
    public OriginalType originalType;
    public int precision;
    public int scale;
    public int repetitionLevel;
    public int definitionLevel;

    ColTypeInfo(OriginalType originalType, int precision, int scale, int repetitionLevel, int definitionLevel) {
      this.originalType = originalType;
      this.precision = precision;
      this.scale = scale;
      this.repetitionLevel = repetitionLevel;
      this.definitionLevel = definitionLevel;
    }
  }

  /**
   * Get the metadata for a single file
   */
  private Metadata_V3.ParquetFileMetadata_v3 getParquetFileMetadata_v3(Metadata_V3.ParquetTableMetadata_v3 parquetTableMetadata,
                                                                       final FileStatus file) throws IOException, InterruptedException {
    final ParquetMetadata metadata;
    final UserGroupInformation processUserUgi = ImpersonationUtil.getProcessUserUGI();
    try {
      metadata = processUserUgi.doAs((PrivilegedExceptionAction<ParquetMetadata>) () ->
        ParquetFileReader.readFooter(fs.getConf(), file, ParquetMetadataConverter.NO_FILTER));
    } catch(Exception e) {
      logger.error("Exception while reading footer of parquet file [Details - path: {}, owner: {}] as process user {}",
        file.getPath(), file.getOwner(), processUserUgi.getShortUserName(), e);
      throw e;
    }

    MessageType schema = metadata.getFileMetaData().getSchema();

//    Map<SchemaPath, OriginalType> originalTypeMap = Maps.newHashMap();
    Map<SchemaPath, ColTypeInfo> colTypeInfoMap = Maps.newHashMap();
    schema.getPaths();
    for (String[] path : schema.getPaths()) {
      colTypeInfoMap.put(SchemaPath.getCompoundPath(path), getColTypeInfo(schema, schema, path, 0));
    }

    List<Metadata_V3.RowGroupMetadata_v3> rowGroupMetadataList = Lists.newArrayList();

    ArrayList<SchemaPath> ALL_COLS = new ArrayList<>();
    ALL_COLS.add(SchemaPath.STAR_COLUMN);
    boolean autoCorrectCorruptDates = formatConfig.areCorruptDatesAutoCorrected();
    ParquetReaderUtility.DateCorruptionStatus containsCorruptDates = ParquetReaderUtility.detectCorruptDates(metadata, ALL_COLS, autoCorrectCorruptDates);
    if (logger.isDebugEnabled()) {
      logger.debug(containsCorruptDates.toString());
    }
    for (BlockMetaData rowGroup : metadata.getBlocks()) {
      List<Metadata_V3.ColumnMetadata_v3> columnMetadataList = Lists.newArrayList();
      long length = 0;
      for (ColumnChunkMetaData col : rowGroup.getColumns()) {
        Metadata_V3.ColumnMetadata_v3 columnMetadata;

        boolean statsAvailable = (col.getStatistics() != null && !col.getStatistics().isEmpty());

        Statistics<?> stats = col.getStatistics();
        String[] columnName = col.getPath().toArray();
        SchemaPath columnSchemaName = SchemaPath.getCompoundPath(columnName);
        ColTypeInfo colTypeInfo = colTypeInfoMap.get(columnSchemaName);

        Metadata_V3.ColumnTypeMetadata_v3 columnTypeMetadata =
            new Metadata_V3.ColumnTypeMetadata_v3(columnName, col.getType(), colTypeInfo.originalType,
                colTypeInfo.precision, colTypeInfo.scale, colTypeInfo.repetitionLevel, colTypeInfo.definitionLevel);

        if (parquetTableMetadata.columnTypeInfo == null) {
          parquetTableMetadata.columnTypeInfo = new ConcurrentHashMap<>();
        }
        // Save the column schema info. We'll merge it into one list
        parquetTableMetadata.columnTypeInfo
            .put(new Metadata_V3.ColumnTypeMetadata_v3.Key(columnTypeMetadata.name), columnTypeMetadata);
        if (statsAvailable) {
          // Write stats when they are not null
          Object minValue = null;
          Object maxValue = null;
          if (stats.genericGetMax() != null && stats.genericGetMin() != null ) {
            minValue = stats.genericGetMin();
            maxValue = stats.genericGetMax();
            if (containsCorruptDates == ParquetReaderUtility.DateCorruptionStatus.META_SHOWS_CORRUPTION
                && columnTypeMetadata.originalType == OriginalType.DATE) {
              minValue = ParquetReaderUtility.autoCorrectCorruptedDate((Integer) minValue);
              maxValue = ParquetReaderUtility.autoCorrectCorruptedDate((Integer) maxValue);
            }

          }
          columnMetadata =
              new Metadata_V3.ColumnMetadata_v3(columnTypeMetadata.name, col.getType(), minValue, maxValue, stats.getNumNulls());
        } else {
          columnMetadata = new Metadata_V3.ColumnMetadata_v3(columnTypeMetadata.name, col.getType(), null, null, null);
        }
        columnMetadataList.add(columnMetadata);
        length += col.getTotalSize();
      }

      // DRILL-5009: Skip the RowGroup if it is empty
      // Note we still read the schema even if there are no values in the RowGroup
      if (rowGroup.getRowCount() == 0) {
        continue;
      }
      Metadata_V3.RowGroupMetadata_v3 rowGroupMeta =
          new Metadata_V3.RowGroupMetadata_v3(rowGroup.getStartingPos(), length, rowGroup.getRowCount(),
              getHostAffinity(file, rowGroup.getStartingPos(), length), columnMetadataList);

      rowGroupMetadataList.add(rowGroupMeta);
    }
    String path = Path.getPathWithoutSchemeAndAuthority(file.getPath()).toString();

    return new Metadata_V3.ParquetFileMetadata_v3(path, file.getLen(), rowGroupMetadataList);
  }

  /**
   * Get the host affinity for a row group.
   *
   * @param fileStatus the parquet file
   * @param start      the start of the row group
   * @param length     the length of the row group
   * @return host affinity for the row group
   */
  private Map<String, Float> getHostAffinity(FileStatus fileStatus, long start, long length)
      throws IOException {
    BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, start, length);
    Map<String, Float> hostAffinityMap = Maps.newHashMap();
    for (BlockLocation blockLocation : blockLocations) {
      for (String host : blockLocation.getHosts()) {
        Float currentAffinity = hostAffinityMap.get(host);
        float blockStart = blockLocation.getOffset();
        float blockEnd = blockStart + blockLocation.getLength();
        float rowGroupEnd = start + length;
        Float newAffinity = (blockLocation.getLength() - (blockStart < start ? start - blockStart : 0) -
            (blockEnd > rowGroupEnd ? blockEnd - rowGroupEnd : 0)) / length;
        if (currentAffinity != null) {
          hostAffinityMap.put(host, currentAffinity + newAffinity);
        } else {
          hostAffinityMap.put(host, newAffinity);
        }
      }
    }
    return hostAffinityMap;
  }

  /**
   * Serialize parquet metadata to json and write to a file.
   *
   * @param parquetTableMetadata parquet table metadata
   * @param p file path
   */
  private void writeFile(Metadata_V3.ParquetTableMetadata_v3 parquetTableMetadata, Path p) throws IOException {
    JsonFactory jsonFactory = new JsonFactory();
    jsonFactory.configure(Feature.AUTO_CLOSE_TARGET, false);
    jsonFactory.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
    ObjectMapper mapper = new ObjectMapper(jsonFactory);
    SimpleModule module = new SimpleModule();
    module.addSerializer(Metadata_V3.ColumnMetadata_v3.class, new Metadata_V3.ColumnMetadata_v3.Serializer());
    mapper.registerModule(module);
    FSDataOutputStream os = fs.create(p);
    mapper.writerWithDefaultPrettyPrinter().writeValue(os, parquetTableMetadata);
    os.flush();
    os.close();
  }

  private void writeFile(ParquetTableMetadataDirs parquetTableMetadataDirs, Path p) throws IOException {
    JsonFactory jsonFactory = new JsonFactory();
    jsonFactory.configure(Feature.AUTO_CLOSE_TARGET, false);
    jsonFactory.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
    ObjectMapper mapper = new ObjectMapper(jsonFactory);
    SimpleModule module = new SimpleModule();
    mapper.registerModule(module);
    FSDataOutputStream os = fs.create(p);
    mapper.writerWithDefaultPrettyPrinter().writeValue(os, parquetTableMetadataDirs);
    os.flush();
    os.close();
  }

  /**
   * Read the parquet metadata from a file
   *
   * @param path to metadata file
   * @param dirsOnly true for {@link Metadata#METADATA_DIRECTORIES_FILENAME}
   *                 or false for {@link Metadata#METADATA_FILENAME} files reading
   * @param metaContext current metadata context
   */
  private void readBlockMeta(Path path, boolean dirsOnly, MetadataContext metaContext) {
    Stopwatch timer = Stopwatch.createStarted(logger.isDebugEnabled());
    Path metadataParentDir = Path.getPathWithoutSchemeAndAuthority(path.getParent());
    String metadataParentDirPath = metadataParentDir.toUri().getPath();
    ObjectMapper mapper = new ObjectMapper();

    final SimpleModule serialModule = new SimpleModule();
    serialModule.addDeserializer(SchemaPath.class, new SchemaPath.De());
    serialModule.addKeyDeserializer(Metadata_V2.ColumnTypeMetadata_v2.Key.class, new Metadata_V2.ColumnTypeMetadata_v2.Key.DeSerializer());
    serialModule.addKeyDeserializer(Metadata_V3.ColumnTypeMetadata_v3.Key.class, new Metadata_V3.ColumnTypeMetadata_v3.Key.DeSerializer());

    AfterburnerModule module = new AfterburnerModule();
    module.setUseOptimizedBeanDeserializer(true);

    mapper.registerModule(serialModule);
    mapper.registerModule(module);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    try (FSDataInputStream is = fs.open(path)) {
      boolean alreadyCheckedModification;
      boolean newMetadata = false;
      alreadyCheckedModification = metaContext.getStatus(metadataParentDirPath);

      if (dirsOnly) {
        parquetTableMetadataDirs = mapper.readValue(is, ParquetTableMetadataDirs.class);
        logger.debug("Took {} ms to read directories from directory cache file", timer.elapsed(TimeUnit.MILLISECONDS));
        timer.stop();
        parquetTableMetadataDirs.updateRelativePaths(metadataParentDirPath);
        if (!alreadyCheckedModification && tableModified(parquetTableMetadataDirs.getDirectories(), path, metadataParentDir, metaContext)) {
          parquetTableMetadataDirs =
              (createMetaFilesRecursively(Path.getPathWithoutSchemeAndAuthority(path.getParent()).toString())).getRight();
          newMetadata = true;
        }
      } else {
        parquetTableMetadata = mapper.readValue(is, MetadataBase.ParquetTableMetadataBase.class);
        logger.debug("Took {} ms to read metadata from cache file", timer.elapsed(TimeUnit.MILLISECONDS));
        timer.stop();
        if (new MetadataVersion(parquetTableMetadata.getMetadataVersion()).compareTo(new MetadataVersion(3, 0)) >= 0) {
          ((Metadata_V3.ParquetTableMetadata_v3) parquetTableMetadata).updateRelativePaths(metadataParentDirPath);
        }
        if (!alreadyCheckedModification && tableModified(parquetTableMetadata.getDirectories(), path, metadataParentDir, metaContext)) {
          parquetTableMetadata =
              (createMetaFilesRecursively(Path.getPathWithoutSchemeAndAuthority(path.getParent()).toString())).getLeft();
          newMetadata = true;
        }

        // DRILL-5009: Remove the RowGroup if it is empty
        List<? extends MetadataBase.ParquetFileMetadata> files = parquetTableMetadata.getFiles();
        for (MetadataBase.ParquetFileMetadata file : files) {
          List<? extends MetadataBase.RowGroupMetadata> rowGroups = file.getRowGroups();
          rowGroups.removeIf(r -> r.getRowCount() == 0);
        }

      }
      if (newMetadata) {
        // if new metadata files were created, invalidate the existing metadata context
        metaContext.clear();
      }
    } catch (IOException e) {
      logger.error("Failed to read '{}' metadata file", path, e);
      metaContext.setMetadataCacheCorrupted(true);
    }
  }

  /**
   * Check if the parquet metadata needs to be updated by comparing the modification time of the directories with
   * the modification time of the metadata file
   *
   * @param directories List of directories
   * @param metaFilePath path of parquet metadata cache file
   * @return true if metadata needs to be updated, false otherwise
   * @throws IOException if some resources are not accessible
   */
  private boolean tableModified(List<String> directories, Path metaFilePath, Path parentDir, MetadataContext metaContext)
      throws IOException {

    Stopwatch timer = Stopwatch.createStarted(logger.isDebugEnabled());

    metaContext.setStatus(parentDir.toUri().getPath());
    long metaFileModifyTime = fs.getFileStatus(metaFilePath).getModificationTime();
    FileStatus directoryStatus = fs.getFileStatus(parentDir);
    int numDirs = 1;
    if (directoryStatus.getModificationTime() > metaFileModifyTime) {
      logger.debug("Directory {} was modified. Took {} ms to check modification time of {} directories", directoryStatus.getPath().toString(),
          timer.elapsed(TimeUnit.MILLISECONDS),
          numDirs);
      timer.stop();
      return true;
    }
    for (String directory : directories) {
      numDirs++;
      metaContext.setStatus(directory);
      directoryStatus = fs.getFileStatus(new Path(directory));
      if (directoryStatus.getModificationTime() > metaFileModifyTime) {
        logger.debug("Directory {} was modified. Took {} ms to check modification time of {} directories", directoryStatus.getPath().toString(),
            timer.elapsed(TimeUnit.MILLISECONDS),
            numDirs);
        timer.stop();
        return true;
      }
    }
    logger.debug("No directories were modified. Took {} ms to check modification time of {} directories", timer.elapsed(TimeUnit.MILLISECONDS), numDirs);
    timer.stop();
    return false;
  }

}

