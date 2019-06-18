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
package org.apache.drill.metastore.iceberg;

import com.typesafe.config.Config;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.metastore.Metastore;
import org.apache.drill.metastore.MetadataUnit;
import org.apache.drill.metastore.iceberg.config.IcebergConfigConstants;
import org.apache.drill.metastore.iceberg.exceptions.IcebergMetastoreException;
import org.apache.drill.metastore.iceberg.metadata.IcebergMetadata;
import org.apache.drill.metastore.iceberg.metadata.IcebergTableSchema;
import org.apache.drill.metastore.iceberg.operate.IcebergModify;
import org.apache.drill.metastore.iceberg.operate.IcebergRead;
import org.apache.drill.metastore.iceberg.transform.Transform;
import org.apache.drill.metastore.iceberg.write.FileWriter;
import org.apache.drill.metastore.iceberg.write.ParquetFileWriter;
import org.apache.drill.shaded.guava.com.google.common.collect.MapDifference;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Table;
import org.apache.iceberg.Tables;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopTables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Iceberg Drill Metastore implementation that inits / loads Iceberg table in which
 * Drill Metastore metadata will be stored.
 * Data will be partitioned by storage plugin, workspace, table name and metadata key.
 */
public class IcebergMetastore implements Metastore, MetastoreContext {

  private static final Logger logger = LoggerFactory.getLogger(IcebergMetastore.class);

  public static final IcebergTableSchema ICEBERG_TABLE_SCHEMA = IcebergTableSchema.of(MetadataUnit.class);

  private final String location;
  private final Tables tables;
  private final Map<String, String> tableProperties;
  private final Table metastore;

  public IcebergMetastore(DrillConfig config) {
    this(location(config), tables(config), tableProperties(config), true);
  }

  public IcebergMetastore(String location,
                          Tables tables,
                          Map<String, String> tableProperties,
                          boolean checkTableProperties) {
    this.location = location;
    this.tableProperties = tableProperties;
    this.tables = tables;
    this.metastore = loadMetastore(location, tables, tableProperties, checkTableProperties);
  }

  public MetastoreContext context() {
    return this;
  }

  @Override
  public Metastore get() {
    // Table properties will be checked / updated only once when initializing Metastore for the first time,
    // next time no need to check table properties, since the same Drill config properties are used.
    // New Iceberg Metastore instance is always returned to support concurrent writes and deletes.
    // Concurrency is correctly supported only for different table instances which point to the same table.
    return new IcebergMetastore(location, tables, tableProperties, false);
  }

  @Override
  public Metadata metadata() {
    return IcebergMetadata.init(metastore);
  }

  @Override
  public Read read() {
    return new IcebergRead(context());
  }

  @Override
  public Modify modify() {
    return new IcebergModify(context());
  }

  @Override
  public Table metastore() {
    return metastore;
  }

  @Override
  public FileWriter fileWriter() {
    return new ParquetFileWriter(metastore);
  }

  @Override
  public Transform transform() {
    return new Transform(context());
  }

  private static Configuration configuration(DrillConfig config) {
    Configuration configuration = new Configuration();
    if (config.hasPath(IcebergConfigConstants.CONFIG_PROPERTIES)) {
      Config configProperties = config.getConfig(IcebergConfigConstants.CONFIG_PROPERTIES);
      configProperties.entrySet().forEach(
        entry -> configuration.set(entry.getKey(), String.valueOf(entry.getValue().unwrapped()))
      );
    }
    return configuration;
  }

  /**
   * Constructs Iceberg table location based on given base and relative paths.
   * If {@link IcebergConfigConstants#BASE_PATH} is not set, user home directory is used.
   * {@link IcebergConfigConstants#RELATIVE_PATH} must be set.
   *
   * @param config Metastore config
   * @return Iceberg table location
   */
  private static String location(DrillConfig config) {
    Configuration configuration = configuration(config);
    FileSystem fs;
    try {
      fs = FileSystem.get(configuration);
    } catch (IOException e) {
      throw new IcebergMetastoreException(
        String.format("Error during file system [%s] setup", configuration.get(FileSystem.FS_DEFAULT_NAME_KEY)));
    }

    String root = fs.getHomeDirectory().toUri().getPath();
    if (config.hasPath(IcebergConfigConstants.BASE_PATH)) {
      root = config.getString(IcebergConfigConstants.BASE_PATH);
    }

    String relativeLocation = config.getString(IcebergConfigConstants.RELATIVE_PATH);
    if (relativeLocation == null) {
      throw new IcebergMetastoreException(String.format(
        "Iceberg Metastore relative path [%s] is not provided", IcebergConfigConstants.RELATIVE_PATH));
    }

    String location = new Path(root, relativeLocation).toUri().getPath();
    logger.info("Iceberg Metastore is located in [{}] on file system [{}]", location, fs.getUri());
    return location;
  }

  private static Tables tables(DrillConfig config) {
    return new HadoopTables(configuration(config));
  }

  private static Map<String, String> tableProperties(DrillConfig config) {
    return config.hasPath(IcebergConfigConstants.TABLE_PROPERTIES)
      ? config.getConfig(IcebergConfigConstants.TABLE_PROPERTIES).entrySet().stream()
      .collect(Collectors.toMap(
        Map.Entry::getKey,
        entry -> String.valueOf(entry.getValue().unwrapped()),
        (o, n) -> n))
      : Collections.emptyMap();
  }

  private Table loadMetastore(String location,
                              Tables tables,
                              Map<String, String> tableProperties,
                              boolean checkTableProperties) {
    Table metastore;
    try {
      metastore = tables.load(location);
    } catch (NoSuchTableException e) {
      try {
        // creating new Iceberg table, no need to check table properties
        return tables.create(ICEBERG_TABLE_SCHEMA.metastoreSchema(), ICEBERG_TABLE_SCHEMA.partitionSpec(),
          tableProperties, location);
      } catch (AlreadyExistsException ex) {
        metastore = tables.load(location);
      }
    }

    if (checkTableProperties) {
      updateTableProperties(metastore, tableProperties);
    }
    return metastore;
  }

  /**
   * Checks config table properties against current table properties.
   * Adds properties that are absent, updates existing and removes absent.
   * If properties are the same, does nothing.
   *
   * @param metastore Iceberg table instance
   * @param tableProperties table properties from the config
   */
  private void updateTableProperties(Table metastore, Map<String, String> tableProperties) {
    Map<String, String> currentProperties = metastore.properties();
    MapDifference<String, String> difference = Maps.difference(tableProperties, currentProperties);

    if (difference.areEqual()) {
      return;
    }

    UpdateProperties updateProperties = metastore.updateProperties();

    // collect properties that are different
    Map<String, String> propertiesToUpdate = difference.entriesDiffering().entrySet().stream()
      .collect(Collectors.toMap(
        Map.Entry::getKey,
        entry -> entry.getValue().leftValue(),
        (o, n) -> n));

    // add new properties
    propertiesToUpdate.putAll(difference.entriesOnlyOnLeft());

    logger.debug("Updating Iceberg table properties: {}", updateProperties);
    propertiesToUpdate.forEach(updateProperties::set);

    logger.debug("Removing Iceberg table properties: {}", difference.entriesOnlyOnRight());
    difference.entriesOnlyOnRight().keySet().forEach(updateProperties::remove);

    updateProperties.commit();
  }
}
