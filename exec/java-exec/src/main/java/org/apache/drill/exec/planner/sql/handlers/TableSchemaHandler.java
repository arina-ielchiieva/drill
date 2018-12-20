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
package org.apache.drill.exec.planner.sql.handlers;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.SchemaUtilites;
import org.apache.drill.exec.planner.sql.parser.SqlTableSchema;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleSchema;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.dfs.WorkspaceSchemaFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;

public abstract class TableSchemaHandler extends DefaultSqlHandler {


    /*
    we can create schema file manager

    who can
    1. create schema path
    2. delete
    3. read
    4. write

    who will use it
    create  / drop
    table function with path
    when reading for table during select if option is set

    who will create table schema with options and tuple metadata for table function when it's inline

Maybe we'll have table schema manager

but different providers:
path
tuple schema
string (inline)


     */

  public static class TableSchemaManager {

    public static final String DEFAULT_SCHEMA_NAME = ".drill.table_schema";
    private static final ObjectMapper mapper = new ObjectMapper().enable(INDENT_OUTPUT);

    private final FileSystem fs;
    private final Path schemaFilePath;
    private final String tableName;

    public TableSchemaManager(String path) throws IOException {
      this.schemaFilePath = new Path(path);
      Configuration conf = new Configuration();
      //conf.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
      //todo check manually how it works on dfs
      String scheme = schemaFilePath.toUri().getScheme();
      if (scheme != null) {
        conf.set(FileSystem.FS_DEFAULT_NAME_KEY, scheme);
      }
      this.fs = schemaFilePath.getFileSystem(conf);
      this.tableName = null;
    }

    public TableSchemaManager(WorkspaceSchemaFactory.WorkspaceSchema wsSchema, String tableName) throws IOException {
      this.fs = wsSchema.getFS();
      Path tablePath = new Path(wsSchema.getDefaultLocation(), tableName);
      Path schemaParentPath = fs.isFile(tablePath) ? tablePath.getParent() : tablePath;
      this.schemaFilePath = new Path(schemaParentPath, DEFAULT_SCHEMA_NAME);
      this.tableName = String.format("%s.`%s`", wsSchema.getFullSchemaName(), tableName);
    }

    public void createSchemaFile(String schema, LinkedHashMap<String, String> properties) throws IOException {
      //todo convert schema to TupleSchema
      createSchemaFile(new TupleSchema(), properties);
    }

    public void createSchemaFile(TupleSchema schema, LinkedHashMap<String, String> properties) throws IOException {
      List<String> schemaList = schema.toMetadataList().stream()
        .map(ColumnMetadata::columnString)
        .collect(Collectors.toList());
      TableSchema tableSchema = new TableSchema(tableName, schemaList, properties);
      String schemaString = mapper.writeValueAsString(tableSchema);
    }

    public TableSchema readSchemaFile() {
      return null;
    }

    public void deleteSchemaFile() throws IOException {
      try {
        if (!fs.delete(schemaFilePath, false)) {
          throw IOException(String.format("Error while deleting schema file - [%s]", schemaFilePath.toUri().getPath());
        }
      } catch (IOException e1) {
        // re-check file existence to cover concurrent deletion case
        try {
          if (fs.exists(schemaFilePath)) {
            throw e1;
          }
        } catch (IOException e2) {
          // ignore new exception and throw original one
          throw e1;
        }
      }
    }

  }

  //todo move to different common location as will be accessed during deserialization


  protected static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TableSchemaHandler.class);

  protected TableSchemaHandler(SqlHandlerConfig config) {
    super(config);
  }

  protected WorkspaceSchemaFactory.WorkspaceSchema getWorkspaceSchema(List<String> tableSchema, String tableName) {
    SchemaPlus defaultSchema = config.getConverter().getDefaultSchema();
    AbstractSchema temporarySchema = SchemaUtilites.resolveToTemporarySchema(tableSchema, defaultSchema, context.getConfig());

    if (context.getSession().isTemporaryTable(temporarySchema, context.getConfig(), tableName)) {
      throw UserException.validationError()
        .message("Indicated table [%s] is temporary table", tableName)
        .build(logger);
    }

    AbstractSchema drillSchema = SchemaUtilites.resolveToMutableDrillSchema(defaultSchema, tableSchema);
    Table table = SqlHandlerUtil.getTableFromSchema(drillSchema, tableName);
    if (table == null || table.getJdbcTableType() != Schema.TableType.TABLE) {
      throw UserException.validationError().message("Table [%s] was not found", tableName).build(logger);
    }

    if (!(drillSchema instanceof WorkspaceSchemaFactory.WorkspaceSchema)) {
      throw UserException.validationError()
        .message("Table [`%s`.`%s`] must belong to file storage plugin", drillSchema.getFullSchemaName(), tableName)
        .build(logger);
    }

    return (WorkspaceSchemaFactory.WorkspaceSchema) drillSchema;
  }

  protected Path getSchemaFilePath(WorkspaceSchemaFactory.WorkspaceSchema wsSchema, String tableName) throws IOException {
    FileSystem fs = wsSchema.getFS();
    Path tablePath = new Path(wsSchema.getDefaultLocation(), tableName);

    Path schemaParentPath = fs.isFile(tablePath) ? tablePath.getParent() : tablePath;
    return new Path(schemaParentPath, TableSchemaManager.DEFAULT_SCHEMA_NAME);
  }

  protected PhysicalPlan produceErrorResult(String message, boolean doFail) {
    if (doFail) {
      throw UserException.validationError().message(message).build(logger);
    } else {
      return DirectPlan.createDirectPlan(context, false, message);
    }
  }

  protected void deleteSchemaFle(FileSystem fs, Path schemaFilePath) throws IOException {
    try {
      if (!fs.delete(schemaFilePath, false)) {
        throw UserException.resourceError()
          .message("Error while deleting schema file: [%s]", schemaFilePath.toUri().getPath())
          .build(logger);
      }
    } catch (IOException e1) {
      // re-check file existence to cover concurrent deletion case
      try {
        if (fs.exists(schemaFilePath)) {
          throw e1;
        }
      } catch (IOException e2) {
        // ignore new exception and throw original one
        throw e1;
      }
    }
  }

  protected String getFullTableName(WorkspaceSchemaFactory.WorkspaceSchema wsSchema, String tableName) {
    return String.format("%s.`%s`", wsSchema.getFullSchemaName(), tableName);
  }

  public static class Create extends TableSchemaHandler {

    //todo move mapper with table schema class


    public Create(SqlHandlerConfig config) {
      super(config);
    }

    @Override
    public PhysicalPlan getPlan(SqlNode sqlNode) {

      SqlTableSchema.Create sqlCall = ((SqlTableSchema.Create) sqlNode);

      Path schemaFilePath;
      FileSystem fs;
      String fullTableName;

      try {
        if (sqlCall.getPath() != null) {
          fullTableName = null;
          schemaFilePath = new Path(sqlCall.getPath());
          Configuration conf = new Configuration();
          //conf.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
          //todo check manually how it works on dfs
          String scheme = schemaFilePath.toUri().getScheme();
          if (scheme != null) {
            conf.set(FileSystem.FS_DEFAULT_NAME_KEY, scheme);
          }
          fs = schemaFilePath.getFileSystem(conf);
          if (!fs.exists(schemaFilePath.getParent())) {
            throw UserException.validationError()
              .message("Parent directory for schema file [%s] does not exist", schemaFilePath.toUri().getPath())
              .build(logger);
          }
        } else {
          String tableName = sqlCall.getTableName();
          WorkspaceSchemaFactory.WorkspaceSchema wsSchema = getWorkspaceSchema(sqlCall.getSchemaPath(), tableName);
          schemaFilePath = getSchemaFilePath(wsSchema, tableName);
          fs = wsSchema.getFS();
          fullTableName = getFullTableName(wsSchema, tableName);
        }

        // validate creation possibility
        if (fs.exists(schemaFilePath)) {
          switch (sqlCall.getSqlCreateType()) {
            case SIMPLE:
              return produceErrorResult(String.format("Schema file already exists in / for %s", schemaFilePath), true);
            case OR_REPLACE:
              deleteSchemaFle(fs, schemaFilePath);
            case IF_NOT_EXISTS:
              return produceErrorResult(String.format("Schema file already exists in / for %s", schemaFilePath), false);
          }
        }
      } catch (IOException e) {
        throw UserException.resourceError(e)
          .message("Error while preparing schema file path for [%s]",
            sqlCall.getTable() != null ? sqlCall.getTable() : sqlCall.getPath())
          .build(logger);
      }

      // transform schema string into TupleSchema
      // create Json Object
      List<String> schema = new TupleSchema().toMetadataList().stream()
        .map(ColumnMetadata::columnString)
        .collect(Collectors.toList());

      //todo check how we will transform table name back, look at views
      TableSchema tableSchema = new TableSchema(fullTableName, schema, sqlCall.getProperties());

      try {
        String json = mapper.writeValueAsString(tableSchema);
        System.out.println(json);
      } catch (JsonProcessingException e) {
        throw UserException.executionError(e)
          .message("Error while transforming json")
          .build(logger);
      }


      // create file with content
      // new StorageStrategy(context.getOption(ExecConstants.PERSISTENT_TABLE_UMASK).string_val
      // when creating schema file use the same permission as for persistent tables
      //todo check how parquet metadata file is created


      System.out.println(sqlNode.toString());
      System.out.println(((SqlTableSchema.Create) sqlNode).getSchema());
      System.out.println(((SqlTableSchema.Create) sqlNode).getSqlCreateType());

      return DirectPlan.createDirectPlan(context, true, "Created table schema");
    }
  }

  public static class Drop extends TableSchemaHandler {

    public Drop(SqlHandlerConfig config) {
      super(config);
    }

    @Override
    public PhysicalPlan getPlan(SqlNode sqlNode) {
      SqlTableSchema.Drop sqlCall = ((SqlTableSchema.Drop) sqlNode);

      String tableName = sqlCall.getTableName();
      WorkspaceSchemaFactory.WorkspaceSchema wsSchema = getWorkspaceSchema(sqlCall.getSchemaPath(), tableName);
      String fullTableName = getFullTableName(wsSchema, tableName);

      try {
        Path schemaFilePath = getSchemaFilePath(wsSchema, tableName);
        FileSystem fs = wsSchema.getFS();

        if (!fs.exists(schemaFilePath)) {
          return produceErrorResult(String.format("Schema file [%s] does not exist in table [%s] root directory",
            TableSchemaManager.DEFAULT_SCHEMA_NAME, fullTableName), !sqlCall.ifExists());
        }

        deleteSchemaFle(fs, schemaFilePath);

        return DirectPlan.createDirectPlan(context, true,
          String.format("Dropped schema file for table [%s]", fullTableName));

      } catch (IOException e) {
        throw UserException.resourceError(e)
          .message("Error while accessing table location or deleting schema file for [%s]", fullTableName)
          .build(logger);
      }
    }
  }

  //todo consider moving to metadata package since will be used for deserialization, move object mapper as well
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public static class TableSchema {

    private String table;
    private List<String> schema = new ArrayList<>();
    // preserve properties order
    private LinkedHashMap<String, String> properties = new LinkedHashMap<>();

    @JsonCreator
    public TableSchema(@JsonProperty("table") String table,
                       @JsonProperty("schema") List<String> schema,
                       @JsonProperty("properties") LinkedHashMap<String, String> properties) {
      this.table = table;

      if (schema != null) {
        this.schema.addAll(schema);
      }

      if (properties != null) {
        this.properties.putAll(properties);
      }
    }

    @JsonProperty
    public String getTable() {
      return table;
    }

    @JsonProperty
    public List<String> getSchema() {
      return schema;
    }

    @JsonProperty
    public LinkedHashMap<String, String> getProperties() {
      return properties;
    }
  }

}
