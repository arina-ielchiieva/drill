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
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.WorkspaceSchemaFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;

public abstract class TableSchemaHandler extends DefaultSqlHandler {

  public static final String DEFAULT_SCHEMA_NAME = ".drill.table_schema";

  protected static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TableSchemaHandler.class);

  protected TableSchemaHandler(SqlHandlerConfig config) {
    super(config);
  }

  protected WorkspaceSchemaFactory.WorkspaceSchema getWorkspaceSchema(List<String> tableSchema, String tableName) {
    SchemaPlus defaultSchema = config.getConverter().getDefaultSchema();
    AbstractSchema temporarySchema = SchemaUtilites.resolveToTemporarySchema(tableSchema, defaultSchema, context.getConfig());

    if (context.getSession().isTemporaryTable(temporarySchema, context.getConfig(), tableName)) {
      throw UserException.validationError()
        .message("Indicated table [%s] is temporary table.", tableName)
        .build(logger);
    }

    AbstractSchema drillSchema = SchemaUtilites.resolveToMutableDrillSchema(defaultSchema, tableSchema);
    Table table = SqlHandlerUtil.getTableFromSchema(drillSchema, tableName);
    if (table == null || table.getJdbcTableType() != Schema.TableType.TABLE) {
      throw UserException.validationError().message("Table [%s] was not found.", tableName).build(logger);
    }

    if (!(drillSchema instanceof WorkspaceSchemaFactory.WorkspaceSchema)) {
      throw UserException.validationError()
        .message("Table [`%s`.`%s`] must reside in file storage plugin.", drillSchema.getFullSchemaName(), tableName)
        .build(logger);
    }

    return (WorkspaceSchemaFactory.WorkspaceSchema) drillSchema;
  }

  protected Path getSchemaFilePath(WorkspaceSchemaFactory.WorkspaceSchema wsSchema, String tableName) throws IOException {
    FileSystem fs = wsSchema.getFS();
    Path tablePath = new Path(wsSchema.getDefaultLocation(), tableName);

    Path schemaParentPath = fs.isFile(tablePath) ? tablePath.getParent() : tablePath;
    return new Path(schemaParentPath, DEFAULT_SCHEMA_NAME);
  }

  protected PhysicalPlan produceErrorResult(String message, boolean doFail) {
    if (doFail) {
      throw UserException.validationError().message(message).build(logger);
    } else {
      return DirectPlan.createDirectPlan(context, false, message);
    }
  }

  protected boolean deleteSchemaFle(FileSystem fs, Path schemaFilePath) throws IOException {
    try {
      //todo when result can be false?
      return fs.delete(schemaFilePath, false);
    } catch (IOException e) {
      if (fs.exists(schemaFilePath)) {
        throw UserException.executionError(e)
          //todo for / in distinction when throwing an error
          .message("Error while deleting schema file - %s", schemaFilePath)
          .build(logger);
      }
      return true;
    }
  }

  public static class Create extends TableSchemaHandler {

    private static final ObjectMapper mapper = new ObjectMapper().enable(INDENT_OUTPUT);

    public Create(SqlHandlerConfig config) {
      super(config);
    }

    @Override
    public PhysicalPlan getPlan(SqlNode sqlNode) throws IOException {

      SqlTableSchema.Create sqlCall = ((SqlTableSchema.Create) sqlNode);

      // construct path
      Path schemaFilePath;
      FileSystem fs;

      if (sqlCall.getPath() != null) {
        schemaFilePath = new Path(sqlCall.getPath());
        Configuration conf = new Configuration();
        //conf.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
        String scheme = schemaFilePath.toUri().getScheme();
        if (scheme != null) {
          conf.set(FileSystem.FS_DEFAULT_NAME_KEY, scheme);
        }
        //todo produces IO -> move to try / catch
        fs = schemaFilePath.getFileSystem(conf);
        //todo should we check if parent directory exists?
      } else {
        String tableName = FileSelection.removeLeadingSlash(sqlCall.getTableName());
        //todo throws IO
        WorkspaceSchemaFactory.WorkspaceSchema wsSchema = getWorkspaceSchema(sqlCall.getSchemaPath(), tableName);
        schemaFilePath = getSchemaFilePath(wsSchema, tableName);
        fs = wsSchema.getFS();
      }

      // validate creation possibility
      if (fs.exists(schemaFilePath)) {
        switch (sqlCall.getSqlCreateType()) {
          case SIMPLE:
            return produceErrorResult(String.format("Schema file already exists in / for %s", schemaFilePath), true);
          case OR_REPLACE:
            //todo produces IO
            //todo handle the result if needed
            boolean result = deleteSchemaFle(fs, schemaFilePath);
          case IF_NOT_EXISTS:
            return produceErrorResult(String.format("Schema file already exists in / for %s", schemaFilePath), false);
        }
      }

      // transform schema string into TupleSchema
      // create Json Object
      List<String> schema = new TupleSchema().toMetadataList().stream()
        .map(ColumnMetadata::columnString)
        .collect(Collectors.toList());

      // todo construct table name with schema
      TableSchema tableSchema = new TableSchema("table", schema, sqlCall.getProperties());

      String json = mapper.writeValueAsString(tableSchema);
      System.out.println(json);

      // create file with content


      System.out.println(sqlNode.toString());
      System.out.println(((SqlTableSchema.Create) sqlNode).getSchema());
      System.out.println(((SqlTableSchema.Create) sqlNode).getSqlCreateType());

      // new StorageStrategy(context.getOption(ExecConstants.PERSISTENT_TABLE_UMASK).string_val
      // when creating schema file use the same permission as for persistent tables
      // we should disallow to create schema for temporary tables

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

      String tableName = FileSelection.removeLeadingSlash(sqlCall.getTableName());
      WorkspaceSchemaFactory.WorkspaceSchema wsSchema = getWorkspaceSchema(sqlCall.getSchemaPath(), tableName);

      try {
        Path schemaFilePath = getSchemaFilePath(wsSchema, tableName);
        FileSystem fs = wsSchema.getFS();

        if (!fs.exists(schemaFilePath)) {
          return produceErrorResult(String.format("Schema file [%s] does not exist in table [`%s`.`%s`] root directory",
            DEFAULT_SCHEMA_NAME, wsSchema.getFullSchemaName(), tableName), !sqlCall.ifExists());
        }

        //todo handle the result
        boolean result = deleteSchemaFle(fs, schemaFilePath);

        return DirectPlan.createDirectPlan(context, true,
          String.format("Dropped schema file for table [`%s`.`%s`].", wsSchema.getFullSchemaName(), tableName));

      } catch (IOException e) {
        throw UserException.resourceError(e)
          .message("There was an error while accessing table location or schema file.")
          .build(logger);
      }
    }
  }

  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public static class TableSchema {

    private String table;
    private List<String> schema = new ArrayList<>();
    private Map<String, String> properties = new LinkedHashMap<>();

    @JsonCreator
    public TableSchema(@JsonProperty("table") String table,
                       @JsonProperty("schema") List<String> schema,
                       @JsonProperty("properties") Map<String, String> properties) {
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
      return new ArrayList<>(schema);
    }

    @JsonProperty
    public Map<String, String> getProperties() {
      return new HashMap<>(properties);
    }
  }

}
