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
package org.apache.drill.exec.record.metadata.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleSchema;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class TableSchema {

  private final String table;
  private final TupleSchema schema;
  // preserve properties order
  private final LinkedHashMap<String, String> properties = new LinkedHashMap<>();

  @JsonCreator
  public TableSchema(@JsonProperty("table") String table,
                     @JsonProperty("schema") List<String> schema,
                     @JsonProperty("properties") LinkedHashMap<String, String> properties) {
    this(table, String.join(", ", schema), properties);
    //this(table, schema == null ? null : String.join(", ", schema), properties);
  }

  public TableSchema(String table, String schema, LinkedHashMap<String, String> properties) {
    this.table = table;
    this.schema = schema == null ? null : convert(schema);
    if (properties != null) {
      this.properties.putAll(properties);
    }
  }

  @JsonProperty("table")
  public String getTable() {
    return table;
  }

  @JsonProperty("schema")
  public List<String> getSchemaList() {
    return schema == null ? null : schema.toMetadataList().stream()
      .map(ColumnMetadata::columnString)
      .collect(Collectors.toList());
  }

  @JsonIgnore
  public TupleSchema getSchema() {
    return schema;
  }

  @JsonProperty("properties")
  public LinkedHashMap<String, String> getProperties() {
    return properties;
  }

  private TupleSchema convert(String schema) {
    //todo create visitor and convert
    return new TupleSchema();
  }

  @Override
  public String toString() {
    return "TableSchema{" + "table='" + table + '\'' + ", schema=" + schema + ", properties=" + properties + '}';
  }
}
