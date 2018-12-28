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

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.LinkedHashMap;

import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;

public interface TableSchemaProvider {

  String DEFAULT_SCHEMA_NAME = ".drill.table_schema";
  ObjectMapper MAPPER = new ObjectMapper().enable(INDENT_OUTPUT);

  /*
  //todo think we need anything to add to the mapper
    mapper.registerModule(deserModule);
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    mapper.configure(Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    mapper.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, true);
    mapper.configure(Feature.ALLOW_COMMENTS, true);
    mapper.setFilterProvider(new SimpleFilterProvider().setFailOnUnknownId(false));
   */

  void delete() throws IOException;

  void store(String schema, LinkedHashMap<String, String> properties) throws IOException;

  TableSchema read() throws IOException;

  boolean exists() throws IOException;

}
