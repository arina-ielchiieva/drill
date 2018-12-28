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

import java.util.LinkedHashMap;

public class InlineTableSchemaProvider implements TableSchemaProvider {

  private final String schema;
  private final LinkedHashMap<String, String> properties;

  //todo check how props can be provided in table function
  public InlineTableSchemaProvider(String schema, LinkedHashMap<String, String> properties) {
    this.schema = schema;
    this.properties = properties;
  }

  @Override
  public void delete() {
    throw new UnsupportedOperationException("Table schema deletion is not supported.");
  }

  @Override
  public void store(String schema, LinkedHashMap<String, String> properties) {
    throw new UnsupportedOperationException("Table schema storage is not supported.");
  }

  @Override
  public TableSchema read() {
    return new TableSchema(null, schema, properties);
  }

  @Override
  public boolean exists() {
    return false;
  }

}
