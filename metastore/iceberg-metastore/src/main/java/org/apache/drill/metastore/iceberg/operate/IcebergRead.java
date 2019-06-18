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
package org.apache.drill.metastore.iceberg.operate;

import org.apache.drill.metastore.Metastore;
import org.apache.drill.metastore.MetadataUnit;
import org.apache.drill.metastore.expressions.FilterExpression;
import org.apache.drill.metastore.iceberg.MetastoreContext;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of {@link Metastore.Read} interface.
 * Reads information from Iceberg table based on given filter expression.
 * Supports reading information for specific columns.
 */
public class IcebergRead implements Metastore.Read {

  private final MetastoreContext context;
  private final String[] defaultColumns;
  private final List<String> columns = new ArrayList<>();
  private FilterExpression filter;

  public IcebergRead(MetastoreContext context) {
    this.context = context;
    this.defaultColumns = context.metastore().schema().columns().stream()
      .map(Types.NestedField::name)
      .toArray(String[]::new);
  }

  @Override
  public Metastore.Read filter(FilterExpression filter) {
    this.filter = filter;
    return this;
  }

  @Override
  public Metastore.Read columns(List<String> columns) {
    this.columns.addAll(columns);
    return this;
  }

  @Override
  public List<MetadataUnit> execute() {
    String[] selectedColumns = columns.isEmpty() ? defaultColumns : columns.toArray(new String[0]);
    Iterable<Record> records = IcebergGenerics.read(context.metastore())
      .select(selectedColumns)
      .where(context.transform().filter().toFilter(filter))
      .build();

    return context.transform().data()
      .columns(selectedColumns)
      .records(Lists.newArrayList(records))
      .toMetadataUnits();
  }
}
