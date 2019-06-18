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
package org.apache.drill.metastore.iceberg.transform;

import org.apache.drill.metastore.MetadataUnit;
import org.apache.drill.metastore.expressions.FilterExpression;
import org.apache.drill.metastore.iceberg.MetastoreContext;
import org.apache.drill.metastore.iceberg.operate.Delete;
import org.apache.drill.metastore.iceberg.operate.Overwrite;
import org.apache.drill.metastore.iceberg.write.File;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.expressions.Expression;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Transforms given input into {@link org.apache.drill.metastore.iceberg.operate.IcebergOperation}
 * implementations.
 */
public class OperationTransformer {

  private final MetastoreContext context;

  public OperationTransformer(MetastoreContext context) {
    this.context = context;
  }

  public List<Overwrite> toOverwrite(List<MetadataUnit> units) {
    Map<DrillTableKey, Map<String, List<MetadataUnit>>> data = units.stream()
      .collect(
        Collectors.groupingBy(DrillTableKey::of,
          Collectors.groupingBy(MetadataUnit::metadataKey)
        ));

    return data.entrySet().parallelStream()
      .map(dataEntry -> dataEntry.getValue().entrySet().parallelStream()
        .map(operationEntry -> toOverwrite(dataEntry.getKey(), operationEntry.getKey(), operationEntry.getValue()))
        .collect(Collectors.toList()))
      .flatMap(Collection::stream)
      .collect(Collectors.toList());
  }

  public Overwrite toOverwrite(DrillTableKey tableKey, String metadataKey, List<MetadataUnit> units) {
    WriteData writeData = context.transform().data()
      .units(units)
      .toWriteData();

    File file = context.fileWriter()
      .records(writeData.records())
      .location(tableKey.toLocation(context.metastore().location()))
      .name(UUID.randomUUID().toString())
      .write();

    DataFile dataFile = DataFiles.builder(context.metastore().spec())
      .withInputFile(file.input())
      .withMetrics(file.metrics())
      .withPartition(writeData.partition())
      .build();

    Expression expression = context.transform().filter().toFilter(tableKey, metadataKey);
    return new Overwrite(dataFile, expression);
  }

  public List<Delete> toDelete(List<FilterExpression> filters) {
    return filters.stream()
      .map(this::toDelete)
      .collect(Collectors.toList());
  }

  public Delete toDelete(FilterExpression filter) {
    return new Delete(context.transform().filter().toFilter((filter)));
  }
}
