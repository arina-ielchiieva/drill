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
import org.apache.drill.metastore.iceberg.exceptions.IcebergMetastoreException;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Transforms given data from {@link MetadataUnit} into {@link Record} or visa versa.
 */
public class DataTransformer {

  private final Schema metastoreSchema;
  private final Schema partitionSpecSchema;
  private final List<String> columns = new ArrayList<>();
  private final List<Record> records = new ArrayList<>();
  private final List<MetadataUnit> units = new ArrayList<>();

  public DataTransformer(Schema metastoreSchema, Schema partitionSpecSchema) {
    this.metastoreSchema = metastoreSchema;
    this.partitionSpecSchema = partitionSpecSchema;
  }

  public DataTransformer columns(List<String> columns) {
    this.columns.addAll(columns);
    return this;
  }

  public DataTransformer columns(String... columns) {
    return columns(Arrays.asList(columns));
  }

  public DataTransformer records(List<Record> records) {
    this.records.addAll(records);
    return this;
  }

  public DataTransformer units(List<MetadataUnit> units) {
    this.units.addAll(units);
    return this;
  }

  public List<MetadataUnit> toMetadataUnits() {
    List<MetadataUnit> results = new ArrayList<>();
    for (Record record : records) {
      MetadataUnit.Builder builder = MetadataUnit.builder();
      for (String column : columns) {
        MethodHandle methodHandle = MetadataUnit.SCHEMA.unitBuilderSetters().get(column);
        if (methodHandle == null) {
          // ignore absent setters
          continue;
        }
        Object value = record.getField(column);
        try {
          methodHandle.invokeWithArguments(builder, value);
        } catch (Throwable e) {
          throw new IcebergMetastoreException(
            String.format("Unable to invoke setter for column [%s] using [%s]", column, methodHandle), e);
        }
      }
      results.add(builder.build());
    }
    return results;
  }

  public WriteData toWriteData() {
    List<Record> records = new ArrayList<>();
    Set<Record> partitions = new HashSet<>();
    Map<String, MethodHandle> unitGetters = MetadataUnit.SCHEMA.unitGetters();
    for (MetadataUnit unit : units) {
      partitions.add(getPartition(unit, partitionSpecSchema, unitGetters));

      Record record = GenericRecord.create(metastoreSchema);
      for (Types.NestedField column : metastoreSchema.columns()) {
        String name = column.name();
        MethodHandle methodHandle = unitGetters.get(name);
        if (methodHandle == null) {
          // ignore absent getters
          continue;
        }
        try {
          record.setField(name, methodHandle.invoke(unit));
        } catch (Throwable e) {
          throw new IcebergMetastoreException(
            String.format("Unable to invoke getter for column [%s] using [%s]", name, methodHandle), e);
        }
      }
      records.add(record);
    }

    if (partitions.size() > 1) {
      throw new IcebergMetastoreException(String.format(
        "Partition keys values must be the same for all records in the partition. " +
          "Partition schema: [%s]. Received partition values: %s", partitionSpecSchema, partitions));
    }

    return new WriteData(records, partitions.isEmpty() ? null : partitions.iterator().next());
  }

  private Record getPartition(MetadataUnit unit,
                              Schema schema,
                              Map<String, MethodHandle> unitGetters) {
    Record partitionRecord = GenericRecord.create(schema);
    for (Types.NestedField column : schema.columns()) {
      String name = column.name();
      MethodHandle methodHandle = unitGetters.get(name);
      if (methodHandle == null) {
        throw new IcebergMetastoreException(
          String.format("Getter for partition key [%s::%s] must be declared in [%s] class",
            name, column.type(), unit.getClass().getSimpleName()));
      }
      Object value;
      try {
       value = methodHandle.invoke(unit);
      } catch (Throwable e) {
        throw new IcebergMetastoreException(
          String.format("Unable to invoke getter for column [%s] using [%s]", name, methodHandle), e);
      }

      if (value == null) {
        throw new IcebergMetastoreException(
          String.format("Partition key [%s::%s] value must be set", name, column.type()));
      }
      partitionRecord.setField(name, value);
    }
    return partitionRecord;
  }
}
