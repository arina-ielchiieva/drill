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
import org.apache.drill.metastore.iceberg.IcebergBaseTest;
import org.apache.drill.metastore.iceberg.IcebergMetastore;
import org.apache.drill.metastore.iceberg.exceptions.IcebergMetastoreException;
import org.apache.drill.metastore.metadata.MetadataInfo;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestDataTransformer extends IcebergBaseTest {

  private static Schema metastoreSchema;
  private static Schema partitionSchema;

  @BeforeClass
  public static void init() {
    metastoreSchema = IcebergMetastore.ICEBERG_TABLE_SCHEMA.metastoreSchema();
    partitionSchema = new Schema(IcebergMetastore.ICEBERG_TABLE_SCHEMA.partitionSpec()
      .partitionType().fields());
  }

  @Test
  public void testToMetadataUnitsNoData() {
    List<MetadataUnit> actualResult = new DataTransformer(metastoreSchema, partitionSchema)
      .columns(Arrays.asList("storagePlugin", "workspace", "tableName"))
      .records(Collections.emptyList())
      .toMetadataUnits();

    assertEquals(Collections.emptyList(), actualResult);
  }

  @Test
  public void testToMetadataUnitsValidDataOneRecord() {
    Map<String, String> partitionKeys = new HashMap<>();
    partitionKeys.put("dir0", "2018");
    partitionKeys.put("dir1", "2019");
    List<String> partitionValues = Arrays.asList("a", "b", "c");
    Long lastModifiedTime = System.currentTimeMillis();

    Record record = GenericRecord.create(metastoreSchema);
    record.setField("storagePlugin", "dfs");
    record.setField("workspace", "tmp");
    record.setField("tableName", "nation");
    record.setField("partitionKeys", partitionKeys);
    record.setField("partitionValues", partitionValues);
    record.setField("lastModifiedTime", lastModifiedTime);

    List<MetadataUnit> actualResult = new DataTransformer(metastoreSchema, partitionSchema)
      .columns(Arrays.asList("storagePlugin", "workspace", "tableName",
        "partitionKeys", "partitionValues", "lastModifiedTime"))
      .records(Collections.singletonList(record))
      .toMetadataUnits();

    List<MetadataUnit> expectedResult = Collections.singletonList(MetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .partitionKeys(partitionKeys)
      .partitionValues(partitionValues)
      .lastModifiedTime(lastModifiedTime)
      .build());

    assertEquals(expectedResult, actualResult);
  }

  @Test
  public void testToMetadataUnitsValidDataSeveralRecords() {
    Record record1 = GenericRecord.create(metastoreSchema);
    record1.setField("tableName", "a");

    Record record2 = GenericRecord.create(metastoreSchema);
    record2.setField("tableName", "b");

    Record record3 = GenericRecord.create(metastoreSchema);
    record3.setField("tableName", "c");


    List<MetadataUnit> actualResult = new DataTransformer(metastoreSchema, partitionSchema)
      .columns(Collections.singletonList("tableName"))
      .records(Arrays.asList(record1, record2, record3))
      .toMetadataUnits();

    List<MetadataUnit> expectedResult = Arrays.asList(
      MetadataUnit.builder().tableName("a").build(),
      MetadataUnit.builder().tableName("b").build(),
      MetadataUnit.builder().tableName("c").build());

    assertEquals(expectedResult, actualResult);
  }

  @Test
  public void testToMetadataUnitsInvalidColumns() {
    Record record = GenericRecord.create(metastoreSchema);
    record.setField("tableName", "a");

    List<MetadataUnit> actualResult = new DataTransformer(metastoreSchema, partitionSchema)
      .records(Collections.singletonList(record))
      .columns(Arrays.asList("a", "b"))
      .toMetadataUnits();

    List<MetadataUnit> expectedResult = Collections.singletonList(MetadataUnit.builder().build());

    assertEquals(expectedResult, actualResult);
  }

  @Test
  public void testToWriteDataNoData() {
    WriteData writeData = new DataTransformer(metastoreSchema, partitionSchema)
      .units(Collections.emptyList())
      .toWriteData();

    assertEquals(Collections.emptyList(), writeData.records());
    assertNull(writeData.partition());
  }

  @Test
  public void testToWriteDataValidDataOneRecord() {
    Map<String, String> partitionKeys = new HashMap<>();
    partitionKeys.put("dir0", "2018");
    partitionKeys.put("dir1", "2019");
    List<String> partitionValues = Arrays.asList("a", "b", "c");
    Long lastModifiedTime = System.currentTimeMillis();

    MetadataUnit unit = MetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .metadataKey(MetadataInfo.GENERAL_INFO_KEY)
      .partitionKeys(partitionKeys)
      .partitionValues(partitionValues)
      .lastModifiedTime(lastModifiedTime)
      .build();

    WriteData writeData = new DataTransformer(metastoreSchema, partitionSchema)
      .units(Collections.singletonList(unit))
      .toWriteData();

    Record tableRecord = GenericRecord.create(metastoreSchema);
    tableRecord.setField("storagePlugin", "dfs");
    tableRecord.setField("workspace", "tmp");
    tableRecord.setField("tableName", "nation");
    tableRecord.setField("metadataKey", MetadataInfo.GENERAL_INFO_KEY);
    tableRecord.setField("partitionKeys", partitionKeys);
    tableRecord.setField("partitionValues", partitionValues);
    tableRecord.setField("lastModifiedTime", lastModifiedTime);

    Record partitionRecord = GenericRecord.create(partitionSchema);
    partitionRecord.setField("storagePlugin", "dfs");
    partitionRecord.setField("workspace", "tmp");
    partitionRecord.setField("tableName", "nation");
    partitionRecord.setField("metadataKey", MetadataInfo.GENERAL_INFO_KEY);

    assertEquals(Collections.singletonList(tableRecord), writeData.records());
    assertEquals(partitionRecord, writeData.partition());
  }

  @Test
  public void testToWriteDataValidDataSeveralRecords() {
    List<MetadataUnit> units = Arrays.asList(
      MetadataUnit.builder()
        .storagePlugin("dfs")
        .workspace("tmp")
        .tableName("nation")
        .metadataKey(MetadataInfo.GENERAL_INFO_KEY)
        .column("a")
        .build(),
      MetadataUnit.builder()
        .storagePlugin("dfs")
        .workspace("tmp")
        .tableName("nation")
        .metadataKey(MetadataInfo.GENERAL_INFO_KEY)
        .column("b")
        .build(),
      MetadataUnit.builder()
        .storagePlugin("dfs")
        .workspace("tmp")
        .tableName("nation")
        .metadataKey(MetadataInfo.GENERAL_INFO_KEY)
        .column("c")
        .build());

    WriteData writeData = new DataTransformer(metastoreSchema, partitionSchema)
      .units(units)
      .toWriteData();

    Record tableRecord1 = GenericRecord.create(metastoreSchema);
    tableRecord1.setField("storagePlugin", "dfs");
    tableRecord1.setField("workspace", "tmp");
    tableRecord1.setField("tableName", "nation");
    tableRecord1.setField("metadataKey", MetadataInfo.GENERAL_INFO_KEY);
    tableRecord1.setField("column", "a");

    Record tableRecord2 = GenericRecord.create(metastoreSchema);
    tableRecord2.setField("storagePlugin", "dfs");
    tableRecord2.setField("workspace", "tmp");
    tableRecord2.setField("tableName", "nation");
    tableRecord2.setField("metadataKey", MetadataInfo.GENERAL_INFO_KEY);
    tableRecord2.setField("column", "b");

    Record tableRecord3 = GenericRecord.create(metastoreSchema);
    tableRecord3.setField("storagePlugin", "dfs");
    tableRecord3.setField("workspace", "tmp");
    tableRecord3.setField("tableName", "nation");
    tableRecord3.setField("metadataKey", MetadataInfo.GENERAL_INFO_KEY);
    tableRecord3.setField("column", "c");

    Record partitionRecord = GenericRecord.create(partitionSchema);
    partitionRecord.setField("storagePlugin", "dfs");
    partitionRecord.setField("workspace", "tmp");
    partitionRecord.setField("tableName", "nation");
    partitionRecord.setField("metadataKey", MetadataInfo.GENERAL_INFO_KEY);

    assertEquals(Arrays.asList(tableRecord1, tableRecord2, tableRecord3), writeData.records());
    assertEquals(partitionRecord, writeData.partition());
  }

  @Test
  public void testToWriteDataInvalidPartition() {
    MetadataUnit unit = MetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .build();

    thrown.expect(IcebergMetastoreException.class);

    new DataTransformer(metastoreSchema, partitionSchema)
      .units(Collections.singletonList(unit))
      .toWriteData();
  }

  @Test
  public void testToWriteDataNonMatchingPartitionKey() {
    List<MetadataUnit> units = Arrays.asList(
      MetadataUnit.builder()
        .storagePlugin("dfs")
        .workspace("tmp")
        .tableName("a")
        .metadataKey(MetadataInfo.GENERAL_INFO_KEY)
        .build(),
      MetadataUnit.builder()
        .storagePlugin("dfs")
        .workspace("tmp")
        .tableName("b")
        .metadataKey(MetadataInfo.DEFAULT_SEGMENT_KEY)
        .build(),
      MetadataUnit.builder()
        .storagePlugin("dfs")
        .workspace("tmp")
        .tableName("c")
        .metadataKey(MetadataInfo.GENERAL_INFO_KEY)
        .build());

    thrown.expect(IcebergMetastoreException.class);

    new DataTransformer(metastoreSchema, partitionSchema)
      .units(units)
      .toWriteData();
  }
}
