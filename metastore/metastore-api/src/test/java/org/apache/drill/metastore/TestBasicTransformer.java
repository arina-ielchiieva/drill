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
package org.apache.drill.metastore;

import org.apache.drill.metastore.metadata.BaseTableMetadata;
import org.apache.drill.metastore.metadata.FileMetadata;
import org.apache.drill.metastore.metadata.MetadataInfo;
import org.apache.drill.metastore.metadata.MetadataType;
import org.apache.drill.metastore.metadata.PartitionMetadata;
import org.apache.drill.metastore.metadata.RowGroupMetadata;
import org.apache.drill.metastore.metadata.SegmentMetadata;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBasicTransformer {

  @Test
  public void testTables() {
    MetadataUnit table = MetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .metadataType(MetadataType.TABLE.name())
      .metadataKey(MetadataInfo.GENERAL_INFO_KEY)
      .columnsStatistics(Collections.emptyMap())
      .metadataStatistics(Collections.emptyList())
      .partitionKeys(Collections.emptyMap())
      .build();

    MetadataUnit segment = MetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .metadataType(MetadataType.SEGMENT.name())
      .metadataKey(MetadataInfo.DEFAULT_SEGMENT_KEY)
      .columnsStatistics(Collections.emptyMap())
      .metadataStatistics(Collections.emptyList())
      .build();

    List<BaseTableMetadata> tables = BasicTransformer.tables(Arrays.asList(table, segment));
    assertEquals(1, tables.size());
  }

  @Test
  public void testSegments() {
    MetadataUnit table = MetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .metadataType(MetadataType.TABLE.name())
      .metadataKey(MetadataInfo.GENERAL_INFO_KEY)
      .columnsStatistics(Collections.emptyMap())
      .metadataStatistics(Collections.emptyList())
      .partitionKeys(Collections.emptyMap())
      .build();

    MetadataUnit segment = MetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .metadataType(MetadataType.SEGMENT.name())
      .metadataKey(MetadataInfo.DEFAULT_SEGMENT_KEY)
      .columnsStatistics(Collections.emptyMap())
      .metadataStatistics(Collections.emptyList())
      .path("/tmp/nation")
      .locations(Collections.emptyList())
      .build();

    List<SegmentMetadata> segments = BasicTransformer.segments(Arrays.asList(table, segment));
    assertEquals(1, segments.size());
  }

  @Test
  public void testFiles() {
    MetadataUnit table = MetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .metadataType(MetadataType.TABLE.name())
      .metadataKey(MetadataInfo.GENERAL_INFO_KEY)
      .columnsStatistics(Collections.emptyMap())
      .metadataStatistics(Collections.emptyList())
      .partitionKeys(Collections.emptyMap())
      .build();

    MetadataUnit file0 = MetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .metadataType(MetadataType.FILE.name())
      .metadataKey(MetadataInfo.DEFAULT_SEGMENT_KEY)
      .columnsStatistics(Collections.emptyMap())
      .metadataStatistics(Collections.emptyList())
      .path("/tmp/nation/0_0_0.parquet")
      .build();

    MetadataUnit file1 = MetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .metadataType(MetadataType.FILE.name())
      .metadataKey(MetadataInfo.DEFAULT_SEGMENT_KEY)
      .columnsStatistics(Collections.emptyMap())
      .metadataStatistics(Collections.emptyList())
      .path("/tmp/nation/0_0_1.parquet")
      .build();

    List<FileMetadata> files = BasicTransformer.files(Arrays.asList(table, file0, file1));
    assertEquals(2, files.size());
  }

  @Test
  public void testRowGroups() {
    MetadataUnit file = MetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .metadataType(MetadataType.FILE.name())
      .metadataKey(MetadataInfo.DEFAULT_SEGMENT_KEY)
      .columnsStatistics(Collections.emptyMap())
      .metadataStatistics(Collections.emptyList())
      .path("/tmp/nation/0_0_0.parquet")
      .build();

    MetadataUnit rowGroup = MetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .metadataType(MetadataType.ROW_GROUP.name())
      .metadataKey(MetadataInfo.DEFAULT_SEGMENT_KEY)
      .columnsStatistics(Collections.emptyMap())
      .metadataStatistics(Collections.emptyList())
      .path("/tmp/nation/0_0_0.parquet")
      .rowGroupIndex(1)
      .hostAffinity(Collections.emptyMap())
      .build();

    List<RowGroupMetadata> rowGroups = BasicTransformer.rowGroups(Arrays.asList(file, rowGroup));
    assertEquals(1, rowGroups.size());
  }

  @Test
  public void testPartitions() {
    MetadataUnit partition1 = MetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .metadataType(MetadataType.PARTITION.name())
      .metadataKey(MetadataInfo.DEFAULT_SEGMENT_KEY)
      .columnsStatistics(Collections.emptyMap())
      .metadataStatistics(Collections.emptyList())
      .path("/tmp/nation/2018")
      .locations(Collections.emptyList())
      .column("dir0")
      .partitionValues(Collections.emptyList())
      .build();

    MetadataUnit partition2 = MetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .metadataType(MetadataType.PARTITION.name())
      .metadataKey(MetadataInfo.DEFAULT_SEGMENT_KEY)
      .columnsStatistics(Collections.emptyMap())
      .metadataStatistics(Collections.emptyList())
      .path("/tmp/nation/2019")
      .locations(Collections.emptyList())
      .column("dir0")
      .partitionValues(Collections.emptyList())
      .build();

    List<PartitionMetadata> partitions = BasicTransformer.partitions(Arrays.asList(partition1, partition2));
    assertEquals(2, partitions.size());
  }

  @Test
  public void testAll() {
    MetadataUnit table = MetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .metadataType(MetadataType.TABLE.name())
      .metadataKey(MetadataInfo.GENERAL_INFO_KEY)
      .columnsStatistics(Collections.emptyMap())
      .metadataStatistics(Collections.emptyList())
      .partitionKeys(Collections.emptyMap())
      .build();

    MetadataUnit segment = MetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .metadataType(MetadataType.SEGMENT.name())
      .metadataKey(MetadataInfo.DEFAULT_SEGMENT_KEY)
      .columnsStatistics(Collections.emptyMap())
      .metadataStatistics(Collections.emptyList())
      .path("/tmp/nation")
      .locations(Collections.emptyList())
      .build();

    MetadataUnit file0 = MetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .metadataType(MetadataType.FILE.name())
      .metadataKey(MetadataInfo.DEFAULT_SEGMENT_KEY)
      .columnsStatistics(Collections.emptyMap())
      .metadataStatistics(Collections.emptyList())
      .path("/tmp/nation/0_0_0.parquet")
      .build();

    MetadataUnit file1 = MetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .metadataType(MetadataType.FILE.name())
      .metadataKey(MetadataInfo.DEFAULT_SEGMENT_KEY)
      .columnsStatistics(Collections.emptyMap())
      .metadataStatistics(Collections.emptyList())
      .path("/tmp/nation/0_0_1.parquet")
      .build();

    MetadataUnit rowGroup = MetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .metadataType(MetadataType.ROW_GROUP.name())
      .metadataKey(MetadataInfo.DEFAULT_SEGMENT_KEY)
      .columnsStatistics(Collections.emptyMap())
      .metadataStatistics(Collections.emptyList())
      .path("/tmp/nation/0_0_0.parquet")
      .rowGroupIndex(1)
      .hostAffinity(Collections.emptyMap())
      .build();

    MetadataUnit partition1 = MetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .metadataType(MetadataType.PARTITION.name())
      .metadataKey(MetadataInfo.DEFAULT_SEGMENT_KEY)
      .columnsStatistics(Collections.emptyMap())
      .metadataStatistics(Collections.emptyList())
      .path("/tmp/nation/2018")
      .locations(Collections.emptyList())
      .column("dir0")
      .partitionValues(Collections.emptyList())
      .build();

    MetadataUnit partition2 = MetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .metadataType(MetadataType.PARTITION.name())
      .metadataKey(MetadataInfo.DEFAULT_SEGMENT_KEY)
      .columnsStatistics(Collections.emptyMap())
      .metadataStatistics(Collections.emptyList())
      .path("/tmp/nation/2019")
      .locations(Collections.emptyList())
      .column("dir0")
      .partitionValues(Collections.emptyList())
      .build();

    BasicTransformer.MetadataHolder all = BasicTransformer.all(
      Arrays.asList(table, segment, file0, file1, rowGroup, partition1, partition2));
    assertEquals(1, all.tables().size());
    assertEquals(1, all.segments().size());
    assertEquals(2, all.files().size());
    assertEquals(1, all.rowGroups().size());
    assertEquals(2, all.partitions().size());
  }

  @Test
  public void testUnexpectedType() {
    MetadataUnit table = MetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .metadataType(MetadataType.TABLE.name())
      .metadataKey(MetadataInfo.GENERAL_INFO_KEY)
      .columnsStatistics(Collections.emptyMap())
      .metadataStatistics(Collections.emptyList())
      .partitionKeys(Collections.emptyMap())
      .build();

    MetadataUnit nullMetadataType = MetadataUnit.builder().metadataType(null).build();
    MetadataUnit noneMetadataType = MetadataUnit.builder().metadataType(MetadataType.NONE.name()).build();

    List<MetadataUnit> units = Arrays.asList(table, nullMetadataType, noneMetadataType);

    List<BaseTableMetadata> tables = BasicTransformer.tables(units);
    assertEquals(1, tables.size());

    BasicTransformer.MetadataHolder all = BasicTransformer.all(units);
    assertEquals(1, all.tables().size());
    assertTrue(all.segments().isEmpty());
    assertTrue(all.files().isEmpty());
    assertTrue(all.rowGroups().isEmpty());
    assertTrue(all.partitions().isEmpty());
  }

  @Test
  public void testNoResult() {
    List<MetadataUnit> units = Collections.singletonList(MetadataUnit.builder().metadataType(null).build());

    assertTrue(BasicTransformer.tables(units).isEmpty());
    assertTrue(BasicTransformer.segments(units).isEmpty());
    assertTrue(BasicTransformer.files(units).isEmpty());
    assertTrue(BasicTransformer.rowGroups(units).isEmpty());
    assertTrue(BasicTransformer.partitions(units).isEmpty());
  }
}
