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
import org.apache.drill.metastore.iceberg.IcebergBaseTest;
import org.apache.drill.metastore.iceberg.IcebergMetastore;
import org.apache.drill.metastore.iceberg.MetastoreContext;
import org.apache.drill.metastore.iceberg.metadata.IcebergTableSchema;
import org.apache.drill.metastore.iceberg.operate.Delete;
import org.apache.drill.metastore.iceberg.operate.Overwrite;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestOperationTransformer extends IcebergBaseTest {

  private static String location;
  private static OperationTransformer transformer;

  @BeforeClass
  public static void init() throws Exception {
    location = new Path(defaultFolder.newFolder(TestOperationTransformer.class.getSimpleName()).toURI().getPath()).toUri().getPath();
    MetastoreContext context = new IcebergMetastore(location,
      new HadoopTables(baseHadoopConfig()), Collections.emptyMap(), false).context();
    transformer = new OperationTransformer(context);
  }

  @Test
  public void testToOverwriteOperation() {
    MetadataUnit unit = MetadataUnit.builder()
      .storagePlugin("dfs").workspace("tmp").tableName("nation").metadataKey("dir0").build();

    DrillTableKey tableKey = new DrillTableKey(unit.storagePlugin(), unit.workspace(), unit.tableName());

    Expression expectedFilter = Expressions.and(Expressions.and(Expressions.and(
      Expressions.equal(IcebergTableSchema.STORAGE_PLUGIN, tableKey.storagePlugin()),
      Expressions.equal(IcebergTableSchema.WORKSPACE, tableKey.workspace())),
      Expressions.equal(IcebergTableSchema.TABLE_NAME, tableKey.tableName())),
      Expressions.equal(IcebergTableSchema.METADATA_KEY, unit.metadataKey()));

    Overwrite operation = transformer.toOverwrite(tableKey, unit.metadataKey(), Collections.singletonList(unit));

    assertEquals(expectedFilter.toString(), operation.filter().toString());

    File file = new File(new Path(String.valueOf(operation.dataFile().path())).toUri().getPath());
    assertTrue(file.exists());
    assertEquals(tableKey.toLocation(location), file.getParent());
  }

  @Test
  public void testToOverwriteOperations() {
    List<MetadataUnit> units = Arrays.asList(
      MetadataUnit.builder().storagePlugin("dfs").workspace("tmp").tableName("nation").metadataKey("dir0").build(),
      MetadataUnit.builder().storagePlugin("dfs").workspace("tmp").tableName("nation").metadataKey("dir0").build(),
      MetadataUnit.builder().storagePlugin("dfs").workspace("tmp").tableName("nation").metadataKey("dir2").build(),
      MetadataUnit.builder().storagePlugin("dfs").workspace("tmp").tableName("nation").metadataKey("dir2").build(),
      MetadataUnit.builder().storagePlugin("dfs").workspace("tmp").tableName("region").metadataKey("dir0").build(),
      MetadataUnit.builder().storagePlugin("s3").workspace("tmp").tableName("region").metadataKey("dir0").build());

    List<Overwrite> operations = transformer.toOverwrite(units);
    assertEquals(4, operations.size());
  }

  @Test
  public void testToDeleteOperation() {
    FilterExpression filter = FilterExpression.and(
      FilterExpression.equal("storagePlugin", "dfs"),
      FilterExpression.equal("workspace", "tmp"));

    Expression expected = Expressions.and(
      Expressions.equal(IcebergTableSchema.STORAGE_PLUGIN, "dfs"),
      Expressions.equal(IcebergTableSchema.WORKSPACE, "tmp"));

    Delete operation = transformer.toDelete(filter);

    assertEquals(expected.toString(), operation.filter().toString());
  }

  @Test
  public void testToDeleteOperations() {
    FilterExpression dfs = FilterExpression.equal("storagePlugin", "dfs");
    FilterExpression s3 = FilterExpression.equal("storagePlugin", "s3");

    List<Delete> operations = transformer.toDelete(Arrays.asList(dfs, s3));

    assertEquals(2, operations.size());
  }
}
