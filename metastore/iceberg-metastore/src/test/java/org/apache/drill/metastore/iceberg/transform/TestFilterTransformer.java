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

import org.apache.drill.metastore.expressions.FilterExpression;
import org.apache.drill.metastore.iceberg.IcebergBaseTest;
import org.apache.drill.metastore.iceberg.metadata.IcebergTableSchema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestFilterTransformer extends IcebergBaseTest {

  private static FilterTransformer transformer;

  @BeforeClass
  public static void init() {
    transformer = new FilterTransformer();
  }

  @Test
  public void testToFilterNull() {
    Expression expected = Expressions.alwaysTrue();
    Expression actual = transformer.toFilter(null);

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testToFilterEqual() {
    Expression expected = Expressions.equal("a", 1);
    Expression actual = transformer.toFilter(FilterExpression.equal("a", 1));

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testToFilterNotEqual() {
    Expression expected = Expressions.notEqual("a", 1);
    Expression actual = transformer.toFilter(FilterExpression.notEqual("a", 1));

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testToFilterLessThan() {
    Expression expected = Expressions.lessThan("a", 1);
    Expression actual = transformer.toFilter(FilterExpression.lessThan("a", 1));

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testToFilterLessThanOrEqual() {
    Expression expected = Expressions.lessThanOrEqual("a", 1);
    Expression actual = transformer.toFilter(FilterExpression.lessThanOrEqual("a", 1));

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testToFilterGreaterThan() {
    Expression expected = Expressions.greaterThan("a", 1);
    Expression actual = transformer.toFilter(FilterExpression.greaterThan("a", 1));

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testToFilterGreaterThanOrEqual() {
    Expression expected = Expressions.greaterThanOrEqual("a", 1);
    Expression actual = transformer.toFilter(FilterExpression.greaterThanOrEqual("a", 1));

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testToFilterIn() {
    Expression expected = Expressions.or(Expressions.equal("a", 1), Expressions.equal("a", 2));
    Expression actual = transformer.toFilter(FilterExpression.in("a", 1, 2));

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testToFilterNotIn() {
    Expression expected = Expressions.not(
      Expressions.or(Expressions.equal("a", 1), Expressions.equal("a", 2)));
    Expression actual = transformer.toFilter(FilterExpression.notIn("a", 1, 2));

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testToFilterIsNull() {
    Expression expected = Expressions.isNull("a");
    Expression actual = transformer.toFilter(FilterExpression.isNull("a"));

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testToFilterIsNotNull() {
    Expression expected = Expressions.notNull("a");
    Expression actual = transformer.toFilter(FilterExpression.isNotNull("a"));

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testToFilterNot() {
    Expression expected = Expressions.not(Expressions.equal("a", 1));
    Expression actual = transformer.toFilter(FilterExpression.not(FilterExpression.equal("a", 1)));

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testToFilterAnd() {
    Expression expected = Expressions.and(
      Expressions.and(
        Expressions.and(
          Expressions.equal("a", 1), Expressions.equal("b", 2)),
        Expressions.equal("c", 3)),
      Expressions.equal("d", 4));

    Expression actual = transformer.toFilter(FilterExpression.and(
      FilterExpression.equal("a", 1), FilterExpression.equal("b", 2),
      FilterExpression.equal("c", 3), FilterExpression.equal("d", 4)));

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testToFilterOr() {
    Expression expected = Expressions.or(Expressions.equal("a", 1), Expressions.equal("a", 2));
    Expression actual = transformer.toFilter(
      FilterExpression.or(FilterExpression.equal("a", 1), FilterExpression.equal("a", 2)));

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testToFilterUnsupported() {
    thrown.expect(UnsupportedOperationException.class);

    transformer.toFilter(new FilterExpression() {
      @Override
      public Operator operator() {
        return null;
      }

      @Override
      public <T> T accept(Visitor<T> visitor) {
        return visitor.visit(this);
      }
    });
  }

  @Test
  public void testToFilterTableKey() {
    DrillTableKey tableKey = new DrillTableKey("dfs", "tmp", "nation");
    String metadataKey = "dir0";

    Expression expected = Expressions.and(Expressions.and(Expressions.and(
      Expressions.equal(IcebergTableSchema.STORAGE_PLUGIN, tableKey.storagePlugin()),
      Expressions.equal(IcebergTableSchema.WORKSPACE, tableKey.workspace())),
      Expressions.equal(IcebergTableSchema.TABLE_NAME, tableKey.tableName())),
      Expressions.equal(IcebergTableSchema.METADATA_KEY, metadataKey));

    Expression actual = transformer.toFilter(tableKey, metadataKey);

    assertEquals(expected.toString(), actual.toString());
  }
}
