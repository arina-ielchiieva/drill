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
import org.apache.drill.metastore.iceberg.metadata.IcebergTableSchema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;

/**
 * Transforms given input into Iceberg {@link Expression} which is used as filter
 * to retrieve, overwrite or delete Metastore data.
 */
public class FilterTransformer {

  private final FilterExpression.Visitor<Expression> visitor = FilterExpressionVisitor.get();

  public Expression toFilter(FilterExpression filter) {
    return filter == null ? Expressions.alwaysTrue() : filter.accept(visitor);
  }

  public Expression toFilter(DrillTableKey tableKey, String metadataKey) {
    Expression expression = Expressions.equal(IcebergTableSchema.STORAGE_PLUGIN, tableKey.storagePlugin());
    expression = Expressions.and(expression, Expressions.equal(IcebergTableSchema.WORKSPACE, tableKey.workspace()));
    expression = Expressions.and(expression, Expressions.equal(IcebergTableSchema.TABLE_NAME, tableKey.tableName()));
    expression = Expressions.and(expression, Expressions.equal(IcebergTableSchema.METADATA_KEY, metadataKey));
    return expression;
  }
}
