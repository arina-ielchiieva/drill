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
import org.apache.drill.metastore.iceberg.transform.OperationTransformer;
import org.apache.iceberg.Transaction;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of {@link Metastore.Modify} interface.
 * Modifies information in Iceberg table based on given overwrite or delete operations.
 * Executes given operations in one transaction.
 */
public class IcebergModify implements Metastore.Modify {

  private final MetastoreContext context;
  private final List<MetadataUnit> overwriteUnits = new ArrayList<>();
  private final List<FilterExpression> deleteFilters = new ArrayList<>();
  private boolean purge = false;

  public IcebergModify(MetastoreContext context) {
    this.context = context;
  }

  @Override
  public Metastore.Modify overwrite(List<MetadataUnit> units) {
    overwriteUnits.addAll(units);
    return this;
  }

  @Override
  public Metastore.Modify delete(FilterExpression filter) {
    deleteFilters.add(filter);
    return this;
  }

  @Override
  public Metastore.Modify purge() {
    purge = true;
    return this;
  }

  @Override
  public void execute() {
    OperationTransformer transformer = context.transform().operation();
    List<IcebergOperation> operations = new ArrayList<>(transformer.toOverwrite(overwriteUnits));
    operations.addAll(transformer.toDelete(deleteFilters));

    if (purge) {
      operations.add(transformer.toDelete((FilterExpression) null));
    }

    if (operations.isEmpty()) {
      return;
    }

    Transaction transaction = context.metastore().newTransaction();
    operations.forEach(op -> op.add(transaction));
    transaction.commitTransaction();
  }
}
