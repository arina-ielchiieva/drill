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

import org.apache.drill.metastore.iceberg.MetastoreContext;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;

/**
 * Provides tools to transform filters, operations and data.
 */
public class Transform {

  private final MetastoreContext context;

  public Transform(MetastoreContext context) {
    this.context = context;
  }

  public FilterTransformer filter() {
    return new FilterTransformer();
  }

  public DataTransformer data() {
    Table metastore = context.metastore();
    return new DataTransformer(metastore.schema(), new Schema(metastore.spec().partitionType().fields()));
  }

  public OperationTransformer operation() {
    return new OperationTransformer(context);
  }
}
