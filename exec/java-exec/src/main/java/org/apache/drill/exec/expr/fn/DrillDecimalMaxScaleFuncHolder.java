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
package org.apache.drill.exec.expr.fn;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MajorType;

import java.util.List;

public class DrillDecimalMaxScaleFuncHolder extends DrillSimpleFuncHolder {

  public DrillDecimalMaxScaleFuncHolder(FunctionAttributes functionAttributes, FunctionInitializer initializer) {
    super(functionAttributes, initializer);
  }

  @Override
  public MajorType getReturnType(List<LogicalExpression> args) {

    TypeProtos.DataMode mode = getReturnTypeDataMode(args);
    int scale = 0;
    int precision = 0;

    for (LogicalExpression e : args) {
      scale = Math.max(scale, e.getMajorType().getScale());
      precision = Math.max(precision, e.getMajorType().getPrecision());
    }

    return TypeProtos.MajorType.newBuilder()
        .setMinorType(getReturnType().getMinorType()).setScale(scale).setPrecision(precision).setMode(mode).build();
  }
}
