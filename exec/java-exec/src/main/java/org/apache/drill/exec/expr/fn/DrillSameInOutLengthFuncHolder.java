/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.drill.common.types.Types;
import org.apache.drill.common.util.CoreDecimalUtility;

import java.util.List;

public class DrillSameInOutLengthFuncHolder extends DrillSimpleFuncHolder {

  public DrillSameInOutLengthFuncHolder(FunctionAttributes functionAttributes, FunctionInitializer initializer) {
    super(functionAttributes, initializer);
  }

  @Override
  public TypeProtos.MajorType getReturnType(List<LogicalExpression> logicalExpressions) {
    TypeProtos.MajorType majorType = logicalExpressions.get(0).getMajorType();

    TypeProtos.MajorType.Builder builder = TypeProtos.MajorType.newBuilder()
        .setMinorType(getReturnType().getMinorType())
        .setMode(getReturnTypeDataMode(logicalExpressions));

    if (Types.isScalarStringType(majorType) || CoreDecimalUtility.isDecimalType(majorType)) {
      if (majorType.hasPrecision()) {
        builder.setPrecision(majorType.getPrecision());
      }
    }

    if (CoreDecimalUtility.isDecimalType(majorType)) {
      if (majorType.hasScale()) {
        builder.setScale(majorType.getScale());
      }
    }

    return builder.build();
  }
}
