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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers;

import java.util.List;

public class DrillSubstringFuncHolder extends DrillSimpleFuncHolder {

  public DrillSubstringFuncHolder(FunctionAttributes functionAttributes, FunctionInitializer initializer) {
    super(functionAttributes, initializer);
  }

  @Override
  public TypeProtos.MajorType getReturnType(List<LogicalExpression> logicalExpressions) {
    TypeProtos.MajorType.Builder builder = TypeProtos.MajorType.newBuilder()
        .setMinorType(getReturnType().getMinorType())
        .setMode(getReturnTypeDataMode(logicalExpressions));

    int sourceLength = logicalExpressions.get(0).getMajorType().hasPrecision() ?
        logicalExpressions.get(0).getMajorType().getPrecision() : Types.MAX_VARCHAR_LENGTH;

    boolean offsetOnly = false;
    if (logicalExpressions.size() == 2) {
      if (logicalExpressions.get(1).iterator().hasNext()
          && logicalExpressions.get(1).iterator().next() instanceof ValueExpressions.IntExpression) {
        // substring(source, offset)
        offsetOnly = true;
      } else {
        // substring(source, regexp)
        return builder.setPrecision(sourceLength).build();
      }
    }

    // calculate start & end
    int offset = ((ValueExpressions.IntExpression) logicalExpressions.get(1).iterator().next()).getInt();
    int length = Types.MAX_VARCHAR_LENGTH;

    if (!offsetOnly) {
      if (logicalExpressions.get(2).iterator().hasNext()
          && logicalExpressions.get(2).iterator().next() instanceof ValueExpressions.IntExpression) {
        // substring(source, offset, length)
        length = ((ValueExpressions.IntExpression) logicalExpressions.get(2).iterator().next()).getInt();
      } else {
        // we could not define length parameter, return initial return type
        return builder.build();
      }
    }

    int targetLength = StringFunctionHelpers.calculateSubstringLength(sourceLength, offset, length, !offsetOnly);
    return builder.setPrecision(targetLength).build();
  }
}