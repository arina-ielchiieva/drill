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
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers;

import java.util.List;

/**
 * Function holder for functions with function scope set as
 * {@link org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope#STRING_LEFT_RIGHT}.
 */
public class DrillStringLeftRightFuncHolder extends DrillSimpleFuncHolder  {

  public DrillStringLeftRightFuncHolder(FunctionAttributes functionAttributes, FunctionInitializer initializer) {
    super(functionAttributes, initializer);
  }

  /**
   * Defines function return type and calculates output precision.
   * Target length calculation logic for left and right functions is the same,
   * they substring string the same way, just from different sides of the string.
   * <br/>
   * <b>left(source, length)</b><br/>
   * <b>right(source, length)</b>
   *
   * <ul>
   * <li>If length is positive, target length is given length.</li>
   * <li>If length is negative, target length is source length minus given length.
   * If after substraction length is negative, target length is 0.</li>
   * <li>If length is 0, target length is 0.</li>
   * </ul>
   *
   * @param logicalExpressions logical expressions
   * @return return type
   */
  @Override
  public TypeProtos.MajorType getReturnType(List<LogicalExpression> logicalExpressions) {
    TypeProtos.MajorType.Builder builder = TypeProtos.MajorType.newBuilder()
        .setMinorType(getReturnType().getMinorType())
        .setMode(getReturnTypeDataMode(logicalExpressions));

    if (!logicalExpressions.get(0).getMajorType().hasPrecision()) {
      return builder.build();
    }

    int sourceLength = logicalExpressions.get(0).getMajorType().getPrecision();

    if (logicalExpressions.get(1).iterator().hasNext()
        && logicalExpressions.get(1).iterator().next() instanceof ValueExpressions.IntExpression) {
      int length = ((ValueExpressions.IntExpression) logicalExpressions.get(1).iterator().next()).getInt();
      int targetLength = StringFunctionHelpers.calculateStringLeftRightLength(sourceLength, length);
      return builder.setPrecision(targetLength).build();
    }

    return builder.setPrecision(sourceLength).build();
  }
}
