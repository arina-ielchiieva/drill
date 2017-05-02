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
import org.apache.drill.exec.expr.fn.output.ReturnTypeInference;

import java.util.List;

/**
 * Function holder for functions with function scope set as
 * {@link org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope#SUBSTRING}.
 */
public class DrillSubstringFuncHolder extends DrillSimpleFuncHolder {


  public DrillSubstringFuncHolder(FunctionAttributes functionAttributes, FunctionInitializer initializer, ReturnTypeInference returnTypeInference) {
    super(functionAttributes, initializer, returnTypeInference);
  }

  /**
   * Defines function return type and calculates output precision.
   * <p/>
   * <b>substring(source, regexp)</b>
   * <ul><li>If input precision is known, output precision is max varchar value {@link Types#MAX_VARCHAR_LENGTH}.</li></ul>
   *
   * <b>substring(source, offset)</b>
   * <ul>
   * <li>If input precision is unknown then output precision is max varchar value {@link Types#MAX_VARCHAR_LENGTH}.</li>
   * <li>If input precision is known, output precision is input precision minus offset plus 1
   * since offset starts from 1.</li>
   * <li>If offset value is greater than input precision or offset value is corrupted (less then equals zero),
   * output precision is zero.</li>
   * </ul>
   *
   * <b>substring(source, offset, length)</b>
   * <ul>
   * <li>If offset value is greater than input precision or offset or length values are corrupted (less then equals zero),
   * output precision is zero.</li>
   * <li>If source length (including offset) is less than substring length, output precision is source length (including offset).</li>
   * <li>If source length (including offset) is greater than substring length, output precision is substring length.</li>
   * </ul>
   *
   * @param logicalExpressions logical expressions
   * @return function return type
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

    // to calculate target length both operands should be numeric
    if (!offsetOnly && !(logicalExpressions.get(1).iterator().hasNext() &&
        logicalExpressions.get(1).iterator().next() instanceof ValueExpressions.IntExpression &&
        logicalExpressions.get(2).iterator().hasNext() &&
        logicalExpressions.get(2).iterator().next() instanceof ValueExpressions.IntExpression)) {
      // could not define one of the operands, return source length
      return builder.setPrecision(sourceLength).build();
    }

    int offset = ((ValueExpressions.IntExpression) logicalExpressions.get(1).iterator().next()).getInt();
    int length = offsetOnly ? -1 : ((ValueExpressions.IntExpression) logicalExpressions.get(2).iterator().next()).getInt();
    int targetLength = StringFunctionHelpers.calculateSubstringLength(sourceLength, offset, length, !offsetOnly);
    return builder.setPrecision(targetLength).build();
  }
}