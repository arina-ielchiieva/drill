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
import java.util.List;

public class DrillConcatOperatorFuncHolder extends DrillSimpleFuncHolder {

  public DrillConcatOperatorFuncHolder(FunctionAttributes functionAttributes, FunctionInitializer initializer) {
    super(functionAttributes, initializer);
  }

  @Override
  public TypeProtos.MajorType getReturnType(List<LogicalExpression> logicalExpressions) {
    TypeProtos.MajorType returnType = super.getReturnType(logicalExpressions);
    if (Types.isStringScalarType(returnType)) {
      int totalPrecision = 0;
      boolean setPrecision = true;
      for (LogicalExpression expression : logicalExpressions) {
        if (expression.getMajorType().hasPrecision()) {
          totalPrecision += expression.getMajorType().getPrecision();
        } else {
          setPrecision = false;
          break;
        }

        /*else if (precision == 65536) {
          totalPrecision = 65536;
          break;
        } else {
          totalPrecision += precision;
        }
        if (totalPrecision >= 65536) {
          totalPrecision = 65536;
          break;
        }*/
      }
      if (setPrecision) {
        return Types.withPrecision(returnType.getMinorType(), returnType.getMode(), totalPrecision);
      }
    }
    return returnType;
  }
}