/*
 * ****************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ****************************************************************************
 */
package org.apache.drill.exec.expr.fn.output;

import com.google.common.collect.Sets;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;
import org.apache.drill.exec.expr.fn.FunctionAttributes;

import java.util.List;
import java.util.Set;

public class DefaultReturnType implements ReturnTypeInference {

  public static final DefaultReturnType INSTANCE = new DefaultReturnType();

/*  private final DrillFuncHolder.ValueReference returnValue;
  private final FunctionTemplate.NullHandling nullHandling;
  private final DrillFuncHolder.ValueReference[] parameters;

  public AbstractReturnType(DrillFuncHolder.ValueReference returnValue,
                            FunctionTemplate.NullHandling nullHandling,
                            DrillFuncHolder.ValueReference[] parameters) {
    this.returnValue = returnValue;
    this.nullHandling = nullHandling;
    this.parameters = parameters;
  }

  public TypeProtos.MajorType getReturnType() {
    return returnValue.getType();
  }*/

  public TypeProtos.MajorType getReturnType(final List<LogicalExpression> logicalExpressions, FunctionAttributes attributes) {
    if (attributes.getReturnValue().getType().getMinorType() == TypeProtos.MinorType.UNION) {
      final Set<TypeProtos.MinorType> subTypes = Sets.newHashSet();
      for (final DrillFuncHolder.ValueReference ref : attributes.getParameters()) {
        subTypes.add(ref.getType().getMinorType());
      }

      final TypeProtos.MajorType.Builder builder = TypeProtos.MajorType.newBuilder()
          .setMinorType(TypeProtos.MinorType.UNION)
          .setMode(TypeProtos.DataMode.OPTIONAL);

      for (final TypeProtos.MinorType subType : subTypes) {
        builder.addSubType(subType);
      }
      return builder.build();
    }
    return attributes.getReturnValue().getType().toBuilder()
        .setMode(FunctionUtils.getReturnTypeDataMode(logicalExpressions, attributes))
        .build();
  }

}
