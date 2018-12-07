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
package org.apache.drill.common.schema;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TypeDef {

  private final TypeProtos.MajorType type;
  private final TypeDef subType;
  private final List<ColumnDef> subColumns;

  public TypeDef(TypeProtos.MajorType type,
                  TypeDef subType,
                  List<ColumnDef> subColumns) {
    this.type = type;
    this.subType = subType;
    this.subColumns = subColumns;
  }

  public static TypeDef createSimpleType(TypeProtos.MinorType minorType) {
    return new TypeDef(getSimpleTypeBuilder(minorType).build(), null, null);
  }

  public static TypeDef createSimpleType(TypeProtos.MinorType minorType, boolean nullable) {
    return new TypeDef(getSimpleTypeBuilder(minorType)
      .setMode(nullable ? TypeProtos.DataMode.OPTIONAL : TypeProtos.DataMode.REQUIRED)
      .build(), null, null);
  }

  public static TypeDef createSimpleType(TypeProtos.MinorType minorType, int precision) {
    TypeProtos.MajorType type = getSimpleTypeBuilder(minorType).setPrecision(precision).build();
    return new TypeDef(type, null, null);
  }

  public static TypeDef createSimpleType(TypeProtos.MinorType minorType, int precision, int scale) {
    TypeProtos.MajorType type = getSimpleTypeBuilder(minorType).setPrecision(precision).setScale(scale).build();
    return new TypeDef(type, null, null);
  }

  private static TypeProtos.MajorType.Builder getSimpleTypeBuilder(TypeProtos.MinorType minorType) {
    if (TypeProtos.MinorType.LIST == minorType
      || TypeProtos.MinorType.MAP == minorType) {
      throw new DrillRuntimeException("Cannot create simple type for: " + minorType);
    }
    return TypeProtos.MajorType.newBuilder().setMinorType(minorType).setMode(TypeProtos.DataMode.OPTIONAL);
  }

  public static TypeDef createArrayType(TypeDef subType) {
    if (subType == null) {
      throw new DrillRuntimeException("Cannot create array type without sub type.");
    }
    return new TypeDef(Types.repeated(TypeProtos.MinorType.LIST), subType, null);
  }

  public static TypeDef createMapType(List<ColumnDef> subColumns) {
    if (subColumns == null || subColumns.isEmpty()) {
      throw new DrillRuntimeException("Cannot create map type without sub columns.");
    }
    return new TypeDef(Types.repeated(TypeProtos.MinorType.MAP), null, subColumns);
  }

  public TypeDef cloneWithNullability(boolean nullable) {
    if (type.hasMode() && TypeProtos.DataMode.REPEATED == type.getMode()) {
      throw new DrillRuntimeException(String.format("Cannot clone complex type with nullability: " +
        "type [%s], nullable: [%s].", type.getMode(), nullable));
    }
    TypeProtos.MajorType updType = type.toBuilder()
      .setMode(nullable ? TypeProtos.DataMode.OPTIONAL : TypeProtos.DataMode.REQUIRED)
      .build();
    return new TypeDef(updType, subType, subColumns);
  }

  public TypeProtos.MajorType getType() {
    return type;
  }

  public TypeDef getSubType() {
    return subType;
  }

  public List<ColumnDef> getSubColumns() {
    return new ArrayList<>(subColumns);
  }

  public boolean isArray() {
    return subType != null;
  }

  public boolean isMap() {
    return subColumns != null;
  }

  public boolean isComplex() {
    return isArray() || isMap();
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, subType, subColumns);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TypeDef typeDef = (TypeDef) o;
    return Objects.equals(type, typeDef.type)
      && Objects.equals(subType, typeDef.subType)
      && Objects.equals(subColumns, typeDef.subColumns);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();

    switch (type.getMinorType()) {
      case VARDECIMAL:
        builder.append("DECIMAL");
        break;
      case FLOAT4:
        builder.append("FLOAT");
        break;
      case FLOAT8:
        builder.append("DOUBLE");
        break;
      case BIT:
        builder.append("BOOLEAN");
        break;
      case LIST:
        builder.append("ARRAY");
        break;
      default:
        builder.append(type.getMinorType().name());
    }

    if (isArray()) {
      builder.append("<").append(subType.toString()).append(">");
    } else if (isMap()) {
      builder.append("<");
      builder.append(subColumns.stream()
        .map(ColumnDef::toString)
        .collect(Collectors.joining(", ")));
      builder.append(">");
    } else {
      if (type.hasPrecision()) {
        builder.append("(").append(type.getPrecision());
        if (type.hasScale()) {
          builder.append(", ").append(type.getScale());
        }
        builder.append(")");
      }
    }

    return builder.toString();
  }
}
