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

import java.util.Objects;

public class ColumnDef {

  private final String name;
  private final String alias;
  private final TypeDef typeDef;
  private final boolean nullable;
  private final int index;

  private ColumnDef(String name, String alias, TypeDef typeDef, boolean nullable, int index) {
    this.name = name;
    this.alias = alias;
    this.typeDef = typeDef;
    this.nullable = nullable;
    this.index = index;
  }

  public String getName() {
    return name;
  }

  public String getAlias() {
    return alias;
  }

  public TypeDef getTypeDef() {
    return typeDef;
  }

  public boolean isNullable() {
    return nullable;
  }

  public int getIndex() {
    return index;
  }

  public boolean hasIndex() {
    return index != -1;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, alias, typeDef, nullable, index);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ColumnDef columnDef = (ColumnDef) o;
    return nullable == columnDef.nullable
      && index == columnDef.index
      && Objects.equals(name, columnDef.name)
      && Objects.equals(alias, columnDef.alias)
      && Objects.equals(typeDef, columnDef.typeDef);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();

    builder.append("`").append(escapeSpecialSymbols(name)).append("`");

    if (hasIndex()) {
      builder.append("[").append(index).append("]");
    }

    builder.append(" ").append(typeDef.toString());

    if (!nullable) {
      builder.append(" NOT NULL");
    }

    if (alias != null) {
      builder.append(" AS `").append(escapeSpecialSymbols(alias)).append("`");
    }

    return builder.toString();
  }

  private String escapeSpecialSymbols(String value) {
    return value.replaceAll("(\\\\)|(`)", "\\\\$0");
}

  public static class Builder {

    private String name;
    private String alias;
    private TypeDef typeDef;
    private boolean nullable;
    private int index = -1;

    public static Builder builder() {
      return new Builder();
    }

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setAlias(String alias) {
      this.alias = alias;
      return this;
    }

    public Builder setTypeDef(TypeDef typeDef) {
      this.typeDef = typeDef;
      return this;
    }

    public Builder setNullability(boolean nullable) {
      this.nullable = nullable;
      return this;
    }

    public  Builder setIndex(int index) {
      this.index = index;
      return this;
    }

    public ColumnDef build() {
      return new ColumnDef(name, alias, typeDef, nullable, index);
    }

  }

}
