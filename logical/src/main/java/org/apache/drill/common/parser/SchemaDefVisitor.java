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
package org.apache.drill.common.parser;

import org.apache.drill.common.schema.ColumnDef;
import org.apache.drill.common.schema.SchemaDef;
import org.apache.drill.common.schema.TypeDef;
import org.apache.drill.common.schema.parser.SchemaParser;
import org.apache.drill.common.schema.parser.SchemaParserBaseVisitor;
import org.apache.drill.common.types.TypeProtos;

import java.util.List;
import java.util.stream.Collectors;

public class SchemaDefVisitor extends SchemaParserBaseVisitor<SchemaDef> {

  @Override
  public SchemaDef visitSchema(SchemaParser.SchemaContext ctx) {
    return new SchemaDef(ctx.columns().accept(new ColumnListVisitor()));
  }

  public static class ColumnListVisitor extends SchemaParserBaseVisitor<List<ColumnDef>> {

    @Override
    public List<ColumnDef> visitNamed_columns(SchemaParser.Named_columnsContext ctx) {
      ColumnDefVisitor columnDefVisitor = new ColumnDefVisitor();
      return ctx.named_column().stream()
        .map(c -> c.accept(columnDefVisitor))
        .collect(Collectors.toList());
    }

    @Override
    public List<ColumnDef> visitIndexed_columns(SchemaParser.Indexed_columnsContext ctx) {
      ColumnDefVisitor columnDefVisitor = new ColumnDefVisitor();
      return ctx.indexed_column().stream()
        .map(c -> c.accept(columnDefVisitor))
        .collect(Collectors.toList());
    }
  }

  public static class ColumnDefVisitor extends SchemaParserBaseVisitor<ColumnDef> {

    @Override
    public ColumnDef visitNamed_column(SchemaParser.Named_columnContext ctx) {
      ColumnDef.Builder builder = initColumnDefBuilder(ctx.column_attr());
      builder.setName(ctx.named_column_def().accept(new NameVisitor()));
      return builder.build();
    }

    @Override
    public ColumnDef visitIndexed_column(SchemaParser.Indexed_columnContext ctx) {
      ColumnDef.Builder builder = initColumnDefBuilder(ctx.column_attr());
      builder.setName(ctx.indexed_column_def().COLUMNS().getText().replaceAll("`", "").toUpperCase());
      builder.setIndex(Integer.parseInt(ctx.indexed_column_def().NUMBER().getText()));
      return builder.build();
    }

    private ColumnDef.Builder initColumnDefBuilder(SchemaParser.Column_attrContext ctx) {
      ColumnDef.Builder builder = ColumnDef.Builder.builder();

      boolean nullable = ctx.nullability() == null;
      builder.setNullability(nullable);

      TypeDef typeDef = ctx.type_def().accept(new TypeDefVisitor());
      // update type mode if not set
      if (!typeDef.getType().hasMode() || TypeProtos.DataMode.REPEATED != typeDef.getType().getMode()) {
        typeDef = typeDef.cloneWithNullability(nullable);
      }
      builder.setTypeDef(typeDef);

      if (ctx.alias() != null) {
        builder.setAlias(ctx.alias().accept(new NameVisitor()));
      }

      return builder;
    }
  }

  public static class NameVisitor extends SchemaParserBaseVisitor<String> {

    @Override
    public String visitId(SchemaParser.IdContext ctx) {
      return ctx.ID().getText();
    }

    @Override
    public String visitQuoted_id(SchemaParser.Quoted_idContext ctx) {
      String text = ctx.QUOTED_ID().getText();
      // first substring first and last symbols (backticks)
      // then find all chars that are preceding with the backslash and remove the backslash
      return text.substring(1, text.length() -1).replaceAll("\\\\(.)", "$1");
    }

  }

  public static class TypeDefVisitor extends SchemaParserBaseVisitor<TypeDef> {

    @Override
    public TypeDef visitInt(SchemaParser.IntContext ctx) {
      return TypeDef.createSimpleType(TypeProtos.MinorType.INT);
    }

    @Override
    public TypeDef visitBigint(SchemaParser.BigintContext ctx) {
      return TypeDef.createSimpleType(TypeProtos.MinorType.BIGINT);
    }

    @Override
    public TypeDef visitFloat(SchemaParser.FloatContext ctx) {
      return TypeDef.createSimpleType(TypeProtos.MinorType.FLOAT4);
    }

    @Override
    public TypeDef visitDouble(SchemaParser.DoubleContext ctx) {
      return TypeDef.createSimpleType(TypeProtos.MinorType.FLOAT8);
    }

    @Override
    public TypeDef visitDecimal(SchemaParser.DecimalContext ctx) {
      TypeProtos.MinorType minorType = TypeProtos.MinorType.VARDECIMAL;
      if (ctx.NUMBER().size() > 1) {
        return TypeDef.createSimpleType(minorType, Integer.parseInt(ctx.NUMBER(0).getText()),
          Integer.parseInt(ctx.NUMBER(1).getText()));
      } else if (ctx.NUMBER().size() > 0) {
        return TypeDef.createSimpleType(minorType, Integer.parseInt(ctx.NUMBER(0).getText()));
      } else {
        return TypeDef.createSimpleType(minorType);
      }
    }

    @Override
    public TypeDef visitBoolean(SchemaParser.BooleanContext ctx) {
      return TypeDef.createSimpleType(TypeProtos.MinorType.BIT);
    }

    @Override
    public TypeDef visitVarchar(SchemaParser.VarcharContext ctx) {
      TypeProtos.MinorType minorType = TypeProtos.MinorType.VARCHAR;
      if (ctx.NUMBER() != null) {
        return TypeDef.createSimpleType(minorType, Integer.parseInt(ctx.NUMBER().getText()));
      }
      return TypeDef.createSimpleType(minorType);
    }

    @Override
    public TypeDef visitBinary(SchemaParser.BinaryContext ctx) {
      TypeProtos.MinorType minorType = TypeProtos.MinorType.VARBINARY;
      if (ctx.NUMBER() != null) {
        return TypeDef.createSimpleType(minorType, Integer.parseInt(ctx.NUMBER().getText()));
      }
      return TypeDef.createSimpleType(minorType);
    }

    @Override
    public TypeDef visitTime(SchemaParser.TimeContext ctx) {
      return TypeDef.createSimpleType(TypeProtos.MinorType.TIME);
    }

    @Override
    public TypeDef visitDate(SchemaParser.DateContext ctx) {
      return TypeDef.createSimpleType(TypeProtos.MinorType.DATE);
    }

    @Override
    public TypeDef visitTimestamp(SchemaParser.TimestampContext ctx) {
      return TypeDef.createSimpleType(TypeProtos.MinorType.TIMESTAMP);
    }

    @Override
    public TypeDef visitInterval(SchemaParser.IntervalContext ctx) {
      return TypeDef.createSimpleType(TypeProtos.MinorType.INTERVAL);
    }

    @Override
    public TypeDef visitArray_def(SchemaParser.Array_defContext ctx) {
      TypeDef subType = super.visit(ctx.type_def());
      return TypeDef.createArrayType(subType);
    }

    @Override
    public TypeDef visitMap_def(SchemaParser.Map_defContext ctx) {
      ColumnDefVisitor columnDefVisitor = new ColumnDefVisitor();
      List<ColumnDef> subColumns = ctx.named_columns().named_column().stream()
        .map(c -> c.accept(columnDefVisitor))
        .collect(Collectors.toList());
      return TypeDef.createMapType(subColumns);
    }

  }

}
