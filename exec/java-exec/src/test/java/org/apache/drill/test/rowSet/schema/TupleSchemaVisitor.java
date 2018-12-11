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
package org.apache.drill.test.rowSet.schema;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.drill.common.parser.CaseChangingCharStream;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.AbstractColumnMetadata;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.TupleSchema;
import org.apache.drill.exec.record.metadata.parser.TupleSchemaLexer;
import org.apache.drill.exec.record.metadata.parser.TupleSchemaParser;
import org.apache.drill.exec.record.metadata.parser.TupleSchemaParserBaseVisitor;

import java.util.List;
import java.util.stream.Collectors;

public class TupleSchemaVisitor extends TupleSchemaParserBaseVisitor<TupleSchema> {

  @Override
  public TupleSchema visitSchema(TupleSchemaParser.SchemaContext ctx) {
    TupleSchema schema = new TupleSchema();
    ctx.columns().accept(new ColumnsVisitor()).forEach(schema::addColumn);
    return schema;
  }

  public static class ColumnsVisitor extends TupleSchemaParserBaseVisitor<List<ColumnMetadata>> {

    @Override
    public List<ColumnMetadata> visitColumns(TupleSchemaParser.ColumnsContext ctx) {
      ColumnVisitor columnVisitor = new ColumnVisitor();
      return ctx.column().stream()
        .map(c -> c.accept(columnVisitor))
        .collect(Collectors.toList());
    }
  }

  public static class ColumnVisitor extends TupleSchemaParserBaseVisitor<ColumnMetadata> {

    @Override
    public ColumnMetadata visitPrimitive_column(TupleSchemaParser.Primitive_columnContext ctx) {
      String name = ctx.column_id().accept(new NameVisitor());
      boolean nullable = ctx.nullability() == null;

      TypeProtos.MajorType type;
      if (ctx.simple_type() != null) {
        type = ctx.simple_type().accept(new SimpleTypeVisitor());
        type = type.toBuilder()
          .setMode(nullable ? TypeProtos.DataMode.OPTIONAL : TypeProtos.DataMode.REQUIRED)
          .build();
      } else {
        type = ctx.simple_arr().simple_type().accept(new SimpleTypeVisitor());
        type = type.toBuilder().setMode(TypeProtos.DataMode.REPEATED).build(); //todo make separate column type?
      }

      MaterializedField field = MaterializedField.create(name, type);
      return MetadataUtils.fromField(field);
    }

    @Override
    public ColumnMetadata visitMap_column(TupleSchemaParser.Map_columnContext ctx) {
      String name = ctx.column_id().accept(new NameVisitor());
      TypeProtos.DataMode mode = ctx.nullability() == null ? TypeProtos.DataMode.OPTIONAL : TypeProtos.DataMode.REQUIRED;
      MapBuilder builder = new MapBuilder(null, name, mode);

      ColumnVisitor visitor = new ColumnVisitor();
      ctx.columns().column().forEach(
        c -> builder.addColumn((AbstractColumnMetadata) c.accept(visitor))
      );

      return builder.buildCol();
    }

    @Override
    public ColumnMetadata visitRepeated_list(TupleSchemaParser.Repeated_listContext ctx) {
      String name = ctx.column_id().accept(new NameVisitor());
      RepeatedListBuilder builder = new RepeatedListBuilder(null, name);
      ColumnMetadata child = ctx.complex_type().accept(new RepeatedListDataType(name, builder));
      builder.addColumn((AbstractColumnMetadata) child);
      return builder.buildCol();
    }
  }

  public static class RepeatedListDataType extends TupleSchemaParserBaseVisitor<ColumnMetadata> {

    private final String name;
    private final RepeatedListBuilder builder;

    public RepeatedListDataType(String name, RepeatedListBuilder builder) {
      this.name = name;
      this.builder = builder;
    }

    @Override
    public ColumnMetadata visitSimple_array(TupleSchemaParser.Simple_arrayContext ctx) {
      TypeProtos.MajorType type = ctx.simple_arr().simple_type().accept(new SimpleTypeVisitor());
      type = type.toBuilder().setMode(TypeProtos.DataMode.REPEATED).build();
      MaterializedField field = MaterializedField.create(name, type);
      return MetadataUtils.fromField(field);
    }

    @Override
    public ColumnMetadata visitComplex_array(TupleSchemaParser.Complex_arrayContext ctx) {
      RepeatedListBuilder childBuilder = new RepeatedListBuilder(builder, name);
      ColumnMetadata child = ctx.complex_type().accept(new RepeatedListDataType(name, childBuilder));
      builder.addColumn((AbstractColumnMetadata) child);
      return builder.buildCol();
    }

    @Override
    public ColumnMetadata visitMap(TupleSchemaParser.MapContext ctx) {
      MapBuilder builder = new MapBuilder(null, name, TypeProtos.DataMode.REPEATED);

      ColumnVisitor visitor = new ColumnVisitor();
      ctx.columns().column().forEach(
        c -> builder.addColumn((AbstractColumnMetadata) c.accept(visitor))
      );

      return builder.buildCol();
    }
  }

  public static class SimpleTypeVisitor extends TupleSchemaParserBaseVisitor<TypeProtos.MajorType> {

    @Override
    public TypeProtos.MajorType visitInt(TupleSchemaParser.IntContext ctx) {
      return Types.optional(TypeProtos.MinorType.INT);
    }

    @Override
    public TypeProtos.MajorType visitBigint(TupleSchemaParser.BigintContext ctx) {
      return Types.optional(TypeProtos.MinorType.BIGINT);
    }

    @Override
    public TypeProtos.MajorType visitFloat(TupleSchemaParser.FloatContext ctx) {
      return Types.optional(TypeProtos.MinorType.FLOAT4);
    }

    @Override
    public TypeProtos.MajorType visitDouble(TupleSchemaParser.DoubleContext ctx) {
      return Types.optional(TypeProtos.MinorType.FLOAT8);
    }

    @Override
    public TypeProtos.MajorType visitDecimal(TupleSchemaParser.DecimalContext ctx) {
      TypeProtos.MajorType type = Types.optional(TypeProtos.MinorType.VARDECIMAL);

      List<TerminalNode> numbers = ctx.NUMBER();

      if (numbers.isEmpty()) {
        return type;
      }

      int precision = Integer.parseInt(numbers.get(0).getText());
      int scale = numbers.size() == 2 ? Integer.parseInt(numbers.get(1).getText()) : 0;

      return type.toBuilder().setPrecision(precision).setScale(scale).build();
    }

    @Override
    public TypeProtos.MajorType visitBoolean(TupleSchemaParser.BooleanContext ctx) {
      return Types.optional(TypeProtos.MinorType.BIT);
    }

    @Override
    public TypeProtos.MajorType visitVarchar(TupleSchemaParser.VarcharContext ctx) {
      TypeProtos.MajorType type = Types.optional(TypeProtos.MinorType.VARCHAR);

      if (ctx.NUMBER() != null) {
        type = type.toBuilder().setPrecision(Integer.parseInt(ctx.NUMBER().getText())).build();
      }

      return type;
    }

    @Override
    public TypeProtos.MajorType visitBinary(TupleSchemaParser.BinaryContext ctx) {
      TypeProtos.MajorType type = Types.optional(TypeProtos.MinorType.VARBINARY);

      if (ctx.NUMBER() != null) {
        type = type.toBuilder().setPrecision(Integer.parseInt(ctx.NUMBER().getText())).build();
      }

      return type;
    }

    @Override
    public TypeProtos.MajorType visitTime(TupleSchemaParser.TimeContext ctx) {
      return Types.optional(TypeProtos.MinorType.TIME);
    }

    @Override
    public TypeProtos.MajorType visitDate(TupleSchemaParser.DateContext ctx) {
      return Types.optional(TypeProtos.MinorType.DATE);
    }

    @Override
    public TypeProtos.MajorType visitTimestamp(TupleSchemaParser.TimestampContext ctx) {
      return Types.optional(TypeProtos.MinorType.TIMESTAMP);
    }

    @Override
    public TypeProtos.MajorType visitInterval(TupleSchemaParser.IntervalContext ctx) {
      return Types.optional(TypeProtos.MinorType.INTERVAL);
    }
  }

  public static class NameVisitor extends TupleSchemaParserBaseVisitor<String> {

    @Override
    public String visitId(TupleSchemaParser.IdContext ctx) {
      return ctx.ID().getText();
    }

    @Override
    public String visitQuoted_id(TupleSchemaParser.Quoted_idContext ctx) {
      String text = ctx.QUOTED_ID().getText();
      // first substring first and last symbols (backticks)
      // then find all chars that are preceding with the backslash and remove the backslash
      return text.substring(1, text.length() -1).replaceAll("\\\\(.)", "$1");
    }
  }

  public static void main(String[] args) {
    // String schemaString = "col1 array<int>, col2 int not null, " + "col3 map<m1 int, m2 array<int>, m3 map<mm1 int not null>> not null";
    //String schemaString = "col1 array<array<int>>";
    //String schemaString = "col1 array<map<m1 int>>";
    String schemaString = "col1 array<array<array<decimal(5, 2)>>>";
    //String schemaString = "col1 array<array<map<m1 int not null>>>";

    CodePointCharStream stream = CharStreams.fromString(schemaString);
    CaseChangingCharStream upperCaseStream = new CaseChangingCharStream(stream, true);
    TupleSchemaLexer lexer = new TupleSchemaLexer(upperCaseStream);

    CommonTokenStream tokens = new CommonTokenStream(lexer);
    TupleSchemaParser parser = new TupleSchemaParser(tokens);
    TupleSchemaVisitor visitor = new TupleSchemaVisitor();
    TupleSchema schema = visitor.visitSchema(parser.schema());
    System.out.println(schema);
  }

  // todo we need to produce schema string from TupleMetadata

}
