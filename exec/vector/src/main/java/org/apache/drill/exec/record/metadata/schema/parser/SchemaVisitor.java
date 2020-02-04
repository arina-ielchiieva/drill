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
package org.apache.drill.exec.record.metadata.schema.parser;

import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.DictBuilder;
import org.apache.drill.exec.record.metadata.MapBuilder;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.RepeatedListBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.metadata.TupleSchema;
import org.apache.drill.exec.record.metadata.VariantColumnMetadata;
import org.apache.drill.exec.record.metadata.VariantSchema;
import org.apache.drill.exec.vector.complex.DictVector;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Visits schema and stores metadata about its columns into {@link TupleMetadata} class.
 */
public class SchemaVisitor extends SchemaParserBaseVisitor<TupleMetadata> {

  @Override
  public TupleMetadata visitSchema(SchemaParser.SchemaContext ctx) {
    TupleMetadata schema = ctx.columns() == null ? new TupleSchema() : visitColumns(ctx.columns());
    if (ctx.property_values() != null) {
      PropertiesVisitor propertiesVisitor = new PropertiesVisitor();
      schema.setProperties(ctx.property_values().accept(propertiesVisitor));
    }
    return schema;
  }

  @Override
  public TupleMetadata visitColumns(SchemaParser.ColumnsContext ctx) {
    TupleMetadata schema = new TupleSchema();
    ColumnDefVisitor columnDefVisitor = new ColumnDefVisitor();
    ctx.column_def().stream()
      .map(columnDef -> columnDef.accept(columnDefVisitor))
      .forEach(schema::addColumn);
    return schema;
  }

  /**
   * Visits column definition, adds column properties to {@link ColumnMetadata} if present.
   */
  public static class ColumnDefVisitor extends SchemaParserBaseVisitor<ColumnMetadata> {

    @Override
    public ColumnMetadata visitColumn_def(SchemaParser.Column_defContext ctx) {
      ColumnVisitor columnVisitor = new ColumnVisitor();
      ColumnMetadata columnMetadata = ctx.column().accept(columnVisitor);
      if (ctx.property_values() != null) {
        PropertiesVisitor propertiesVisitor = new PropertiesVisitor();
        columnMetadata.setProperties(ctx.property_values().accept(propertiesVisitor));
      }
      return columnMetadata;
    }
  }

  /**
   * Visits various types of columns (primitive, struct, map, array) and stores their metadata
   * into {@link ColumnMetadata} class.
   */
  public static class ColumnVisitor extends SchemaParserBaseVisitor<ColumnMetadata> {

    @Override
    public ColumnMetadata visitPrimitive_column(SchemaParser.Primitive_columnContext ctx) {
      String name = ctx.column_id().accept(new IdVisitor());
      TypeProtos.DataMode mode = ctx.nullability() == null ? TypeProtos.DataMode.OPTIONAL : TypeProtos.DataMode.REQUIRED;
      ColumnMetadata columnMetadata = ctx.simple_type().accept(new TypeVisitor(name, mode));
      StringValueVisitor stringValueVisitor = new StringValueVisitor();
      if (ctx.format_value() != null) {
        columnMetadata.setFormat(stringValueVisitor.visit(ctx.format_value().string_value()));
      }
      if (ctx.default_value() != null) {
        columnMetadata.setDefaultValue(stringValueVisitor.visit(ctx.default_value().string_value()));
      }
      return columnMetadata;
    }

    @Override
    public ColumnMetadata visitSimple_array_column(SchemaParser.Simple_array_columnContext ctx) {
      String name = ctx.column_id().accept(new IdVisitor());
      return ctx.simple_array_type().accept(new ArrayTypeVisitor(name));
    }

    @Override
    public ColumnMetadata visitStruct_column(SchemaParser.Struct_columnContext ctx) {
      String name = ctx.column_id().accept(new IdVisitor());
      // Drill does not distinguish between nullable and not null structs, by default they are not null
      return ctx.struct_type().accept(new TypeVisitor(name, TypeProtos.DataMode.REQUIRED));
    }

    @Override
    public ColumnMetadata visitMap_column(SchemaParser.Map_columnContext ctx) {
      String name = ctx.column_id().accept(new IdVisitor());
      // Drill does not distinguish between nullable and not null maps, by default they are not null
      return ctx.map_type().accept(new TypeVisitor(name, TypeProtos.DataMode.REQUIRED));
    }

    @Override
    public ColumnMetadata visitUnion_column(SchemaParser.Union_columnContext ctx) {
      String name = ctx.column_id().accept(new IdVisitor());
      return ctx.union_type().accept(new UnionTypeVisitor(name));
    }

    @Override
    public ColumnMetadata visitComplex_array_column(SchemaParser.Complex_array_columnContext ctx) {
      String name = ctx.column_id().accept(new IdVisitor());
      ColumnMetadata child = ctx.complex_array_type().complex_array_value_type()
        .accept(new ComplexArrayValueTypeVisitor(name));
      RepeatedListBuilder builder = new RepeatedListBuilder(null, name);
      builder.addColumn(child);
      return builder.buildColumn();
    }
  }

  /**
   * Visits quoted string, strips backticks, single quotes or double quotes and returns bare string value.
   */
  private static class StringValueVisitor extends SchemaParserBaseVisitor<String> {

    @Override
    public String visitString_value(SchemaParser.String_valueContext ctx) {
      String text = ctx.getText();
      // first substring first and last symbols (backticks, single quotes, double quotes)
      // then find all chars that are preceding with the backslash and remove the backslash
      return text.substring(1, text.length() -1).replaceAll("\\\\(.)", "$1");
    }
  }

  /**
   * Visits ID and QUOTED_ID, returning their string representation.
   */
  private static class IdVisitor extends SchemaParserBaseVisitor<String> {

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

  /**
   * Visits simple, struct and map types and stores their metadata into {@link ColumnMetadata} holder.
   */
  private static class TypeVisitor extends SchemaParserBaseVisitor<ColumnMetadata> {

    private final String name;
    private final TypeProtos.DataMode mode;

    TypeVisitor(TypeProtos.DataMode mode) {
      this(null, mode);
    }

    TypeVisitor(String name, TypeProtos.DataMode mode) {
      this.name = name;
      this.mode = mode;
    }

    @Override
    public ColumnMetadata visitInt(SchemaParser.IntContext ctx) {
      return constructColumn(Types.withMode(TypeProtos.MinorType.INT, mode));
    }

    @Override
    public ColumnMetadata visitBigint(SchemaParser.BigintContext ctx) {
      return constructColumn(Types.withMode(TypeProtos.MinorType.BIGINT, mode));
    }

    @Override
    public ColumnMetadata visitFloat(SchemaParser.FloatContext ctx) {
      return constructColumn(Types.withMode(TypeProtos.MinorType.FLOAT4, mode));
    }

    @Override
    public ColumnMetadata visitDouble(SchemaParser.DoubleContext ctx) {
      return constructColumn(Types.withMode(TypeProtos.MinorType.FLOAT8, mode));
    }

    @Override
    public ColumnMetadata visitDecimal(SchemaParser.DecimalContext ctx) {
      TypeProtos.MajorType type = Types.withMode(TypeProtos.MinorType.VARDECIMAL, mode);

      List<TerminalNode> numbers = ctx.NUMBER();
      if (!numbers.isEmpty()) {
        int precision = Integer.parseInt(numbers.get(0).getText());
        int scale = numbers.size() == 2 ? Integer.parseInt(numbers.get(1).getText()) : 0;
        type = type.toBuilder().setPrecision(precision).setScale(scale).build();
      }

      return constructColumn(type);
    }

    @Override
    public ColumnMetadata visitBoolean(SchemaParser.BooleanContext ctx) {
      return constructColumn(Types.withMode(TypeProtos.MinorType.BIT, mode));
    }

    @Override
    public ColumnMetadata visitVarchar(SchemaParser.VarcharContext ctx) {
      TypeProtos.MajorType type = Types.withMode(TypeProtos.MinorType.VARCHAR, mode);

      if (ctx.NUMBER() != null) {
        type = type.toBuilder().setPrecision(Integer.parseInt(ctx.NUMBER().getText())).build();
      }

      return constructColumn(type);
    }

    @Override
    public ColumnMetadata visitBinary(SchemaParser.BinaryContext ctx) {
      TypeProtos.MajorType type = Types.withMode(TypeProtos.MinorType.VARBINARY, mode);

      if (ctx.NUMBER() != null) {
        type = type.toBuilder().setPrecision(Integer.parseInt(ctx.NUMBER().getText())).build();
      }

      return constructColumn(type);
    }

    @Override
    public ColumnMetadata visitTime(SchemaParser.TimeContext ctx) {
      TypeProtos.MajorType type = Types.withMode(TypeProtos.MinorType.TIME, mode);

      if (ctx.NUMBER() != null) {
        type = type.toBuilder().setPrecision(Integer.parseInt(ctx.NUMBER().getText())).build();
      }

      return constructColumn(type);
    }

    @Override
    public ColumnMetadata visitDate(SchemaParser.DateContext ctx) {
      return constructColumn(Types.withMode(TypeProtos.MinorType.DATE, mode));
    }

    @Override
    public ColumnMetadata visitTimestamp(SchemaParser.TimestampContext ctx) {
      TypeProtos.MajorType type = Types.withMode(TypeProtos.MinorType.TIMESTAMP, mode);

      if (ctx.NUMBER() != null) {
        type = type.toBuilder().setPrecision(Integer.parseInt(ctx.NUMBER().getText())).build();
      }

      return constructColumn(type);
    }

    @Override
    public ColumnMetadata visitInterval_year(SchemaParser.Interval_yearContext ctx) {
      return constructColumn(Types.withMode(TypeProtos.MinorType.INTERVALYEAR, mode));
    }

    @Override
    public ColumnMetadata visitInterval_day(SchemaParser.Interval_dayContext ctx) {
      return constructColumn(Types.withMode(TypeProtos.MinorType.INTERVALDAY, mode));
    }

    @Override
    public ColumnMetadata visitInterval(SchemaParser.IntervalContext ctx) {
      return constructColumn(Types.withMode(TypeProtos.MinorType.INTERVAL, mode));
    }

    @Override
    public ColumnMetadata visitStruct_type(SchemaParser.Struct_typeContext ctx) {
      // internally Drill refers to structs as maps
      MapBuilder builder = new MapBuilder(null, name(TypeProtos.MinorType.MAP), mode);
      ColumnDefVisitor visitor = new ColumnDefVisitor();
      ctx.columns().column_def().forEach(
        c -> builder.addColumn(c.accept(visitor))
      );
      return builder.buildColumn();
    }

    @Override
    public ColumnMetadata visitMap_type(SchemaParser.Map_typeContext ctx) {
      // internally Drill refers to maps as dicts
      DictBuilder builder = new DictBuilder(null, name(TypeProtos.MinorType.DICT), mode);
      builder.key(ctx.map_key_type_def().map_key_type().accept(new MapKeyTypeVisitor()));

      SchemaParser.Map_value_type_defContext valueDef = ctx.map_value_type_def();
      TypeProtos.DataMode valueMode = valueDef.nullability() == null ? TypeProtos.DataMode.OPTIONAL : TypeProtos.DataMode.REQUIRED;
      builder.addColumn(valueDef.map_value_type().accept(new MapValueTypeVisitor(valueMode)));
      return builder.buildColumn();
    }

    private String name(TypeProtos.MinorType minorType) {
      return name == null ? Types.typeKey(minorType) : name;
    }

    private ColumnMetadata constructColumn(TypeProtos.MajorType type) {
      MaterializedField field = MaterializedField.create(name(type.getMinorType()), type);
      return MetadataUtils.fromField(field);
    }
  }

  /**
   * Visits map key type and returns its {@link TypeProtos.MajorType} definition.
   */
  private static class MapKeyTypeVisitor extends SchemaParserBaseVisitor<TypeProtos.MajorType> {

    @Override
    public TypeProtos.MajorType visitMap_key_simple_type_def(SchemaParser.Map_key_simple_type_defContext ctx) {
      // map key is always required
      TypeVisitor keyVisitor = new TypeVisitor(DictVector.FIELD_KEY_NAME, TypeProtos.DataMode.REQUIRED);
      return ctx.simple_type().accept(keyVisitor).majorType();
    }
  }

  /**
   * Visits map value type and stores its metadata into {@link ColumnMetadata} holder.
   */
  private static class MapValueTypeVisitor extends SchemaParserBaseVisitor<ColumnMetadata> {

    private final TypeProtos.DataMode mode;

    MapValueTypeVisitor (TypeProtos.DataMode mode) {
      this.mode = mode;
    }

    @Override
    public ColumnMetadata visitMap_value_simple_type_def(SchemaParser.Map_value_simple_type_defContext ctx) {
      return ctx.simple_type().accept(new TypeVisitor(DictVector.FIELD_VALUE_NAME, mode));
    }

    @Override
    public ColumnMetadata visitMap_value_struct_type_def(SchemaParser.Map_value_struct_type_defContext ctx) {
      // Drill does not distinguish between nullable and not null structs, by default they are not null
      TypeProtos.DataMode structMode = TypeProtos.DataMode.REPEATED == mode ? mode : TypeProtos.DataMode.REQUIRED;
      return ctx.struct_type().accept(new TypeVisitor(DictVector.FIELD_VALUE_NAME, structMode));
    }

    @Override
    public ColumnMetadata visitMap_value_map_type_def(SchemaParser.Map_value_map_type_defContext ctx) {
      // Drill does not distinguish between nullable and not null maps, by default they are not null
      TypeProtos.DataMode mapMode = TypeProtos.DataMode.REPEATED == mode ? mode : TypeProtos.DataMode.REQUIRED;
      return ctx.map_type().accept(new TypeVisitor(DictVector.FIELD_VALUE_NAME, mapMode));
    }

    @Override
    public ColumnMetadata visitMap_value_array_type_def(SchemaParser.Map_value_array_type_defContext ctx) {
      return ctx.array_type().accept(new ArrayTypeVisitor(DictVector.FIELD_VALUE_NAME));
    }

    @Override
    public ColumnMetadata visitMap_value_union_type_def(SchemaParser.Map_value_union_type_defContext ctx) {
      return ctx.union_type().accept(new UnionTypeVisitor(DictVector.FIELD_VALUE_NAME));
    }
  }

  /**
   * Visits union type and stores its metadata into {@link ColumnMetadata} holder.
   */
  private static class UnionTypeVisitor extends SchemaParserBaseVisitor<ColumnMetadata> {

    private final String name;

    UnionTypeVisitor(String name) {
      this.name = name;
    }

    @Override
    public ColumnMetadata visitUnion_type(SchemaParser.Union_typeContext ctx) {
      VariantSchema variantSchema = new VariantSchema();
      UnionMemberTypeVisitor unionMemberTypeVisitor = new UnionMemberTypeVisitor();
      ctx.union_members().union_member_type().stream()
        .map(subType -> subType.accept(unionMemberTypeVisitor))
        .forEach(variantSchema::addType);
      return new VariantColumnMetadata(name, TypeProtos.MinorType.UNION, variantSchema);
    }
  }

  /**
   * Visits union member type and stores its metadata into {@link ColumnMetadata} holder.
   */
  private static class UnionMemberTypeVisitor extends SchemaParserBaseVisitor<ColumnMetadata> {

    @Override
    public ColumnMetadata visitUnion_member_simple_type_def(SchemaParser.Union_member_simple_type_defContext ctx) {
      return ctx.simple_type().accept(new TypeVisitor(TypeProtos.DataMode.OPTIONAL));
    }

    @Override
    public ColumnMetadata visitUnion_member_struct_type_def(SchemaParser.Union_member_struct_type_defContext ctx) {
      return ctx.struct_type().accept(new TypeVisitor(TypeProtos.DataMode.OPTIONAL));
    }

    @Override
    public ColumnMetadata visitUnion_member_map_type_def(SchemaParser.Union_member_map_type_defContext ctx) {
      return ctx.map_type().accept(new TypeVisitor(TypeProtos.DataMode.OPTIONAL));
    }

    @Override
    public ColumnMetadata visitUnion_member_array_type_def(SchemaParser.Union_member_array_type_defContext ctx) {
      return ctx.array_type().accept(new ArrayTypeVisitor(Types.typeKey(TypeProtos.MinorType.LIST)));
    }
  }

  /**
   * Visits array type:
   * simple (which has only one nested element: array<int>) or
   * complex (which has several nested elements (array<int<int>>) or union (array<union<int, varchar>>)).
   */
  private static class ArrayTypeVisitor extends SchemaParserBaseVisitor<ColumnMetadata> {

    private final String name;

    ArrayTypeVisitor(String name) {
      this.name = name;
    }

    @Override
    public ColumnMetadata visitSimple_array_type(SchemaParser.Simple_array_typeContext ctx) {
      SimpleArrayValueTypeVisitor visitor = new SimpleArrayValueTypeVisitor(name);
      return ctx.simple_array_value_type().accept(visitor);
    }

    @Override
    public ColumnMetadata visitComplex_array_type(SchemaParser.Complex_array_typeContext ctx) {
      RepeatedListBuilder childBuilder = new RepeatedListBuilder(null, name);
      ColumnMetadata columnMetadata = ctx.complex_array_value_type().accept(new ComplexArrayValueTypeVisitor(name));
      childBuilder.addColumn(columnMetadata);
      return childBuilder.buildColumn();
    }
  }

  /**
   * Visits complex array value type and stores its metadata into {@link ColumnMetadata} holder.
   */
  private static class ComplexArrayValueTypeVisitor extends SchemaParserBaseVisitor<ColumnMetadata> {

    private final String name;

    ComplexArrayValueTypeVisitor(String name) {
      this.name = name;
    }

    @Override
    public ColumnMetadata visitArray_nested_type_def(SchemaParser.Array_nested_type_defContext ctx) {
      return ctx.array_type().accept(new ArrayTypeVisitor(name));
    }

    @Override
    public ColumnMetadata visitArray_union_type_def(SchemaParser.Array_union_type_defContext ctx) {
      return ctx.union_type().accept(new UnionTypeVisitor(name));
    }
  }

  /**
   * Visits simple array value type and stores its metadata into {@link ColumnMetadata} holder.
   */
  private static class SimpleArrayValueTypeVisitor extends SchemaParserBaseVisitor<ColumnMetadata> {

    private final TypeVisitor typeVisitor;

    SimpleArrayValueTypeVisitor(String name) {
      this.typeVisitor = new TypeVisitor(name, TypeProtos.DataMode.REPEATED);
    }

    @Override
    public ColumnMetadata visitArray_simple_type_def(SchemaParser.Array_simple_type_defContext ctx) {
      return ctx.simple_type().accept(typeVisitor);
    }

    @Override
    public ColumnMetadata visitArray_struct_type_def(SchemaParser.Array_struct_type_defContext ctx) {
      return ctx.struct_type().accept(typeVisitor);
    }

    @Override
    public ColumnMetadata visitArray_map_type_def(SchemaParser.Array_map_type_defContext ctx) {
      return ctx.map_type().accept(typeVisitor);
    }

    private void t() {
      VariantColumnMetadata metadata = new VariantColumnMetadata("name",
        TypeProtos.MinorType.LIST, null);
    }
  }

  /**
   * Visits schema or column properties.
   * Properties must be identified as key values pairs separated by equals sign.
   * Properties pairs must be separated by comma.
   * Property name and value must be enclosed into backticks, single quotes or double quotes.
   */
  public static class PropertiesVisitor extends SchemaParserBaseVisitor<Map<String, String>> {

    @Override
    public Map<String, String> visitProperty_values(SchemaParser.Property_valuesContext ctx) {
      StringValueVisitor stringValueVisitor = new StringValueVisitor();
      Map<String, String> properties = new LinkedHashMap<>();
      ctx.property_pair().forEach(
        pair -> {
          List<String> pairValues = pair.string_value().stream()
            .map(stringValueVisitor::visit)
            .collect(Collectors.toList());
          Preconditions.checkState(pairValues.size() == 2);
          properties.put(pairValues.get(0), pairValues.get(1));
        }
      );
      return properties;
    }
  }
}
