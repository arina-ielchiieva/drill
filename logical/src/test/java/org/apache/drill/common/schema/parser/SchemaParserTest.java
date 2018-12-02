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
package org.apache.drill.common.schema.parser;

import org.apache.drill.common.parser.SchemaDefParser;
import org.apache.drill.common.schema.ColumnDef;
import org.apache.drill.common.schema.SchemaDef;
import org.apache.drill.common.schema.TypeDef;
import org.apache.drill.common.types.TypeProtos;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SchemaParserTest {

  @Test
  public void testNumericTypes() {
    SchemaDef schemaDef = new SchemaDef(Arrays.asList(
      ColumnDef.Builder.builder().setName("int_col").setNullability(true)
        .setTypeDef(TypeDef.createSimpleType(TypeProtos.MinorType.INT)).build(),

      ColumnDef.Builder.builder().setName("integer_col").setNullability(true)
        .setTypeDef(TypeDef.createSimpleType(TypeProtos.MinorType.INT)).build(),

      ColumnDef.Builder.builder().setName("bigint_col").setNullability(true)
        .setTypeDef(TypeDef.createSimpleType(TypeProtos.MinorType.BIGINT)).build(),

      ColumnDef.Builder.builder().setName("float_col").setNullability(true)
        .setTypeDef(TypeDef.createSimpleType(TypeProtos.MinorType.FLOAT4)).build(),

      ColumnDef.Builder.builder().setName("double_col").setNullability(true)
        .setTypeDef(TypeDef.createSimpleType(TypeProtos.MinorType.FLOAT8)).build()
      ));

    checkSchema("int_col int, integer_col integer, bigint_col bigint, float_col float, double_col double",
      schemaDef,
      "`int_col` INT, `integer_col` INT, `bigint_col` BIGINT, `float_col` FLOAT, `double_col` DOUBLE");
  }

  @Test
  public void testDecimalTypes() {
    SchemaDef schemaDef = new SchemaDef(Arrays.asList(
      ColumnDef.Builder.builder().setName("col").setNullability(true)
        .setTypeDef(TypeDef.createSimpleType(TypeProtos.MinorType.VARDECIMAL)).build(),

      ColumnDef.Builder.builder().setName("col_p").setNullability(true)
        .setTypeDef(TypeDef.createSimpleType(TypeProtos.MinorType.VARDECIMAL, 5)).build(),

      ColumnDef.Builder.builder().setName("col_ps").setNullability(true)
        .setTypeDef(TypeDef.createSimpleType(TypeProtos.MinorType.VARDECIMAL, 10, 2)).build()
    ));

    List<String> schemas = Arrays.asList(
      "col dec, col_p dec(5), col_ps dec(10, 2)",
      "col decimal, col_p decimal(5), col_ps decimal(10, 2)",
      "col numeric, col_p numeric(5), col_ps numeric(10, 2)"
    );

    String expectedSchema = "`col` DECIMAL, `col_p` DECIMAL(5), `col_ps` DECIMAL(10, 2)";

    schemas.forEach(
      s -> checkSchema(s, schemaDef, expectedSchema)
    );
  }


  @Test
  public void testBooleanType() {
    SchemaDef schemaDef = new SchemaDef(
      Collections.singletonList(
        ColumnDef.Builder.builder()
          .setName("col")
          .setNullability(true)
          .setTypeDef(TypeDef.createSimpleType(TypeProtos.MinorType.BIT))
          .build()));

    checkSchema("col boolean", schemaDef, "`col` BOOLEAN");
  }

  @Test
  public void testCharacterTypes() {
    String schema = "col %1$s, col_p %1$s(50)";
    String expectedSchema = "`col` %1$s, `col_p` %1$s(50)";

    Map<String, TypeProtos.MinorType> properties = new HashMap<>();
    properties.put("char", TypeProtos.MinorType.VARCHAR);
    properties.put("character", TypeProtos.MinorType.VARCHAR);
    properties.put("character varying", TypeProtos.MinorType.VARCHAR);
    properties.put("varchar", TypeProtos.MinorType.VARCHAR);
    properties.put("binary", TypeProtos.MinorType.VARBINARY);
    properties.put("varbinary", TypeProtos.MinorType.VARBINARY);

    properties.forEach((key, value) -> {
      SchemaDef schemaDef = new SchemaDef(Arrays.asList(
        ColumnDef.Builder.builder().setName("col").setNullability(true)
          .setTypeDef(TypeDef.createSimpleType(value)).build(),

        ColumnDef.Builder.builder().setName("col_p").setNullability(true)
          .setTypeDef(TypeDef.createSimpleType(value, 50)).build()
      ));

      checkSchema(String.format(schema, key), schemaDef, String.format(expectedSchema, value.name()));
    });
  }

  @Test
  public void testTimeTypes() {
    SchemaDef schemaDef = new SchemaDef(Arrays.asList(
      ColumnDef.Builder.builder().setName("time_col").setNullability(true)
        .setTypeDef(TypeDef.createSimpleType(TypeProtos.MinorType.TIME)).build(),

      ColumnDef.Builder.builder().setName("date_col").setNullability(true)
        .setTypeDef(TypeDef.createSimpleType(TypeProtos.MinorType.DATE)).build(),

      ColumnDef.Builder.builder().setName("timestamp_col").setNullability(true)
        .setTypeDef(TypeDef.createSimpleType(TypeProtos.MinorType.TIMESTAMP)).build(),

      ColumnDef.Builder.builder().setName("interval_col").setNullability(true)
        .setTypeDef(TypeDef.createSimpleType(TypeProtos.MinorType.INTERVAL)).build()
    ));

    checkSchema("time_col time, date_col date, timestamp_col timestamp, interval_col interval",
      schemaDef,
      "`time_col` TIME, `date_col` DATE, `timestamp_col` TIMESTAMP, `interval_col` INTERVAL");
  }

  @Test
  public void testArray() {

    SchemaDef schemaDef = new SchemaDef(Arrays.asList(
      ColumnDef.Builder.builder().setName("simple_array").setNullability(true)
        .setTypeDef(TypeDef.createArrayType(
          TypeDef.createSimpleType(TypeProtos.MinorType.INT))).build(),

     ColumnDef.Builder.builder().setName("nested_array").setNullability(true)
        .setTypeDef(TypeDef.createArrayType(TypeDef.createArrayType(
          TypeDef.createSimpleType(TypeProtos.MinorType.INT)))).build(),

      ColumnDef.Builder.builder().setName("array_with_map").setNullability(true)
        .setTypeDef(TypeDef.createArrayType(TypeDef.createMapType(Arrays.asList(

          ColumnDef.Builder.builder().setName("m1").setNullability(true)
            .setTypeDef(TypeDef.createSimpleType(TypeProtos.MinorType.INT)).build(),

          ColumnDef.Builder.builder().setName("m2").setNullability(true)
            .setTypeDef(TypeDef.createSimpleType(TypeProtos.MinorType.INT)).build()
          )))).build()
    ));

    checkSchema("simple_array array<int>"
        + ", nested_array array<array<int>>"
        + ", array_with_map array<map<m1 int, m2 int>>",
      schemaDef,
      "`simple_array` ARRAY<INT>"
        + ", `nested_array` ARRAY<ARRAY<INT>>"
        + ", `array_with_map` ARRAY<MAP<`m1` INT, `m2` INT>>"
    );
  }

  @Test
  public void testMap() {
    SchemaDef schemaDef = new SchemaDef(Collections.singletonList(
      ColumnDef.Builder.builder().setName("map_col").setNullability(true)
        .setTypeDef(TypeDef.createMapType(Arrays.asList(

          ColumnDef.Builder.builder().setName("int_col").setNullability(true)
            .setTypeDef(TypeDef.createSimpleType(TypeProtos.MinorType.INT)).build(),

          ColumnDef.Builder.builder().setName("array_col").setNullability(true)
            .setTypeDef(TypeDef.createArrayType(
              TypeDef.createSimpleType(TypeProtos.MinorType.INT))).build(),

          ColumnDef.Builder.builder().setName("nested_map").setNullability(true)
            .setTypeDef(TypeDef.createMapType(Arrays.asList(
              ColumnDef.Builder.builder().setName("m1").setNullability(true)
                .setTypeDef(TypeDef.createSimpleType(TypeProtos.MinorType.INT)).build(),

              ColumnDef.Builder.builder().setName("m2").setNullability(true)
                .setTypeDef(TypeDef.createSimpleType(TypeProtos.MinorType.INT)).build()
            ))).build()
        ))).build()));

    checkSchema("map_col map<int_col int, array_col array<int>, nested_map map<m1 int, m2 int>>",
      schemaDef,
      "`map_col` MAP<`int_col` INT, `array_col` ARRAY<INT>, `nested_map` MAP<`m1` INT, `m2` INT>>");
  }

  @Test
  public void testIndexedColumns() {
    SchemaDef schemaDef = new SchemaDef(Arrays.asList(
      ColumnDef.Builder.builder().setName("COLUMNS").setIndex(0).setNullability(false)
        .setTypeDef(TypeDef.createSimpleType(TypeProtos.MinorType.INT, false)).setAlias("id").build(),

      ColumnDef.Builder.builder().setName("COLUMNS").setIndex(1).setNullability(true)
        .setTypeDef(TypeDef.createSimpleType(TypeProtos.MinorType.VARCHAR)).setAlias("name").build()
      ));

    checkSchema("columns[0] INT NOT NULL as id, `COLUMNS`[1] VARCHAR as `name`",
      schemaDef,
      "`COLUMNS`[0] INT NOT NULL AS `id`, `COLUMNS`[1] VARCHAR AS `name`");
  }

  @Test
  public void testNullabilityForSimpleType() {
    String schema = "ID INT NOT NULL, NAME VARCHAR";
    SchemaDef schemaDef = SchemaDefParser.parseSchema(schema);
    assertEquals(2, schemaDef.getColumns().size());

    ColumnDef id = schemaDef.getColumns().get(0);
    assertFalse(id.isNullable());
    assertEquals(TypeProtos.DataMode.REQUIRED, id.getTypeDef().getType().getMode());

    ColumnDef name = schemaDef.getColumns().get(1);
    assertTrue(name.isNullable());
    assertEquals(TypeProtos.DataMode.OPTIONAL, name.getTypeDef().getType().getMode());
  }

  @Test
  public void testNullabilityForRepeatedType() {
    String schema = "array_col array<int> not null, map_col map<m1 varchar, m2 boolean not null>";
    SchemaDef schemaDef = SchemaDefParser.parseSchema(schema);
    assertEquals(2, schemaDef.getColumns().size());

    ColumnDef arrayColumn = schemaDef.getColumns().get(0);
    assertFalse(arrayColumn.isNullable());
    assertEquals(TypeProtos.DataMode.REPEATED, arrayColumn.getTypeDef().getType().getMode());

    TypeDef subType = arrayColumn.getTypeDef().getSubType();
    assertEquals(TypeProtos.DataMode.OPTIONAL, subType.getType().getMode());

    ColumnDef mapColumn = schemaDef.getColumns().get(1);
    assertTrue(mapColumn.isNullable());
    assertEquals(TypeProtos.DataMode.REPEATED, mapColumn.getTypeDef().getType().getMode());

    List<ColumnDef> subColumns = mapColumn.getTypeDef().getSubColumns();
    assertEquals(2, subColumns.size());

    ColumnDef m1 = subColumns.get(0);
    assertTrue(m1.isNullable());
    assertEquals(TypeProtos.DataMode.OPTIONAL, m1.getTypeDef().getType().getMode());

    ColumnDef m2 = subColumns.get(1);
    assertFalse(m2.isNullable());
    assertEquals(TypeProtos.DataMode.REQUIRED, m2.getTypeDef().getType().getMode());
  }

  @Test
  public void checkQuotedId() {
    String schemaWithEscapes = "`a\\\\b\\`c` INT AS `col(\\`a\\\\b\\`)`";
    assertEquals(schemaWithEscapes, SchemaDefParser.parseSchema(schemaWithEscapes).toString());

    String schemaWithKeywords = "`INTEGER` INT AS `NOT NULL`";
    assertEquals(schemaWithKeywords, SchemaDefParser.parseSchema(schemaWithKeywords).toString());
  }

  @Test
  public void testAlias() {
    String schema = "col int as id, name varchar";
    SchemaDef schemaDef = SchemaDefParser.parseSchema(schema);
    assertEquals(2, schemaDef.getColumns().size());

    ColumnDef id = schemaDef.getColumns().get(0);
    assertEquals("id", id.getAlias());

    ColumnDef name = schemaDef.getColumns().get(1);
    assertNull(name.getAlias());
  }

  @Test
  public void testSchemaWithParen() {
    String schema = "`a` INT NOT NULL, `b` VARCHAR(10)";
    SchemaDef actualSchema = SchemaDefParser.parseSchema("(" + schema + ")");
    assertEquals(schema, actualSchema.toString());
  }

  @Test
  public void testSkip() {
    String schema = "id\n /* comment */   int\r  , // comment\r\nname  \n  varchar\t\t\t";
    SchemaDef schemaDef = SchemaDefParser.parseSchema(schema);
    assertEquals(2, schemaDef.getColumns().size());
    assertEquals("`id` INT, `name` VARCHAR", schemaDef.toString());
  }

  @Test
  public void testCaseInsensitivity() {
    String schema = "`Id` InTeGeR NoT NuLl As `New Id`";
    SchemaDef schemaDef = SchemaDefParser.parseSchema(schema);
    assertEquals("`Id` INT NOT NULL AS `New Id`", schemaDef.toString());
  }

  @Test
  public void testParseColumnDef() {
    ColumnDef namedColumn = SchemaDefParser.parseColumn("col int not null as id");
    assertEquals("`col` INT NOT NULL AS `id`", namedColumn.toString());

    ColumnDef indexedColumn = SchemaDefParser.parseColumn("columns[0] int not null as id");
    assertEquals("`COLUMNS`[0] INT NOT NULL AS `id`", indexedColumn.toString());
  }

  @Test
  public void testParseTypeDef() {
    TypeDef typeDef = SchemaDefParser.parseType("map<m int>");
    assertEquals("MAP<`m` INT>", typeDef.toString());
  }

  private void checkSchema(String schemaString, SchemaDef expectedSchema, String expectedSchemaString) {
    SchemaDef actualSchema = SchemaDefParser.parseSchema(schemaString);
    assertEquals(expectedSchema, actualSchema);
    assertEquals(expectedSchemaString, actualSchema.toString());

    SchemaDef unparsedSchema = SchemaDefParser.parseSchema(actualSchema.toString());
    assertEquals(unparsedSchema, expectedSchema);
    assertEquals(expectedSchemaString, unparsedSchema.toString());
  }

}
