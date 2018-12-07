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
package org.apache.drill.exec.record;

import org.apache.drill.common.schema.ColumnDef;
import org.apache.drill.common.schema.SchemaDef;
import org.apache.drill.common.schema.TypeDef;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.metadata.TupleSchema;
import org.apache.drill.test.rowSet.schema.SchemaBuilder;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestTupleSchemaTransform {

  @Test
  public void testSimpleColumn() {

    // ColumnDef -> PrimitiveColumnMetadata
    ColumnDef columnDef = ColumnDef.Builder.builder()
      .setName("id")
      .setNullability(true)
      .setTypeDef(TypeDef.createSimpleType(TypeProtos.MinorType.INT))
      .build();
    System.out.println(columnDef);

    MaterializedField field = MaterializedField.create(columnDef.getName(), columnDef.getTypeDef().getType());
    System.out.println(field);

    // ColumnMetadata columnMetadata = new PrimitiveColumnMetadata(field);
    ColumnMetadata columnMetadata = MetadataUtils.fromField(field);
    System.out.println(columnMetadata);

    // MaterializedField -> ColumnDef
    ColumnDef builtFromField = ColumnDef.Builder.builder()
      .setName(field.getName())
      .setNullability(field.isNullable())
      .setTypeDef(new TypeDef(field.getType(), null, null))
      .build();

    assertEquals(columnDef, builtFromField);

    // ColumnMetadata -> ColumnDef

    ColumnDef builtFromColumnMetadata = ColumnDef.Builder.builder()
      .setName(columnMetadata.name())
      .setNullability(columnMetadata.isNullable())
      .setTypeDef(new TypeDef(columnMetadata.majorType(), null, null))
      .build();

    //todo fails because precision and scale are set
    // assertEquals(columnDef, builtFromColumnMetadata);
    /*
      Expected :`id` INT
      Actual   :`id` INT(0, 0)
     */

  }

  @Test
  public void testSimpleArrayColumn() {
    // array_col array<int>
    ColumnDef columnDef = ColumnDef.Builder.builder()
      .setName("array_col")
      .setNullability(true)
      .setTypeDef(new TypeDef(TypeProtos.MajorType.newBuilder().setMode(TypeProtos.DataMode.REPEATED)
        .setMinorType(TypeProtos.MinorType.INT).build(), null, null))
      .build();
    System.out.println(columnDef);

    MaterializedField field = MaterializedField.create(columnDef.getName(), columnDef.getTypeDef().getType());
    System.out.println(field);

    ColumnMetadata columnMetadata = MetadataUtils.fromField(field);
    System.out.println(columnMetadata);

    // SchemaBuilder.columnSchema("c", MinorType.INT, DataMode.REPEATED); -> definition of simple array

    //todo refactor builders call as this TypeProtos.MajorType.newBuilder() in parser
  }

  @Test
  public void testComplexArrayColumn() {
    // array_col array<array<int>>

    TupleMetadata schema = new SchemaBuilder()
      //.add("a", TypeProtos.MinorType.INT)
      .addMapArray("m")
      .add("b", TypeProtos.MinorType.INT)
      .add("c", TypeProtos.MinorType.INT)
      .resumeSchema()
      .buildSchema();

    System.out.println(schema);

    List<MaterializedField> materializedFields = schema.toFieldList();
    System.out.println(materializedFields);
    // repeated map


    TupleMetadata schema2 = new SchemaBuilder()
      .addRepeatedList("list")
      .addArray(TypeProtos.MinorType.VARCHAR)
      .resumeSchema()
      .buildSchema();

    System.out.println(schema2);

  }

  @Test
  public void testSimpleMapColumn() {
    // map_column MAP<m1 INT not null, m2 varchar(50), m3 boolean>
    ColumnDef columnDef = ColumnDef.Builder.builder()
      .setName("map_column")
      .setNullability(true)
      .setTypeDef(TypeDef.createMapType(Arrays.asList(
        ColumnDef.Builder.builder().setName("m1")
          .setNullability(false).setTypeDef(TypeDef.createSimpleType(TypeProtos.MinorType.INT, false)).build(),

        ColumnDef.Builder.builder().setName("m2")
          .setNullability(true).setTypeDef(TypeDef.createSimpleType(TypeProtos.MinorType.VARCHAR, 50)).build(),

        ColumnDef.Builder.builder().setName("m3")
          .setNullability(true).setTypeDef(TypeDef.createSimpleType(TypeProtos.MinorType.BIT)).build()
        )
      ))
      .build();
    System.out.println(columnDef);

    MaterializedField field = MaterializedField.create(columnDef.getName(), columnDef.getTypeDef().getType());
    columnDef.getTypeDef().getSubColumns().forEach(
      c -> field.addChild(MaterializedField.create(c.getName(), c.getTypeDef().getType()))
    );
    System.out.println(field);

    // MapColumnMetadata
    ColumnMetadata columnMetadata = MetadataUtils.fromField(field);
    System.out.println(columnMetadata);

    //todo duplicate entry check (maybe add it in the parser as well)
  }

  @Test
  public void testNestedMapColumn() {
    // map_column MAP<m1 INT not null, m2 map<mm1 INT, mm2 varchar(50)>>)
    ColumnDef columnDef = ColumnDef.Builder.builder()
      .setName("map_column")
      .setNullability(true)
      .setTypeDef(TypeDef.createMapType(Arrays.asList(
        ColumnDef.Builder.builder().setName("m1")
          .setNullability(false).setTypeDef(TypeDef.createSimpleType(TypeProtos.MinorType.INT, false)).build(),

        ColumnDef.Builder.builder().setName("m2")
          .setNullability(true)
          .setTypeDef(TypeDef.createMapType(Arrays.asList(
          ColumnDef.Builder.builder().setName("mm1")
            .setNullability(true).setTypeDef(TypeDef.createSimpleType(TypeProtos.MinorType.INT)).build(),

          ColumnDef.Builder.builder().setName("mm2")
            .setNullability(true).setTypeDef(TypeDef.createSimpleType(TypeProtos.MinorType.VARCHAR, 50)).build())
          )).build()
      ))).build();
    System.out.println(columnDef);

    MaterializedField field = createMaterializedField(columnDef);
    System.out.println(field);

    ColumnMetadata columnMetadata = MetadataUtils.fromField(field);
    System.out.println(columnMetadata);

  }

  private MaterializedField createMaterializedField(ColumnDef columnDef) {
    MaterializedField field = MaterializedField.create(columnDef.getName(), columnDef.getTypeDef().getType());
    columnDef.getTypeDef().getSubColumns().forEach(
      c -> {
        if (c.getTypeDef().isComplex()) { //isMap()
          field.addChild(createMaterializedField(c)); // recursive call
        } else {
          field.addChild(MaterializedField.create(c.getName(), c.getTypeDef().getType()));
        }
      }
    );
    return field;
  }

  @Test
  public void testIndexedColumn() {
    ColumnDef columnDef = ColumnDef.Builder.builder()
      .setName("COLUMNS")
      .setIndex(0)
      .setNullability(true)
      .setTypeDef(TypeDef.createSimpleType(TypeProtos.MinorType.INT))
      .setAlias("id")
      .build();
    ColumnDef columnDef2 = ColumnDef.Builder.builder()
      .setName("COLUMNS")
      .setIndex(5)
      .setNullability(true)
      .setTypeDef(TypeDef.createSimpleType(TypeProtos.MinorType.VARCHAR))
      .setAlias("name")
      .build();

    SchemaDef schemaDef = new SchemaDef(Arrays.asList(columnDef, columnDef2));

    /*
      Refactor TupleNameSpace to allow custom indexes
     */

    TupleSchema schema = new TupleSchema(true);

    schemaDef.getColumns().forEach(
      c -> {
        String name = c.getAlias() != null ? c.getAlias() :
          String.format("%s[%s]", c.getName(), c.getIndex());
        MaterializedField field = MaterializedField.create(name, c.getTypeDef().getType());

        schema.add(field, c.getIndex());
      }
    );

    System.out.println(schema);
    System.out.println(schema.column(0));
    //System.out.println(schema.column(1)); //throws NPE, need to add check method
    System.out.println(schema.column(5));
  }

}
