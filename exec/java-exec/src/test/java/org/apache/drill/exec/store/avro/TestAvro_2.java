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
package org.apache.drill.exec.store.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

public class TestAvro_2 extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    startCluster(builder);
  }

  @Test
  public void testSimple() throws Exception {
    String file = generateStringAndUtf8Data().getFileName();
    //String sql = "select count(*) as row_count from dfs.`root`.`%s`";
    String sql = "select * from dfs.`root`.`%s`";
    queryBuilder().sql(sql, file).print();
  }

  @Test
  public void testSimple_projection() throws Exception {
    String file = generateStringAndUtf8Data().getFileName();
    //String sql = "select count(*) as row_count from dfs.`root`.`%s`";
    String sql = "select a_string from dfs.`root`.`%s`";
    queryBuilder().sql(sql, file).print();
  }

  @Test
  public void testArraySimple() throws Exception {
    String file = generateSimpleArraySchema_NoNullValues().getFileName();
    String sql = "select * from dfs.`%s`";
    queryBuilder().sql(sql, file).print();
  }

  @Test
  public void testArrayComplex() throws Exception {
    String file = generateNestedArraySchema(1, 3).getFileName();
    String sql = "select * from dfs.`%s`";
    queryBuilder().sql(sql, file).print();
  }

  @Test
  public void testUnion() throws Exception {
    String file = generateUnionSchema_WithNonNullValues().getFileName();
    String sql = "select * from dfs.`%s`";
    queryBuilder().sql(sql, file).print();
  }

  @Test
  public void testLinkedList() throws Exception {
    String file = generateLinkedList();
    String sql = "select * from dfs.`%s`";
    queryBuilder().sql(sql, file).print();
  }

  @Test
  public void testLinkedList_Complex() throws Exception {
    String file = generateLinkedList_Complex();
    String sql = "select * from dfs.`%s`";
    queryBuilder().sql(sql, file).print();
  }

  @Test
  public void testRecord_projection() throws Exception {
    String file = generateRecordSchema();
    String sql = "select t.rec.i from dfs.`%s` t";
    queryBuilder().sql(sql, file).print();
  }

  @Test
  public void testFixed() {
    // SpecificFixed - avro type for the record
  }

  public static final int RECORD_COUNT = 10;

  public static AvroTestUtil.AvroTestRecordWriter generateStringAndUtf8Data() throws Exception {

    final Schema schema = SchemaBuilder.record("AvroRecordReaderTest")
      .namespace("org.apache.drill.exec.store.avro").fields()
      .name("a_string").type().optional().stringBuilder().prop("avro.java.string", "String").endString()
      //.noDefault()
      .name("b_utf8").type().optional().stringType()
      //.noDefault()
      .endRecord();

    final File file = File.createTempFile("avro-primitive-test", ".avro", dirTestWatcher.getRootDir());
    final AvroTestUtil.AvroTestRecordWriter record = new AvroTestUtil.AvroTestRecordWriter(schema, file);
    try {

      ByteBuffer bb = ByteBuffer.allocate(1);
      bb.put(0, (byte) 1);

      for (int i = 0; i < RECORD_COUNT; i++) {
        record.startRecord();
        if (i == 4) {
          record.put("a_string", null);
          record.put("b_utf8", null);
        } else {
          record.put("a_string", "a_" + i);
          record.put("b_utf8", "b_" + i);
        }
        record.endRecord();
      }
    } finally {
      record.close();
    }

    return record;
  }

  public static int ARRAY_SIZE = 20;

  public static AvroTestUtil.AvroTestRecordWriter generateSimpleArraySchema_NoNullValues() throws Exception {
    final File file = File.createTempFile("avro-array-test", ".avro", dirTestWatcher.getRootDir());
    final Schema schema = SchemaBuilder.record("AvroRecordReaderTest").namespace("org.apache.drill.exec.store.avro").fields().name("a_string").type().stringType().noDefault().name("b_int").type().intType().noDefault().name("c_string_array").type().array().items().stringType().noDefault().name("d_int_array").type().array().items().intType().noDefault().name("e_float_array").type().array().items().floatType().noDefault().endRecord();

    final AvroTestUtil.AvroTestRecordWriter record = new AvroTestUtil.AvroTestRecordWriter(schema, file);
    try {
      for (int i = 0; i < RECORD_COUNT; i++) {
        record.startRecord();
        record.put("a_string", "a_" + i);
        record.put("b_int", i);
        {
          GenericArray<String> array = new GenericData.Array<>(ARRAY_SIZE, schema.getField("c_string_array").schema());
          for (int j = 0; j < ARRAY_SIZE; j++) {
            array.add(j, "c_string_array_" + i + "_" + j);
          }
          record.put("c_string_array", array);
        }
        {
          GenericArray<Integer> array = new GenericData.Array<>(ARRAY_SIZE, schema.getField("d_int_array").schema());
          for (int j = 0; j < ARRAY_SIZE; j++) {
            array.add(j, i * j);
          }
          record.put("d_int_array", array);
        }
        {
          GenericArray<Float> array = new GenericData.Array<>(ARRAY_SIZE, schema.getField("e_float_array").schema());
          for (int j = 0; j < ARRAY_SIZE; j++) {
            array.add(j, (float) (i * j));
          }
          record.put("e_float_array", array);
        }
        record.endRecord();
      }

    } finally {
      record.close();
    }
    return record;
  }


  public static AvroTestUtil.AvroTestRecordWriter generateNestedArraySchema(int numRecords, int numArrayItems) throws IOException {
    final File file = File.createTempFile("avro-nested-test", ".avro", dirTestWatcher.getRootDir());
    final Schema schema = SchemaBuilder.record("AvroRecordReaderTest").namespace("org.apache.drill.exec.store.avro")
      .fields()
      .name("a_int").type().intType().noDefault()
      .name("b_array").type().array().items()
      .record("my_record_1").namespace("foo.blah.org")
      .fields()
      .name("nested_1_int").type().optional().intType()
      .endRecord()
      .arrayDefault(Collections.emptyList())
      .endRecord();

    final Schema arraySchema = schema.getField("b_array").schema();
    final Schema itemSchema = arraySchema.getElementType();

    final AvroTestUtil.AvroTestRecordWriter record = new AvroTestUtil.AvroTestRecordWriter(schema, file);
    try {
      for (int i = 0; i < numRecords; i++) {
        record.startRecord();
        record.put("a_int", i);
        GenericArray<GenericRecord> array = new GenericData.Array<>(ARRAY_SIZE, arraySchema);

        for (int j = 0; j < numArrayItems; j++) {
          final GenericRecord nestedRecord = new GenericData.Record(itemSchema);
          nestedRecord.put("nested_1_int", j);
          array.add(nestedRecord);
        }
        record.put("b_array", array);
        record.endRecord();
      }
    } finally {
      record.close();
    }

    return record;
  }


  public static AvroTestUtil.AvroTestRecordWriter generateUnionSchema_WithNonNullValues() throws Exception {
    final Schema schema = SchemaBuilder.record("AvroRecordReaderTest")
      .namespace("org.apache.drill.exec.store.avro")
      .fields()
      .name("i_union").type().unionOf().doubleType().and().longType().endUnion().noDefault()
      .endRecord();

    final File file = File.createTempFile("avro-primitive-test", ".avro", dirTestWatcher.getRootDir());
    final AvroTestUtil.AvroTestRecordWriter record = new AvroTestUtil.AvroTestRecordWriter(schema, file);
    try {

      ByteBuffer bb = ByteBuffer.allocate(1);
      bb.put(0, (byte) 1);

      for (int i = 0; i < RECORD_COUNT; i++) {
        record.startRecord();
        record.put("i_union", (i % 2 == 0 ? (double) i : (long) i));
        record.endRecord();
      }
    } finally {
      record.close();
    }

    return record;
  }

  public static String generateLinkedList() throws Exception {
    final File file = File.createTempFile("avro-linkedlist", ".avro", dirTestWatcher.getRootDir());
    final Schema schema = SchemaBuilder.record("LongList")
      .namespace("org.apache.drill.exec.store.avro")
      .aliases("LinkedLongs")
      .fields()
      .name("value").type().optional().longType()
      .name("next").type().optional().type("LongList")
      .endRecord();

    final DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema));
    writer.create(schema, file);
    GenericRecord previousRecord = null;
    try {
      for (int i = 0; i < RECORD_COUNT; i++) {
        GenericRecord record = (GenericRecord) (previousRecord == null ? new GenericData.Record(schema) : previousRecord.get("next"));
        record.put("value", (long) i);
        if (previousRecord != null) {
          writer.append(previousRecord);
        }
        GenericRecord nextRecord = new GenericData.Record(record.getSchema());
        record.put("next", nextRecord);
        previousRecord = record;
      }
      writer.append(previousRecord);
    } finally {
      writer.close();
    }

    return file.getName();
  }

  public static String generateLinkedList_Complex() throws Exception {
    final File file = File.createTempFile("avro-linkedlist-complex", ".avro", dirTestWatcher.getRootDir());
    final Schema schema = SchemaBuilder.record("StringList")
      .namespace("org.apache.drill.exec.store.avro")
      .fields()
      .name("a").type().optional().stringType()
      .name("prev").type().optional().type("StringList")
      .name("next").type().optional().type("StringList")
      .name("d_array").type().optional().array().items()
        .nullable().type("StringList")
//      .name("a_record").type()
//        .record("record_type")
//      //.namespace("abc")
//        .fields()
//        .optionalBoolean("a_boolean_nullable")
//        .requiredBoolean("a_boolean") //todo
//        .name("bbb").type().optional().type("record_type")
//        .endRecord()
//      .noDefault()
      .endRecord();

    DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema));
    writer.create(schema, file);
    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("a", "a_" + i);
      GenericRecord prev_record = new GenericData.Record(schema);
      prev_record.put("a", "prev_a_" + i);
      record.put("prev", prev_record);
      GenericRecord next_record = new GenericData.Record(schema);
      next_record.put("a", "next_a_" + i);
      record.put("next", next_record);

      Schema arraySchema = schema.getField("d_array").schema();
      //Schema itemSchema = arraySchema.getElementType();

      GenericArray<GenericRecord> array = new GenericData.Array<>(2, arraySchema.getTypes().get(1));
      array.add(prev_record);
      array.add(next_record);
      record.put("d_array", array);
      writer.append(record);
    }
    writer.close();

//    GenericRecord previousRecord = null;
//    try {
//      for (int i = 0; i < RECORD_COUNT; i++) {
//        GenericRecord record = (GenericRecord) (previousRecord == null
//          ? new GenericData.Record(schema)
//          : previousRecord.get("next"));
//        record.put("value", (long) i);
//        if (previousRecord != null) {
//          writer.append(previousRecord);
//        }
//        GenericRecord nextRecord = new GenericData.Record(record.getSchema());
//        record.put("next", nextRecord);
//        previousRecord = record;
//      }
//      writer.append(previousRecord);
//    } finally {
//      writer.close();
//    }

    return file.getName();
  }

  public static String generateRecordSchema() throws Exception {
    File file = File.createTempFile("avro-record_schema", ".avro", dirTestWatcher.getRootDir());
    Schema schema = SchemaBuilder.record("record")
      .namespace("org.apache.drill.exec.store.avro")
      .fields()
      .name("rec").type().record("nested_record")
        .fields()
          .requiredString("s")
          .optionalInt("i")
        .endRecord().noDefault()
      .endRecord();


    Schema recSchema = schema.getField("rec").schema();

    DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema));
    writer.create(schema, file);
    for (int i = 0; i < 5; i++) {
      GenericRecord rec = new GenericData.Record(recSchema);
      rec.put("s", "val_"+ i);
      rec.put("i", i);

      GenericRecord mainRecord = new GenericData.Record(schema);
      mainRecord.put("rec", rec);
      writer.append(mainRecord);
    }
    writer.close();

    return file.getName();
  }

}


/*
  @Test
  public void testCountStar() throws Exception {
    final String file = generateStringAndUtf8Data().getFileName();
    final String sql = "select count(*) as row_count from dfs.`%s`";
    testBuilder()
        .sqlQuery(sql, file)
        .ordered()
        .baselineColumns("row_count")
        .baselineValues((long) RECORD_COUNT)
        .go();
  }

 */
