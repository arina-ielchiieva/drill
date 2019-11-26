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

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class TestAvro extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    startCluster(builder);
  }

  @Test
  public void testInterval() throws Exception {
    String file = generateInterval();
    String sql = "select * from dfs.`%s`";
    queryBuilder().sql(sql, file).print();
  }

  public static String generateInterval() throws Exception {
    File file = File.createTempFile("avro-interval", ".avro", dirTestWatcher.getRootDir());

    Schema duration = new LogicalType("duration")
      .addToSchema(SchemaBuilder.builder().fixed("duration_fixed").size(12));

    Schema schema = SchemaBuilder.record("record")
      .fields()
      .name("col_duration").type(duration).noDefault()
      .endRecord();

    Schema durationSchema = schema.getField("col_duration").schema();
    System.out.println(durationSchema);

    DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema));


    writer.create(schema, file);
    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);

      ByteBuffer bb = ByteBuffer.allocate(12);
      bb.order(ByteOrder.LITTLE_ENDIAN);
      bb.putInt(2); // month
      bb.putInt(3); // days
      bb.putInt(10 + i); // milliseconds


      GenericData.Fixed fixed = new GenericData.Fixed(durationSchema, bb.array());
      record.put("col_duration", fixed);
      writer.append(record);
    }
    writer.close();

    DatumReader<GenericContainer> datumReader = new GenericDatumReader<>();
    DataFileReader<GenericContainer> reader = new DataFileReader<>(file, datumReader);
    Schema readerSchema = reader.getSchema();
    System.out.println("READER SCHEMA:");
    System.out.println(readerSchema);

    return file.getName();
  }

  //todo test fixed
}
