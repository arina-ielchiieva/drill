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
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.util.Utf8;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.DictWriter;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.security.UserGroupInformation;
import org.joda.time.DateTimeConstants;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;

public class AvroBatchReader implements ManagedReader<FileScanFramework.FileSchemaNegotiator> {

  private static final Logger logger = LoggerFactory.getLogger(AvroBatchReader.class);

  // currently config is unused but maybe used later
  private final AvroReaderConfig config;

  private Path filePath;
  private long endPosition;
  private DataFileReader<GenericContainer> reader;
  private ResultSetLoader loader;
  // re-use container instance
  private GenericContainer container = null;

  public AvroBatchReader(AvroReaderConfig config) {
    this.config = config;
  }

  @Override
  public boolean open(FileScanFramework.FileSchemaNegotiator negotiator) {
    FileSplit split = negotiator.split();
    filePath = split.getPath();

    // Avro files are splittable, define reading start / end positions
    long startPosition = split.getStart();
    endPosition = startPosition + split.getLength();

    logger.debug("Processing Avro file: {}, start position: {}, end position: {}",
      filePath, startPosition, endPosition);

    reader = prepareReader(split, negotiator.fileSystem(),
      negotiator.userName(), negotiator.context().getFragmentContext().getQueryUserName());

    logger.debug("Avro file schema: {}", reader.getSchema());
    TupleMetadata schema = AvroSchemaUtil.convert(reader.getSchema());
    logger.debug("Avro file converted schema: {}", schema);
    negotiator.setTableSchema(schema, true);
    loader = negotiator.build();

    return true;
  }

  @Override
  public boolean next() {
    RowSetLoader rowWriter = loader.writer();

    while (!rowWriter.isFull()) {
      if (!nextLine(rowWriter)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void close() {
    try {
      reader.close();
    } catch (IOException e) {
      logger.warn("Error closing Avro reader: {}", e.getMessage(), e);
    } finally {
      reader = null;
    }
  }

  @Override
  public String toString() {
    long currentPosition = -1L;
    try {
      if (reader != null) {
        currentPosition = reader.tell();
      }
    } catch (IOException e) {
      logger.trace("Unable to obtain Avro reader position: {}", e.getMessage(), e);
    }
    return "AvroBatchReader[File=" + filePath
      + ", Position=" + currentPosition
      + "]";
  }

  /**
   * Initialized Avro data reader based on given file system and file path.
   * Moves reader to the sync point from where to start reading the data.
   *
   * @param fileSplit file split
   * @param fs file system
   * @param opUserName name of the user whom to impersonate while reading the data
   * @param queryUserName name of the user who issues the query
   * @return Avro file reader
   */
  private DataFileReader<GenericContainer> prepareReader(FileSplit fileSplit, FileSystem fs, String opUserName, String queryUserName) {
    try {
      UserGroupInformation ugi = ImpersonationUtil.createProxyUgi(opUserName, queryUserName);
      DataFileReader<GenericContainer> reader = ugi.doAs((PrivilegedExceptionAction<DataFileReader<GenericContainer>>) () ->
        new DataFileReader<>(new FsInput(fileSplit.getPath(), fs.getConf()), new GenericDatumReader<GenericContainer>()));

      // move to sync point from where to read the file
      reader.sync(fileSplit.getStart());
      return reader;
    } catch (IOException | InterruptedException e) {
      throw UserException.dataReadError(e)
        .message("Error preparing Avro reader")
        .addContext("Reader", this)
        .build(logger);
    }
  }

  private boolean nextLine(RowSetLoader rowWriter) {
    try {
      if (!reader.hasNext() || reader.pastSync(endPosition)) {
        return false;
      }
      container = reader.next(container);
    } catch (IOException e) {
      throw UserException.dataReadError(e)
        .addContext("Reader", this)
        .build(logger);
    }

    Schema schema = container.getSchema();
    GenericRecord record = (GenericRecord) container;

    if (Schema.Type.RECORD != schema.getType()) {
      throw UserException.dataReadError()
        .message("Root object must be record type. Found: %s", schema.getType())
        .addContext("Reader", this)
        .build(logger);
    }

    rowWriter.start();
    List<Schema.Field> fields = schema.getFields();
    for (Schema.Field field : fields) {
      String fieldName = field.name();
      Object value = record.get(fieldName);
      ObjectWriter writer = rowWriter.column(fieldName);
      processRecord(writer, value, field.schema());
    }
    rowWriter.save();
    return true;
  }

  private void processRecord(ObjectWriter writer, Object value, Schema schema) {
    // skip processing record if it is null or is not projected
    if (value == null || !writer.isProjected()) {
      return;
    }

    switch (schema.getType()) {
      case UNION:
        processRecord(writer, value, AvroSchemaUtil.extractSchemaFromNullable(schema, writer.schema().name()));
        break;
      case RECORD:
        TupleWriter tupleWriter = writer.tuple();

        if (tupleWriter.tupleSchema().isEmpty()) {
          // fill in tuple schema for cases when there recursive named record types are present
          TupleMetadata recordSchema = AvroSchemaUtil.convert(schema);
          recordSchema.toMetadataList().forEach(tupleWriter::addColumn);
        }

        GenericRecord genericRecord = (GenericRecord) value;
        schema.getFields().forEach(
          field -> processRecord(tupleWriter.column(field.name()), genericRecord.get(field.name()), field.schema())
        );
        break;
      case ARRAY:
        ArrayWriter arrayWriter = writer.array();
        GenericArray<?> array = (GenericArray<?>) value;
        ObjectWriter entryWriter = arrayWriter.entry();
        for (Object arrayValue : array) {
          processRecord(entryWriter, arrayValue, array.getSchema().getElementType());
          arrayWriter.save();
        }
        break;

      case MAP:
        @SuppressWarnings("unchecked")
        Map<Object, Object> map = (Map<Object, Object>) value;
        Schema valueSchema = schema.getValueType();

        DictWriter dictWriter = writer.dict();
        ScalarWriter keyWriter = dictWriter.keyWriter();
        ObjectWriter valueWriter = dictWriter.valueWriter();

        for (Map.Entry<Object, Object> mapEntry : map.entrySet()) {
          processScalar(keyWriter, mapEntry.getKey());
          processRecord(valueWriter, mapEntry.getValue(), valueSchema);
          dictWriter.save();
        }
        break;

      default:
        try {
          ScalarWriter scalarWriter = writer.scalar();
          processScalar(scalarWriter, value);
        } catch (UnsupportedOperationException e) {
          throw UserException.dataReadError(e)
            .message("Unexpected writer type '%s', expected scalar", writer.type())
            .addContext("Reader", this)
            .build(logger);
        }
    }
  }

  private void processScalar(ScalarWriter scalarWriter, Object value) {
    ColumnMetadata columnMetadata = scalarWriter.schema();
    switch (columnMetadata.type()) {
      case INT:
        scalarWriter.setInt((int) value);
        break;
      case BIGINT:
        scalarWriter.setLong((long) value);
        break;
      case FLOAT4:
        scalarWriter.setDouble((float) value);
        break;
      case FLOAT8:
        scalarWriter.setDouble((double) value);
        break;
      case VARDECIMAL:
        BigInteger bigInteger;
        if (value instanceof ByteBuffer) {
          ByteBuffer decBuf = (ByteBuffer) value;
          bigInteger = new BigInteger(decBuf.array());
        } else {
          GenericFixed genericFixed = (GenericFixed) value;
          bigInteger = new BigInteger(genericFixed.bytes());
        }
        BigDecimal decimalValue = new BigDecimal(bigInteger, columnMetadata.scale());
        scalarWriter.setDecimal(decimalValue);
        break;
      case BIT:
        scalarWriter.setBoolean((boolean) value);
        break;
      case VARCHAR:
        byte[] binary;
        int length;
        if (value instanceof Utf8) {
          Utf8 utf8 = (Utf8) value;
          binary = utf8.getBytes();
          length = utf8.getByteLength();
        } else {
          binary = value.toString().getBytes(Charsets.UTF_8);
          length = binary.length;
        }
        scalarWriter.setBytes(binary, length);
        break;
      case VARBINARY:
        if (value instanceof ByteBuffer) {
          ByteBuffer buf = (ByteBuffer) value;
          scalarWriter.setBytes(buf.array(), buf.remaining());
        } else {
          byte[] bytes = ((GenericFixed) value).bytes();
          scalarWriter.setBytes(bytes, bytes.length);
        }
        break;
      case TIMESTAMP:
        String avroLogicalType = columnMetadata.property(AvroSchemaUtil.AVRO_LOGICAL_TYPE_PROPERTY);
        if (AvroSchemaUtil.TIMESTAMP_MILLIS_LOGICAL_TYPE.equals(avroLogicalType)) {
          scalarWriter.setLong((long) value);
        } else {
          scalarWriter.setLong((long) value / 1000);
        }
        break;
      case DATE:
        scalarWriter.setLong((int) value * (long) DateTimeConstants.MILLIS_PER_DAY);
        break;
      case TIME:
        if (value instanceof Long) {
          scalarWriter.setInt((int) ((long) value / 1000));
        } else {
          scalarWriter.setInt((int) value);
        }
        break;
      case INTERVAL:
        GenericFixed genericFixed = (GenericFixed) value;
        IntBuffer intBuf = ByteBuffer
          .wrap(genericFixed.bytes())
          .order(ByteOrder.LITTLE_ENDIAN)
          .asIntBuffer();

        Period period = Period
          .months(intBuf.get(0))
          .withDays(intBuf.get(1))
          .withMillis(intBuf.get(2));

        scalarWriter.setPeriod(period);
        break;
      default:
        throw UserException.dataReadError()
          .message("Unexpected scalar schema type: ", columnMetadata.type())
          .addContext("Column", columnMetadata)
          .addContext("Reader", this)
          .build(logger);
    }
  }

  public static class AvroReaderConfig {

    private final AvroFormatPlugin plugin;

    public AvroReaderConfig(AvroFormatPlugin plugin) {
      this.plugin = plugin;
    }

    public AvroFormatPlugin getPlugin() {
      return plugin;
    }
  }
}
