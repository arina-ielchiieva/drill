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

import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.DictWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.joda.time.DateTimeConstants;
import org.joda.time.Period;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.IntStream;

public interface Converter {

  void convert(Object value);

  class DummyConverter implements Converter {

    public static final DummyConverter INSTANCE = new DummyConverter();

    @Override
    public void convert(Object value) {
      // do nothing
    }
  }

  class ScalarConverter implements Converter {

    private final Consumer<Object> valueConverter;

    public ScalarConverter(Consumer<Object> valueConverter) {
      this.valueConverter = valueConverter;
    }

    public static ScalarConverter init(ScalarWriter writer) {
      ColumnMetadata columnMetadata = writer.schema();
      switch (columnMetadata.type()) {
        case VARCHAR:
          return new ScalarConverter(value -> {
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
            writer.setBytes(binary, length);
          });
        case VARBINARY:
          return new ScalarConverter(value -> {
            if (value instanceof ByteBuffer) {
              ByteBuffer buf = (ByteBuffer) value;
              writer.setBytes(buf.array(), buf.remaining());
            } else {
              byte[] bytes = ((GenericFixed) value).bytes();
              writer.setBytes(bytes, bytes.length);
            }
          });
        case VARDECIMAL:
          return new ScalarConverter(value -> {
            BigInteger bigInteger;
            if (value instanceof ByteBuffer) {
              ByteBuffer decBuf = (ByteBuffer) value;
              bigInteger = new BigInteger(decBuf.array());
            } else {
              GenericFixed genericFixed = (GenericFixed) value;
              bigInteger = new BigInteger(genericFixed.bytes());
            }
            BigDecimal decimalValue = new BigDecimal(bigInteger, writer.schema().scale());
            writer.setDecimal(decimalValue);
          });
        case TIMESTAMP:
          return new ScalarConverter(value -> {
            String avroLogicalType = writer.schema().property(AvroSchemaUtil.AVRO_LOGICAL_TYPE_PROPERTY);
            if (AvroSchemaUtil.TIMESTAMP_MILLIS_LOGICAL_TYPE.equals(avroLogicalType)) {
              writer.setLong((long) value);
            } else {
              writer.setLong((long) value / 1000);
            }
          });
        case DATE:
          return new ScalarConverter(value -> writer.setLong((int) value * (long) DateTimeConstants.MILLIS_PER_DAY));
        case TIME:
          return new ScalarConverter(value -> {
            if (value instanceof Long) {
              writer.setInt((int) ((long) value / 1000));
            } else {
              writer.setInt((int) value);
            }
          });
        case INTERVAL:
          return new ScalarConverter(value -> {
            GenericFixed genericFixed = (GenericFixed) value;
            IntBuffer intBuf = ByteBuffer.wrap(genericFixed.bytes())
              .order(ByteOrder.LITTLE_ENDIAN)
              .asIntBuffer();

            Period period = Period.months(intBuf.get(0))
              .withDays(intBuf.get(1)
              ).withMillis(intBuf.get(2));

            writer.setPeriod(period);
          });
        default:
          return new ScalarConverter(writer::setObject);
      }
    }

    @Override
    public void convert(Object value) {
      if (value == null) {
        return;
      }

      valueConverter.accept(value);
    }
  }

  class ArrayConverter implements Converter {

    private final ArrayWriter arrayWriter;
    private final Converter valueConverter;

    public ArrayConverter(ArrayWriter arrayWriter, Converter valueConverter) {
      this.arrayWriter = arrayWriter;
      this.valueConverter = valueConverter;
    }

    @Override
    public void convert(Object value) {
      if (value == null || !arrayWriter.isProjected()) {
        return;
      }

      GenericArray<?> array = (GenericArray<?>) value;
      array.forEach(arrayValue -> {
        valueConverter.convert(arrayValue);
        arrayWriter.save();
      });
    }
  }

  class MapConverter implements Converter {

    private final TupleWriter tupleWriter;
    private final List<Converter> converters;

    public MapConverter(TupleWriter tupleWriter, List<Converter> converters) {
      this.tupleWriter = tupleWriter;
      this.converters = new ArrayList<>(converters);
    }

    @Override
    public void convert(Object value) {
      if (value == null) {
        return;
      }

      GenericRecord genericRecord = (GenericRecord) value;

      if (converters.isEmpty()) {
        // fill in tuple schema for cases when it contains recursive named record types
        TupleMetadata metadata = AvroSchemaUtil.convert(genericRecord.getSchema());
        metadata.toMetadataList().forEach(tupleWriter::addColumn);

        IntStream.range(0, metadata.size())
          .mapToObj(i -> ConvertersUtil.getConverter(metadata.metadata(i), tupleWriter.column(i)))
          .forEach(converters::add);
      }

      IntStream.range(0, converters.size())
        .forEach(i -> converters.get(i).convert(genericRecord.get(i)));
    }
  }

  class DictConverter implements Converter {

    private final DictWriter dictWriter;
    private final Converter keyConverter;
    private final Converter valueConverter;

    public DictConverter(DictWriter dictWriter, Converter keyConverter, Converter valueConverter) {
      this.dictWriter = dictWriter;
      this.keyConverter = keyConverter;
      this.valueConverter = valueConverter;
    }

    @Override
    public void convert(Object value) {
      if (value == null) {
        return;
      }

      @SuppressWarnings("unchecked") Map<Object, Object> map = (Map<Object, Object>) value;
      map.forEach((key, val) -> {
        keyConverter.convert(key);
        valueConverter.convert(val);
        dictWriter.save();
      });
    }
  }
}
