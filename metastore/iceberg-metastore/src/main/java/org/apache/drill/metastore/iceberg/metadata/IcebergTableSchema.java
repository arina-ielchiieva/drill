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
package org.apache.drill.metastore.iceberg.metadata;

import org.apache.drill.metastore.MetastoreFieldDefinition;
import org.apache.drill.metastore.iceberg.exceptions.IcebergMetastoreException;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Provides Iceberg table schema and its partition specification.
 */
public class IcebergTableSchema {

  public static final String STORAGE_PLUGIN = "storagePlugin";
  public static final String WORKSPACE = "workspace";
  public static final String TABLE_NAME = "tableName";
  public static final String METADATA_KEY = "metadataKey";

  public static final int STARTING_SCHEMA_INDEX = 1;
  public static final int STARTING_COMPLEX_TYPES_INDEX = 10_000;

  private static final Map<String, org.apache.iceberg.types.Type> JAVA_TO_ICEBERG_TYPE_MAP =
    ImmutableMap.<String, org.apache.iceberg.types.Type>builder()
    .put("string", Types.StringType.get())
    .put("int", Types.IntegerType.get())
    .put("integer", Types.IntegerType.get())
    .put("long", Types.LongType.get())
    .put("float", Types.FloatType.get())
    .put("double", Types.DoubleType.get())
    .put("boolean", Types.BooleanType.get())
    .build();

  private static final Map<String, Integer> PARTITION_KEYS = ImmutableMap.of(
    STORAGE_PLUGIN, 1,
    WORKSPACE, 2,
    TABLE_NAME, 3,
    METADATA_KEY, 4);

  private final Schema metastoreSchema;
  private final PartitionSpec partitionSpec;

  public IcebergTableSchema(Schema metastoreSchema, PartitionSpec partitionSpec) {
    this.metastoreSchema = metastoreSchema;
    this.partitionSpec = partitionSpec;
  }

  /**
   * Based on given class fields annotated with {@link MetastoreFieldDefinition}
   * generates Iceberg table schema and its partition specification.
   *
   * @param clazz base class for Iceberg schema
   * @return instance of Iceberg schema
   */
  public static IcebergTableSchema of(Class<?> clazz) {
    List<Types.NestedField> metastoreSchemaFields = new ArrayList<>();
    Map<Integer, Types.NestedField> partitionSpecSchemaFields = new TreeMap<>();

    int schemaIndex = STARTING_SCHEMA_INDEX;
    int complexTypesIndex = STARTING_COMPLEX_TYPES_INDEX;

    for (Field field : clazz.getDeclaredFields()) {
      if (!field.isAnnotationPresent(MetastoreFieldDefinition.class)) {
        continue;
      }

      String typeSimpleName = field.getType().getSimpleName().toLowerCase();
      org.apache.iceberg.types.Type icebergType = JAVA_TO_ICEBERG_TYPE_MAP.get(typeSimpleName);

      if (icebergType == null && field.getAnnotatedType().getType() instanceof ParameterizedType) {
        Type[] actualTypeArguments = ((ParameterizedType) field.getAnnotatedType().getType()).getActualTypeArguments();
        switch (typeSimpleName) {
          case "list":
            org.apache.iceberg.types.Type listIcebergType = getGenericsType(actualTypeArguments[0]);
            icebergType = Types.ListType.ofOptional(complexTypesIndex++, listIcebergType);
            break;
          case "map":
            org.apache.iceberg.types.Type keyIcebergType = getGenericsType(actualTypeArguments[0]);
            org.apache.iceberg.types.Type valueIcebergType = getGenericsType(actualTypeArguments[1]);
            icebergType = Types.MapType.ofOptional(complexTypesIndex++, complexTypesIndex++, keyIcebergType, valueIcebergType);
            break;
          default:
            throw new IcebergMetastoreException("Unexpected parametrized type: " + typeSimpleName);
        }
      }

      if (icebergType == null) {
        throw new IcebergMetastoreException("Unexpected type: " + typeSimpleName);
      }

      Types.NestedField icebergField = Types.NestedField.optional(schemaIndex++, field.getName(), icebergType);

      metastoreSchemaFields.add(icebergField);

      Integer partitionIndex = PARTITION_KEYS.get(field.getName());
      if (partitionIndex != null) {
        partitionSpecSchemaFields.put(partitionIndex, icebergField);
      }
    }

    Schema partitionSpecSchema = new Schema(new ArrayList<>(partitionSpecSchemaFields.values()));
    return new IcebergTableSchema(new Schema(metastoreSchemaFields), buildPartitionSpec(partitionSpecSchema));
  }

  public Schema metastoreSchema() {
    return metastoreSchema;
  }

  public PartitionSpec partitionSpec() {
    return partitionSpec;
  }

  private static org.apache.iceberg.types.Type getGenericsType(Type type) {
    if (!(type instanceof Class)) {
      throw new IcebergMetastoreException("Unexpected generics type: " + type.getTypeName());
    }
    Class typeArgument = (Class) type;
    String typeSimpleName = typeArgument.getSimpleName().toLowerCase();
    org.apache.iceberg.types.Type icebergType = JAVA_TO_ICEBERG_TYPE_MAP.get(typeSimpleName);
    if (icebergType == null) {
      throw new IcebergMetastoreException("Unexpected type: " + typeSimpleName);
    }
    return icebergType;
  }

  private static PartitionSpec buildPartitionSpec(Schema schema) {
    if (schema.columns().isEmpty()) {
      return PartitionSpec.unpartitioned();
    }
    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
    schema.columns().
      forEach(column -> builder.identity(column.name()));
    return builder.build();
  }
}
