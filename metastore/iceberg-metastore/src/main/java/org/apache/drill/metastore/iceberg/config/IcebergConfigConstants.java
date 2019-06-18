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
package org.apache.drill.metastore.iceberg.config;

import org.apache.drill.metastore.config.MetastoreConfigConstants;

/**
 * Drill Iceberg Metastore configuration which is defined
 * in {@link MetastoreConfigConstants#MODULE_RESOURCE_FILE_NAME} file.
 */
public interface IcebergConfigConstants {

  /**
   * Drill Iceberg configuration properties namespace.
   */
  String BASE = MetastoreConfigConstants.BASE + "iceberg.";

  /**
   * Defines file system configuration properties which can set
   * using {@link org.apache.hadoop.conf.Configuration} class.
   */
  String CONFIG_PROPERTIES = BASE + "config.properties";

  /**
   * Iceberg table config location namespace.
   */
  String LOCATION_NAMESPACE = BASE  + "location.";

  /**
   * Defines Iceberg table base path.
   */
  String BASE_PATH = LOCATION_NAMESPACE + "base_path";

  /**
   * Defines Iceberg table relative path.
   */
  String RELATIVE_PATH = LOCATION_NAMESPACE + "relative_path";

  /**
   * Defines Iceberg table properties. List of available table properties is defined
   * in {@link org.apache.iceberg.TableProperties} class.
   */
  String TABLE_PROPERTIES = BASE + "table.properties";
}
