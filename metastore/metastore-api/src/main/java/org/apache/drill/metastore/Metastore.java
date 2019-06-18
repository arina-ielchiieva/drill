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
package org.apache.drill.metastore;

import org.apache.drill.metastore.expressions.FilterExpression;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Drill Metastore interface contains methods needed to be implemented by Metastore implementations.
 *
 * Drill Metastore main goal is to read and write Metastore data. Each row is represented
 * in a form of {@link MetadataUnit} class which is a generic representation of Metastore metadata
 * suitable to any Metastore metadata type (table, segment, file, row group, partition).
 *
 * Besides implementing {@link Metastore}, Metastore implementation must have constructor
 * which accepts {@link org.apache.drill.common.config.DrillConfig}.
 */
public interface Metastore {

  /**
   * Depending on Metastore implementation will return Drill Metastore instance.
   * For example, for Iceberg Metastore which stores Metastore data in Iceberg table, caller always
   * needs to receive new instance of Iceberg table; for other implementations returning the same instance
   * may be more sufficient to avoid multiple initialization.
   *
   * @return Drill Metastore instance
   */
  Metastore get();

  /**
   * @return new instance of Drill Metastore Metadata interface implementation
   */
  Metadata metadata();

  /**
   * @return new instance of Drill Metastore Read interface implementation
   */
  Read read();

  /**
   * @return new instance of Drill Metastore Modify interface implementation
   */
  Modify modify();

  /**
   * @return new basic requests instance that provides methods to make most frequent
   * calls to Metastore in order to obtain metadata needed for analysis
   */
  default BasicRequests basicRequests() {
    return new BasicRequests(this);
  }

  /**
   * Provides Metastore implementation metadata, including information about versioning support if any
   * and current properties applicable to the Metastore instance.
   */
  interface Metadata {

    int UNDEFINED = -1;

    /**
     * Indicates if Metastore supports versioning,
     * i.e. Metastore version is changed each time write operation is executed.
     *
     * @return true if Metastore supports versioning, false otherwise
     */
    boolean supportsVersioning();

    /**
     * Depending on Metastore implementation, Metastore may have version which can be used to determine
     * if anything has changed during last call to the Metastore.
     * If Metastore implementation, supports versioning, version is changed each time Metastore data has changed.
     * {@link #supportsVersioning()} indicates if Metastore supports versioning.
     * If Metastore does not support versioning, {@link #UNDEFINED} is returned.
     *
     * @return current metastore version
     */
    default long version() {
      return UNDEFINED;
    }

    /**
     * Depending on Metastore implementation, it may have properties.
     * If Metastore supports properties, map with properties names and values are returned,
     * otherwise empty map is returned.
     *
     * @return Metastore implementation properties
     */
    default Map<String, String> properties() {
      return Collections.emptyMap();
    }
  }


  /**
   * Drill Metastore Read interface that contains methods to be implemented in order
   * to provide read functionality from Metastore.
   */
  interface Read {

    /**
     * Provides filter expression by which metastore data will be filtered.
     * If filter expression is not indicated, all Metastore data will be read.
     *
     * @param filter filter expression
     * @return current instance of Read interface implementation
     */
    Read filter(FilterExpression filter);

    /**
     * Provides list of columns to be read from Metastore.
     * If no columns are indicated, all columns will be read.
     * Depending on Metastore implementation, providing list of columns to be read,
     * can improve retrieval performance.
     *
     * @param columns list of columns to be read from Metastore
     * @return current instance of Read interface implementation
     */
    Read columns(List<String> columns);

    default Read columns(String... columns) {
      return columns(Arrays.asList(columns));
    }

    /**
     * Executes read operation from Metastore, returns obtained result in a form
     * of list of {@link MetadataUnit}s which later can be transformed into suitable format.
     *
     * @return list of {@link MetadataUnit}s
     */
    List<MetadataUnit> execute();
  }

  /**
   * Drill Metastore Modify interface that contains methods to be implemented in order
   * to provide modify functionality in the Metastore.
   */
  interface Modify {

    /**
     * Adds overwrite operation for the Metastore, can be used to add new table data
     * or replace partially / fully existing. For example, if one of the table segments has changed,
     * all this segment data and table general information must be replaced with updated data.
     * Thus provided units must include updated data, filter by which existing data will be overwritten
     * will be determined based on given data.
     *
     * @param units metadata units to be overwritten
     * @return current instance of Modify interface implementation
     */
    Modify overwrite(List<MetadataUnit> units);

    default Modify overwrite(MetadataUnit... units) {
      return overwrite(Arrays.asList(units));
    }

    /**
     * Adds delete operation for the Metastore based on the given filter expression.
     * For example, table has two segments and data for one of the segments needs to be deleted.
     * Thus filter must be based on unique identifier of the top-level segment:
     * storagePlugin = 'dfs' and workspace = 'tmp' and tableName = 'nation' and metadataKey = 'part_int=3'
     *
     * @param filter filter expression
     * @return current instance of Modify interface implementation
     */
    Modify delete(FilterExpression filter);

    /**
     * Deletes all data from the Metastore.
     *
     * @return current instance of Modify interface implementation
     */
    Modify purge();

    /**
     * Executes list of provided metastore operations in one transaction if Metastore implementation
     * supports transactions, otherwise executes operations consecutively.
     */
    void execute();
  }
}
