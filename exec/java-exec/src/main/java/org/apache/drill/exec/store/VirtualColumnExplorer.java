/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store;

import com.google.common.base.Enums;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VirtualColumnExplorer {

  private final String partitionDesignator;
  private final List<SchemaPath> columns;
  private final boolean selectAllColumns;
  private final List<Integer> selectedPartitionColumns;
  private final List<String> implicitColumns;
  private final List<SchemaPath> tableColumns;

  /**
   * Helper class that encapsulates logic for sorting out columns
   * between actual table columns, partition columns and file implicit columns.
   * Also populates map with virtual columns names as keys and their values
   */
  public VirtualColumnExplorer(FragmentContext context, List<SchemaPath> columns) {
    this.partitionDesignator = context.getOptions()
     .getOption(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL).string_val;
    this.columns = columns;
    this.selectAllColumns = columns != null && AbstractRecordReader.isStarQuery(columns);
    this.selectedPartitionColumns = Lists.newArrayList();
    this.implicitColumns = Lists.newArrayList();
    this.tableColumns = Lists.newArrayList();

    init();
  }

  /**
   * If it's not select all query, sorts out columns into three categories:
   * 1. table columns
   * 2. partition columns
   * 3. file implicit columns
   */
  private void init() {
    if (selectAllColumns) {
      implicitColumns.addAll(EnumUtils.getEnumMap(ImplicitFileColumns.class).keySet());
    } else {
      Pattern pattern = Pattern.compile(String.format("%s[0-9]+", partitionDesignator));
      for (SchemaPath column : columns) {
        String path = column.getAsUnescapedPath();
        Matcher m = pattern.matcher(path);
        if (m.matches()) {
          selectedPartitionColumns.add(Integer.parseInt(path.substring(partitionDesignator.length())));
        } else if (Enums.getIfPresent(ImplicitFileColumns.class, path.toUpperCase()).isPresent()) {
          implicitColumns.add(path);
        } else {
          tableColumns.add(column);
        }
      }

      // We must make sure to pass a table column(not to be confused with partition column) to the underlying record
      // reader.
      if (tableColumns.size() == 0) {
        tableColumns.add(AbstractRecordReader.STAR_COLUMN);
      }
    }
  }

  /**
   * Compares selection root and actual file path to determine partition columns values.
   * Adds implicit columns according to implicit columns list.
   * @return map with columns names as keys and their values
   */
  public Map<String, String> populateVirtualColumns(FileWork work, String selectionRoot) {
    Map<String, String> virtualValues = Maps.newLinkedHashMap();
    if (selectionRoot != null) {
      String[] r = Path.getPathWithoutSchemeAndAuthority(new Path(selectionRoot)).toString().split("/");
      Path path = Path.getPathWithoutSchemeAndAuthority(new Path(work.getPath()));
      String[] p = path.toString().split("/");
      if (p.length > r.length) {
        String[] q = ArrayUtils.subarray(p, r.length, p.length - 1);
        for (int a = 0; a < q.length; a++) {
          if (selectAllColumns || selectedPartitionColumns.contains(a)) {
            virtualValues.put(partitionDesignator + a, q[a]);
          }
        }
      }
      //add implicit file columns
      virtualValues.putAll(ImplicitFileColumns.toMap(path, implicitColumns));
    }
    return virtualValues;
  }

  public boolean isSelectAllColumns() {
    return selectAllColumns;
  }

  public List<SchemaPath> getTableColumns() {
    return tableColumns;
  }

  /**
   * Columns that give information about file data comes from.
   * Columns are implicit, so should be called explicitly in query
   */
  public enum ImplicitFileColumns {

    /**
     * Fully qualified name, contains full path to file and file name
     */
    FQN {
      @Override
      public String getValue(Path path) {
        return path.toString();
      }
    },

    /**
     * Full path to file without file name
     */
    DIRNAME {
      @Override
      public String getValue(Path path) {
        return path.getParent().toString();
      }
    },

    /**
     * File name with extension without path
     */
    FILENAME {
      @Override
      public String getValue(Path path) {
        return path.getName();
      }
    },

    /**
     * File suffix (without dot at the beginning)
     */
    SUFFIX {
      @Override
      public String getValue(Path path) {
        return Files.getFileExtension(path.getName());
      }
    };

    /**
     * Using list of required implicit file name columns
     * creates map where key is column name from list,
     * value is result of ImplicitFileColumnsEnum getValue method
     * @param path file path
     * @param columns list of implicit file columns needed
     * @return map with columns name and their values
     * @throws IllegalArgumentException when column name is not in ImplicitFileColumns enum.
     */
    public static Map<String, String> toMap(Path path, List<String> columns) {
      Map<String, String> map = Maps.newLinkedHashMap();
      for (String column : columns) {
        map.put(column, ImplicitFileColumns.valueOf(column.toUpperCase()).getValue(path));
      }
      return map;
    }

    /**
     * Using file path calculates value for each implicit file column
     */
    public abstract String getValue(Path path);

  }

}
