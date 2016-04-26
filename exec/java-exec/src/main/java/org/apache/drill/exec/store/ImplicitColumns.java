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

import com.google.common.collect.Maps;
import com.google.common.io.Files;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Map;

public enum ImplicitColumns {

    FQN {
      @Override
      public String getValue(Path path) {
        return path.toString();
      }
    },
    DIRNAME {
      @Override
      public String getValue(Path path) {
        return path.getParent().toString();
      }
    },
    FILENAME {
      @Override
      public String getValue(Path path) {
        return path.getName();
      }
    },
    SUFFIX {
      @Override
      public String getValue(Path path) {
        return Files.getFileExtension(path.getName());
      }
    };

    public static Map<String, String> toMap(Path path, List<String> columns) {
      Map<String, String> map = Maps.newLinkedHashMap();
      for (String column : columns) {
        map.put(column, ImplicitColumns.valueOf(column.toUpperCase()).getValue(path));
      }
      return map;
    }

    public abstract String getValue(Path path);

}
