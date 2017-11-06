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
package org.apache.drill.exec.store.hive.readers;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.hive.HivePartition;
import org.apache.drill.exec.store.hive.HiveSubScan;
import org.apache.drill.exec.store.hive.HiveTableWithColumnCache;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.security.UserGroupInformation;

import java.lang.reflect.Constructor;
import java.util.List;

public abstract class AbstractReadersInitializer {

  protected final Class<? extends HiveAbstractReader> readerClass;
  protected final HiveSubScan config;

  private final FragmentContext context;
  private final UserGroupInformation proxyUgi;

  public AbstractReadersInitializer(FragmentContext context, HiveSubScan config, Class<? extends HiveAbstractReader> readerClass) {
    this.config = config;
    this.context = context;
    this.readerClass = readerClass;
    this.proxyUgi = ImpersonationUtil.createProxyUgi(config.getUserName(), context.getQueryUserName());
  }

  protected Constructor<? extends HiveAbstractReader> createReaderConstructor(Class<?> inputClass) {
    try {
      return readerClass.getConstructor(HiveTableWithColumnCache.class, HivePartition.class,
          inputClass,
          List.class, FragmentContext.class, HiveConf.class, UserGroupInformation.class);
    } catch (ReflectiveOperationException e) {
      throw new DrillRuntimeException(String.format("Unable to retrieve constructor for Hive reader class [%s]", readerClass), e);
    }
  }

  protected HiveAbstractReader createReader(Constructor<? extends HiveAbstractReader> readerConstructor, Partition partition, Object split) {
    try {
      return readerConstructor.newInstance(config.getTable(), partition, split, config.getColumns(), context, config.getHiveConf(), proxyUgi);
    } catch (ReflectiveOperationException e) {
      throw new DrillRuntimeException(String.format("Unable to create instance for Hive reader [%s]", readerConstructor), e);
    }
  }

  protected boolean hasPartitions(List<HivePartition> partitions) {
    return partitions != null && !partitions.isEmpty();
  }

  public abstract List<RecordReader> init();
}
