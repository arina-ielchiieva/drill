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
package org.apache.drill.exec.store.schedule;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.carrotsearch.hppc.cursors.ObjectLongCursor;

/**
 * Presents an interface that describes the number of bytes for a particular work unit associated with a particular DrillbitEndpoint.
 */
public interface EndpointByteMap extends Iterable<ObjectLongCursor<DrillbitEndpoint>>{

  boolean isSet(DrillbitEndpoint endpoint);
  long get(DrillbitEndpoint endpoint);
  boolean isEmpty();
  long getMaxBytes();
  void add(DrillbitEndpoint endpoint, long bytes);
}