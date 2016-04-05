/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.util;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import java.net.URI;
import java.util.Map;

public class RestUtil {

  private static final CacheControl cacheControl = new CacheControl() {
    {
      setNoCache(true);
    }
  };

  public static <T> T request(String path, GenericType<T> genericType, Map<String, Object> parameters, String node) {
    WebTarget webTarget = startUrl(node, path);

    if (parameters != null) {
      for (Map.Entry<String, Object> entry: parameters.entrySet()) {
        webTarget = webTarget.queryParam(entry.getKey(), entry.getValue());
      }
    }

    return webTarget.request(MediaType.APPLICATION_JSON_TYPE)
        .cacheControl(cacheControl)
        .get(genericType);
  }

  private static WebTarget startUrl(String node, String path) {
    return ClientBuilder.newClient().target(node).path(path);
  }

}
