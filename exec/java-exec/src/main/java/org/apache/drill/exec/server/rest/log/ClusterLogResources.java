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
package org.apache.drill.exec.server.rest.log;

import com.google.common.collect.Maps;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.server.rest.DrillRestServer;
import org.apache.drill.exec.server.rest.ViewableWithPermissions;
import org.apache.drill.exec.util.RestUtil;
import org.apache.drill.exec.work.WorkManager;
import org.glassfish.jersey.server.mvc.Viewable;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriBuilder;

import java.net.URI;
import java.util.Map;
import java.util.Set;

import static org.apache.drill.exec.server.rest.auth.DrillUserPrincipal.ADMIN_ROLE;

@Path("/")
@RolesAllowed(ADMIN_ROLE)
public class ClusterLogResources {

  @Inject DrillRestServer.UserAuthEnabled authEnabled;
  @Inject WorkManager work;
  @Inject SecurityContext sc;

  @GET
  @Path("/cluster/logs")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getClusterLogs() {
    Map<String, Set<LogResources.Log>> logs = getClusterLogsJSON();
    return ViewableWithPermissions.create(authEnabled.get(), "/rest/logs/list.ftl", sc, logs);
  }

  @GET
  @Path("/cluster/logs.json")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<String, Set<LogResources.Log>> getClusterLogsJSON() {
    Map<String, Set<LogResources.Log>> result = Maps.newHashMap();
    for (CoordinationProtos.DrillbitEndpoint bit : work.getContext().getBits()) {
      String node = "http://" + bit.getAddress() + ":8047"; // + bit.getUserPort();
      System.out.println(node);
      Set<LogResources.Log> logs = RestUtil.request("/logs.json", new GenericType<Set<LogResources.Log>>() {}, null, node);
      System.out.println(logs);
      result.put(node, logs);
    }
    return result;
  }

}

