/**
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
package org.apache.drill.exec.server.rest;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import javax.annotation.security.PermitAll;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;
import javax.xml.bind.annotation.XmlRootElement;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.server.rest.DrillRestServer.UserAuthEnabled;
import org.apache.drill.exec.work.WorkManager;
import org.glassfish.jersey.server.mvc.Viewable;

import com.fasterxml.jackson.annotation.JsonCreator;

@Path("/")
@PermitAll
public class DrillRoot {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillRoot.class);

  @Inject UserAuthEnabled authEnabled;
  @Inject WorkManager work;
  @Inject SecurityContext sc;

  @GET
  @Produces(MediaType.TEXT_HTML)
  public Viewable getStats() {
    return ViewableWithPermissions.create(authEnabled.get(), "/rest/index.ftl", sc, getStatsJSON());
  }

  @GET
  @Path("/stats.json")
  @Produces(MediaType.APPLICATION_JSON)
  public Stats getStatsJSON() {
    final String version = work.getContext().getOptionManager().getOption(ExecConstants.CLUSTER_VERSION).string_val;

    final Map<String, Object> props = Maps.newLinkedHashMap();
    props.put("Cluster Version", version);
    props.put("Number of Drillbits", work.getContext().getBits().size());
    CoordinationProtos.DrillbitEndpoint currentEndpoint = work.getContext().getEndpoint();
    final String address = currentEndpoint.getAddress();
    props.put("Data Port Address", address + ":" + currentEndpoint.getDataPort());
    props.put("User Port Address", address + ":" + currentEndpoint.getUserPort());
    props.put("Control Port Address", address + ":" + currentEndpoint.getControlPort());
    props.put("Maximum Direct Memory", DrillConfig.getMaxDirectMemory());

    return new Stats(props, collectDrillbits(version));
  }

  private Collection<DrillbitInfo> collectDrillbits(String version) {
    Set<DrillbitInfo> drillbits = Sets.newTreeSet();
    for (CoordinationProtos.DrillbitEndpoint endpoint : work.getContext().getBits()) {
      boolean versionMatch = version.equals(endpoint.getVersion());
      DrillbitInfo drillbit = new DrillbitInfo(endpoint.getAddress(), endpoint.isInitialized(), endpoint.getVersion(), versionMatch);
      drillbits.add(drillbit);
    }
    return drillbits;
  }

  @XmlRootElement
  public static class Stats {
    private final Map<String, Object> props;
    private final Collection<DrillbitInfo> drillbits;

    @JsonCreator
    public Stats(Map<String, Object> props, Collection<DrillbitInfo> drillbits) {
      this.props = props;
      this.drillbits = drillbits;
    }

    public Map<String, Object> getProps() {
      return props;
    }

    public Collection<DrillbitInfo> getDrillbits() {
      return drillbits;
    }
  }

  public static class DrillbitInfo implements Comparable<DrillbitInfo> {
    private final String address;
    private final boolean initialized;
    private final String version;
    private final boolean versionMatch;

    @JsonCreator
    public DrillbitInfo(String address, boolean initialized, String version, boolean versionMatch) {
      this.address = address;
      this.initialized = initialized;
      this.version = version;
      this.versionMatch = versionMatch;
    }

    public String getAddress() {
      return address;
    }

    public String isInitialized() {
      return initialized ? "initialized" : "not initialized";
    }

    public String getVersion() {
      return version;
    }

    public boolean isVersionMatch() {
      return versionMatch;
    }

    @Override
    public int compareTo(DrillbitInfo o) {
      if (this.isVersionMatch() == o.isVersionMatch()) {
        return this.address.compareTo(o.address);
      }
      return versionMatch ? 1 : -1;
    }
  }

}
