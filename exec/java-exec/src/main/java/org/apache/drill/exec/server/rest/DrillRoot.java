/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.server.rest;

import java.util.Collection;
import javax.annotation.security.PermitAll;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;
import javax.xml.bind.annotation.XmlRootElement;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.server.rest.DrillRestServer.UserAuthEnabled;
import org.apache.drill.exec.work.WorkManager;
import org.glassfish.jersey.server.mvc.Viewable;

import com.fasterxml.jackson.annotation.JsonCreator;

@Path("/")
@PermitAll
public class DrillRoot {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillRoot.class);

  @Inject
  UserAuthEnabled authEnabled;
  @Inject
  WorkManager work;
  @Inject
  SecurityContext sc;

  @GET
  @Produces(MediaType.TEXT_HTML)
  public Viewable getClusterInfo() {
    return ViewableWithPermissions.create(authEnabled.get(), "/rest/index.ftl", sc, getClusterInfoJSON());
  }

  @GET
  @Path("/cluster.json")
  @Produces(MediaType.APPLICATION_JSON)
  public ClusterInfo getClusterInfoJSON() {
    Collection<DrillbitInfo> drillbits = Sets.newTreeSet();
    Collection<String> mismatchedVersions = Sets.newTreeSet();

    DrillbitEndpoint currentDrillbit = work.getContext().getEndpoint();
    String currentVersion = currentDrillbit.getVersion();

    // add fake drillbits
    Collection<DrillbitEndpoint> bits = work.getContext().getBits();
    bits = Lists.newArrayList(bits);
    bits.add(DrillbitEndpoint.newBuilder().setAddress("drill_1.lab").setUserPort(31010).setDataPort(31012).setControlPort(31011).setVersion("1.9.0-SNAPSHOT").build());
    bits.add(DrillbitEndpoint.newBuilder().setAddress("drill_2.lab").setUserPort(31010).setDataPort(31012).setControlPort(31011).setVersion("1.9.0").build());
    bits.add(DrillbitEndpoint.newBuilder().setAddress("drill_3.lab").setUserPort(31010).setDataPort(31012).setControlPort(31011).setVersion("1.10.0").build());
    bits.add(DrillbitEndpoint.newBuilder().setAddress("drill_4.lab").setUserPort(31010).setDataPort(31012).setControlPort(31011).setVersion("1.10.0").build());
    bits.add(DrillbitEndpoint.newBuilder().setAddress("drill_5.lab").setUserPort(31010).setDataPort(31012).setControlPort(31011).setVersion("1.9.0-SNAPSHOT").build());
    bits.add(DrillbitEndpoint.newBuilder().setAddress("drill_6.lab").setUserPort(31010).setDataPort(31012).setControlPort(31011).setVersion("").build());
    bits.add(DrillbitEndpoint.newBuilder().setAddress("drill_7.lab").setUserPort(31010).setDataPort(31012).setControlPort(31011).setVersion("1.8.0").build());
    bits.add(DrillbitEndpoint.newBuilder().setAddress("drill_8.lab").setUserPort(31010).setDataPort(31012).setControlPort(31011).build());
    bits.add(DrillbitEndpoint.newBuilder().setAddress("drill_9.lab").setUserPort(31010).setDataPort(31012).setControlPort(31011).setVersion("1.9.0-SNAPSHOT").build());
    // add fake drillbits

    for (DrillbitEndpoint endpoint : bits) {
      DrillbitInfo drillbit = new DrillbitInfo(endpoint,
              currentDrillbit.equals(endpoint),
              currentVersion.equals(endpoint.getVersion()));
      if (!drillbit.isVersionMatch()) {
        mismatchedVersions.add(drillbit.getVersion());
      }
      drillbits.add(drillbit);
    }


    return new ClusterInfo(drillbits, currentVersion, mismatchedVersions);
  }

  @XmlRootElement
  public static class ClusterInfo {
    private final Collection<DrillbitInfo> drillbits;
    private final String currentVersion;
    private final Collection<String> mismatchedVersions;

    @JsonCreator
    public ClusterInfo(Collection<DrillbitInfo> drillbits,
                       String currentVersion,
                       Collection<String> mismatchedVersions) {
      this.drillbits = drillbits;
      this.currentVersion = currentVersion;
      this.mismatchedVersions = mismatchedVersions;
    }

    public Collection<DrillbitInfo> getDrillbits() {
      return drillbits;
    }

    public String getCurrentVersion() {
      return currentVersion;
    }

    public Collection<String> getMismatchedVersions() {
      return mismatchedVersions;
    }
  }

  public static class DrillbitInfo implements Comparable<DrillbitInfo> {
    private final String address;
    private final String userPort;
    private final String controlPort;
    private final String dataPort;
    private final String version;
    private final boolean current;
    private final boolean versionMatch;

    @JsonCreator
    public DrillbitInfo(DrillbitEndpoint drillbit, boolean current, boolean versionMatch) {
      this.address = drillbit.getAddress();
      this.userPort = String.valueOf(drillbit.getUserPort());
      this.controlPort = String.valueOf(drillbit.getControlPort());
      this.dataPort = String.valueOf(drillbit.getDataPort());
      this.version = Strings.isNullOrEmpty(drillbit.getVersion()) ? "Undefined" : drillbit.getVersion();
      this.current = current;
      this.versionMatch = versionMatch;
    }

    public String getAddress() {
      return address;
    }

    public String getUserPort() { return userPort; }

    public String getControlPort() { return controlPort; }

    public String getDataPort() { return dataPort; }

    public String getVersion() {
      return version;
    }

    public boolean isCurrent() {
      return current;
    }

    public boolean isVersionMatch() {
      return versionMatch;
    }

    @Override
    public int compareTo(DrillbitInfo o) {
      // current version should go first
      if (isCurrent()) {
        return -1;
      }
      if (this.isVersionMatch() == o.isVersionMatch()) {
        if (this.version.equals(o.version)) {
          return this.address.compareTo(o.address);
        } else {
          return this.version.compareTo(o.version);
        }
      }
      return versionMatch ? -1 : 1;
    }
  }

}
