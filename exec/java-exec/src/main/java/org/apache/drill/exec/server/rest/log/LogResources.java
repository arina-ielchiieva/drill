/**
 * ****************************************************************************
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
 * ****************************************************************************
 */
package org.apache.drill.exec.server.rest.log;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.server.rest.DrillRestServer;
import org.apache.drill.exec.server.rest.ViewableWithPermissions;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.work.WorkManager;
import org.glassfish.jersey.server.mvc.Viewable;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.xml.bind.annotation.XmlRootElement;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.drill.exec.server.rest.auth.DrillUserPrincipal.ADMIN_ROLE;

@Path("/")
@RolesAllowed(ADMIN_ROLE)
public class LogResources {

  @Inject DrillRestServer.UserAuthEnabled authEnabled;
  @Inject SecurityContext sc;

  @GET
  @Path("/logs")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getLogs() {
    Set<Log> logs = getLogsJSON();
    return ViewableWithPermissions.create(authEnabled.get(), "/rest/logs/list.ftl", sc, logs);
  }

  @GET
  @Path("/logs.json")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<Log> getLogsJSON() {
    Set<Log> logs = Sets.newTreeSet(new Comparator<Log>() {
      @Override
      public int compare(Log o1, Log o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });
    File[] files = getLogFolder().listFiles(new FileFilter() {
      public boolean accept(File file) {
        return file.isFile();
      }
    });

    for (File file : files) {
      logs.add(new Log(file.getName(), file.length(), file.lastModified()));
    }

    return logs;
  }

  @GET
  @Path("/log/{name}")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getLog(@PathParam("name") String name) throws IOException {
    LogContent content = getLogJSON(name);
    return ViewableWithPermissions.create(authEnabled.get(), "/rest/logs/log.ftl", sc, content);
  }

  @GET
  @Path("/log/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  public LogContent getLogJSON(@PathParam("name") final String name) throws IOException {
    File file = getFileByName(getLogFolder(), name);

    final int maxSize = 10; //todo should be from config

    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      Map<String, String> cache = new LinkedHashMap<String, String>(maxSize, .75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
          return size() > maxSize;
        }
      };

      String line;
      while ((line = br.readLine()) != null) {
        cache.put(line, null);
      }

      return new LogContent(file.getName(), cache.keySet(), maxSize);
    }
  }

  @GET
  @Path("/log/download/{name}")
  @Produces(MediaType.TEXT_PLAIN)
  public Response getFullLog(@PathParam("name") final String name) {
    File file = getFileByName(getLogFolder(), name);
    Response.ResponseBuilder response = Response.ok(file);
    response.header("Content-Disposition", String.format("attachment;filename\"%s\"", name));
    return response.build();
  }

  private File getLogFolder() {
    return new File(System.getenv("DRILL_LOG_DIR"));
  }

  private File getFileByName(File folder, final String name) {
    File[] files = folder.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String fileName) {
        return fileName.equals(name);
      }
    });
    if (files.length == 0) {
      throw new DrillRuntimeException (name + " doesn't exist");
    }
    return files[0];
  }


  @XmlRootElement
  public static class Log {
    public final SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

    private String name;
    private String size;
    private Date lastModified;

    public Log() {

    }

    @JsonCreator
    public Log (String name, long size, long lastModified) {
      this.name = name;
      this.size = (size / 1024) + " KB";
      this.lastModified = new Date(lastModified);
    }

    public String getName() {
      return name;
    }

    public String getSize() {
      return size;
    }

    public String getLastModified() {
      return format.format(lastModified);
    }

    public void setName(String name) {
      this.name = name;
    }

    public void setLastModified(Date lastModified) {
      this.lastModified = lastModified;
    }

    public void setSize(String size) {
      this.size = size;
    }
  }

  @XmlRootElement
  public static class LogContent {
    private String name;
    private Collection<String> lines;
    private int maxSize;

    @JsonCreator
    public LogContent (String name, Collection<String> lines, int maxSize) {
      this.name = name;
      this.lines = lines;
      this.maxSize = maxSize;
    }

    public String getName() {
      return name;
    }

    public Collection<String> getLines() { return lines; }

    public int getMaxSize() { return maxSize; }
  }
}