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
package org.apache.drill.exec.server.rest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.apache.drill.exec.store.StoragePluginRegistry;
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
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static org.apache.drill.exec.server.rest.auth.DrillUserPrincipal.ADMIN_ROLE;

@Path("/")
@RolesAllowed(ADMIN_ROLE)
public class LogsResources {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StorageResources.class);

  @Inject DrillRestServer.UserAuthEnabled authEnabled;
  @Inject StoragePluginRegistry storage;
  @Inject ObjectMapper mapper;
  @Inject SecurityContext sc;

/*  @GET
  @Path("/storage.json")
  @Produces(MediaType.APPLICATION_JSON)
  public List<PluginConfigWrapper> getStoragePluginsJSON() {

    List<PluginConfigWrapper> list = Lists.newArrayList();
    for (Map.Entry<String, StoragePluginConfig> entry : Lists.newArrayList(storage.getStore().getAll())) {
      PluginConfigWrapper plugin = new PluginConfigWrapper(entry.getKey(), entry.getValue());
      list.add(plugin);
    }
    return list;
  }*/

  @GET
  @Path("/logs")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getLogs() {
    List<Log> list = getLogsJSON();
    return ViewableWithPermissions.create(authEnabled.get(), "/rest/logs/list.ftl", sc, list);
  }

  @GET
  @Path("/logs.json")
  @Produces(MediaType.APPLICATION_JSON)
  public List<Log> getLogsJSON() {
    List<Log> logs = Lists.newLinkedList();

    File folder = new File(System.getenv("DRILL_LOG_DIR"));
    File[] files = folder.listFiles(new FileFilter() {
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
  @Path("/log/{name}.json")
  @Produces(MediaType.TEXT_HTML)
  public LogContent getLogJSON(@PathParam("name") final String name) throws IOException {
    File folder = new File(System.getenv("DRILL_LOG_DIR"));
    File[] files = folder.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String fileName) {
        return fileName.equals(name);
      }
    });

    if (files.length == 0) {
      throw new RuntimeException(name + " doesn't exist"); //todo which exception to throw
    }

    File file = files[0];
    //String text = Files.toString(file, Charsets.UTF_8); //todo utf-8 ???

    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      Map<String, String> cache = new LinkedHashMap<String, String>(10, .75f, true) { //todo should be from config
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
          return size() > 10; //todo should be from config
        }
      };

      String line;
      while ((line = br.readLine()) != null) {
        cache.put(line, null);
      }

      return new LogContent(file.getName(), cache.keySet());
    }
  }

  @GET
  @Path("/log/download/{name}")
  @Produces(MediaType.TEXT_PLAIN)
  public Response getFullLog(@PathParam("name") final String name) {
    File folder = new File(System.getenv("DRILL_LOG_DIR"));
    File[] files = folder.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String fileName) {
        return fileName.equals(name);
      }
    });

    if (files.length == 0) {
      throw new RuntimeException(name + " doesn't exist"); //todo which exception to throw
    }

    File file = files[0];
    Response.ResponseBuilder response = Response.ok(file);
    response.header("Content-Disposition", String.format("attachment;filename\"%s\"", name));
    return response.build();
  }


  @XmlRootElement
  public class Log {
    public final SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

    private String name;
    private long size;
    private Date lastModified;

    @JsonCreator
    public Log (String name, long size, long lastModified) {
      this.name = name;
      this.size = size;
      this.lastModified = new Date(lastModified);
    }

    public String getName() {
      return name;
    }

    public String getSize() {
      return (size / 1024) + " KB";
    }

    public String getLastModified() {
      return format.format(lastModified);
    }
  }

  @XmlRootElement
  public class LogContent {
    private String name;
    private Collection<String> lines;

    @JsonCreator
    public LogContent (String name, Collection<String> lines) {
      this.name = name;
      this.lines = lines;
    }

    public String getName() {
      return name;
    }

    public Collection<String> getLines() { return lines;}
  }
}
