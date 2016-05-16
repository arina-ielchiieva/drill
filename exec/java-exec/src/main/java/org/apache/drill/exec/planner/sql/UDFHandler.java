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
package org.apache.drill.exec.planner.sql;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.io.FileUtils;
import org.apache.drill.common.config.CommonConstants;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.scanner.RunTimeScan;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.proto.UserProtos.StringList;

import javax.annotation.Nullable;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.List;

public class UDFHandler {

  private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UDFHandler.class);

  private final FunctionImplementationRegistry functionRegistry;

  public UDFHandler(FunctionImplementationRegistry functionRegistry) {
    this.functionRegistry = functionRegistry;
  }

  public StringList createUDF(StringList list) {
    // prepare binary and source jar names
    String binaryJarName = list.getList(0);
    String sourceJarname = getSourceJarName(binaryJarName);

    // prepare classpath directory
    String pathToClasspath = getUDFClasspath();

    // check if binary and source jars don't exist in classpath
    File binary = new File(pathToClasspath, binaryJarName);
    File source = new File(pathToClasspath, sourceJarname);
    if (binary.exists() || source.exists()) {
      throw new RuntimeException("Binary and source jars exists in classpath. Please cleanup first."); //todo
    }

    // check if binary and source jars exist in temp directory
    File tempBinary = new File(getSystemTempFolder(), binaryJarName);
    File tempSource = new File(getSystemTempFolder(), sourceJarname);

    if (!tempBinary.exists() || !tempSource.exists()) {
      throw new RuntimeException("Binary and source jars are absent in temp directory."); //todo
    }

    try {
      // copy files from system temp directory to classpath to
      Files.copy(tempBinary, binary);
      Files.copy(tempSource, source);

      // add to classpath binary and source jars
      URL binaryUrl = binary.toURI().toURL();
      addToClasspath(binaryUrl, source.toURI().toURL());

      // look for marker file in binary jar in classpath
      URL markerFile = getMarkerFile(binaryUrl);

      // make sure maker file was found
      Preconditions.checkNotNull(markerFile, String.format("%s should contain %s", binary.getName(), CommonConstants.DRILL_JAR_MARKER_FILE_RESOURCE_PATHNAME));

      return doWork(markerFile, binaryUrl, new Function<ScanResult, Collection<String>>() {
        @Nullable
        @Override
        public Collection<String> apply(ScanResult scanResult) {
          return functionRegistry.registerFunctions(scanResult);
        }
      });

    } catch (Exception e) {
      deleteFile(binary, source);
      throw new RuntimeException(e); //todo
    } finally {
      deleteFile(tempBinary, tempSource);
    }
  }

  public StringList deleteUDF(StringList list) {
    // get binary and source jar name
    String binaryJarName = list.getList(0);
    String sourceJarName = getSourceJarName(binaryJarName);

    // prepare classpath directory
    String pathToClasspath = getUDFClasspath();

    File binary = new File(pathToClasspath, binaryJarName);
    File source = new File(pathToClasspath, sourceJarName);

    try {
      if (binary.exists()) {
        // look for marker file in binary jar in classpath
        URL binaryUrl = binary.toURI().toURL();
        URL markerFile = getMarkerFile(binaryUrl);

        if (markerFile != null) {
          return doWork(markerFile, binaryUrl, new Function<ScanResult, Collection<String>>() {
            @Nullable
            @Override
            public Collection<String> apply(@Nullable ScanResult scanResult) {
              return functionRegistry.deleteFunctions(scanResult);
            }
          });
        }
      }
    } catch (Exception e) {
      logger.error("", e);      // todo do nothing
    } finally {
      deleteFile(binary, source);
    }
    return StringList.getDefaultInstance();
  }

  private String getSourceJarName(String binaryJarName) {
    return binaryJarName.replace(".jar", "-sources.jar");
  }

  private String getSystemTempFolder() {
    return System.getProperty("java.io.tmpdir");
  }


  private void addToClasspath(URL url, URL... urls) {
    List<URL> urlList = Lists.newArrayList(urls);
    urlList.add(url);
    try {
      Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
      method.setAccessible(true);
      for (URL u : urlList) {
        method.invoke(ClassLoader.getSystemClassLoader(), u);
      }
    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
      throw UserException.validationError() //todo
          .message("Unable to upload %s to classpath", urlList)
          .build(logger);
    }
  }

  private String getUDFClasspath() {
    String pathToClasspath = System.getenv("DRILL_CP_UDF");
    Preconditions.checkNotNull(pathToClasspath, "DRILL_CP_UDF variable should be defined");
    return pathToClasspath;
  }

  private URL getMarkerFile(URL url) {
    URL markerFile = null;
    for (URL resource : ClassPathScanner.getConfigURLs()) {
      if (resource.getPath().contains(url.getPath())) {
        markerFile = resource;
        break;
      }
    }
    return markerFile;
  }

  private StringList doWork(URL markerFile, URL jarUrl, Function<ScanResult, Collection<String>> function) {
    //get config based on marker file info
    Config config = ConfigFactory.parseURL(markerFile);

    // wrap config into DrillConfig and receive scan result for our jar
    DrillConfig drillConfig = new DrillConfig(config.resolve(), true);
    ScanResult scanResult = RunTimeScan.dynamicPackageScan(drillConfig, Sets.newHashSet(jarUrl));

    function.apply(scanResult);
    return StringList.newBuilder().addAllList(function.apply(scanResult)).build();
  }

  private void deleteFile(File file, File... files) {
    List<File> fileList = Lists.newArrayList(files);
    fileList.add(file);
    for (File f : fileList) {
      if (!f.delete()) {
        logger.warn("File {} does not exist", f.getName());
      }
    }
  }
}
