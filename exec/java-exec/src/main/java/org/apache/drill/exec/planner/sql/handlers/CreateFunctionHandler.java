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
package org.apache.drill.exec.planner.sql.handlers;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.io.FileUtils;
import org.apache.drill.common.config.CommonConstants;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.scanner.RunTimeScan;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.parser.SqlCreateFunction;
import org.apache.drill.exec.work.foreman.ForemanSetupException;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

public class CreateFunctionHandler extends DefaultSqlHandler {

  private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CreateFunctionHandler.class);

  public CreateFunctionHandler(SqlHandlerConfig config) { super(config); }

  /**
   * Creates udf-s dynamically.
   * @return - Single row indicating list of created udf-s, raise exception otherwise
   */
  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ForemanSetupException, IOException {
    SqlCreateFunction node = unwrap(sqlNode, SqlCreateFunction.class);

    // get path to jar and sources
    String pathToJar = ((SqlCharStringLiteral) node.getPathToJar()).toValue();

    // if user didn't provide path to sources
    // we assume that sources lie in the same directory as jar and have standard naming convention
    String pathToSources = pathToJar.replace(".jar", "-sources.jar");
    if (node.getPathToSources() != null) {
      pathToSources = ((SqlCharStringLiteral) node.getPathToSources()).toValue();
    }

    // create file using path to jar and sources
    File initialJar = new File(pathToJar);
    File initialSources = new File(pathToSources);

    // get path where to copy 3rd party libraries
    String pathToClasspath = System.getenv("DRILL_CP_3RD_PARTY");
    Preconditions.checkNotNull(pathToClasspath, "DRILL_CP_3RD_PARTY variable should be defined");
    File classpath = new File(pathToClasspath);

    // create path to jar and sources in classpath
    File jar = new File(classpath.getPath(), initialJar.getName());
    File sources = new File(classpath.getPath(), initialSources.getName());

    // copy to classpath jar and sources
    FileUtils.copyFile(initialJar, jar);
    FileUtils.copyFile(initialSources, sources);

    // add to classpath jar and sources
    addToClasspath(jar.toURI().toURL(), sources.toURI().toURL());

    // look for marker file from our jar in classpath
    URL markerFile = null;
    for (URL resource : ClassPathScanner.getConfigURLs()) {
      if (resource.getPath().contains(jar.toURI().toURL().getPath())) {
        markerFile = resource;
        break;
      }
    }

    // make sure maker file was found
    Preconditions.checkNotNull(markerFile,
        String.format("%s should contain %s", jar.getName(), CommonConstants.DRILL_JAR_MARKER_FILE_RESOURCE_PATHNAME));

    // create config based on marker file information
    Config config = ConfigFactory.parseURL(markerFile);

    // wrap config into DrillConfig and receive scan result for our jar
    DrillConfig drillConfig = new DrillConfig(config.resolve(), true);
    ScanResult scanResult = RunTimeScan.dynamicPackageScan(drillConfig, Sets.newHashSet(jar.toURI().toURL()));

    // register function (-s) in function registry
    context.getFunctionRegistry().dynamicallyRegister(scanResult);

    return DirectPlan.createDirectPlan(context, true, "Dynamic function has been registered"); //todo
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
      throw UserException.validationError() //todo probably not user exception
          .message("Unable to upload %s to classpath", urlList)
          .build(logger);
    }
  }

}
