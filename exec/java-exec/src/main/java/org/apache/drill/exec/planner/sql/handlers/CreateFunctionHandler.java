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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.DrillBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.io.FileUtils;
import org.apache.drill.common.config.CommonConstants;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.scanner.RunTimeScan;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.parser.SqlCreateFunction;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.rpc.UserRpcException;
import org.apache.drill.exec.rpc.control.ControlTunnel;
import org.apache.drill.exec.rpc.control.Controller;
import org.apache.drill.exec.work.foreman.ForemanSetupException;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;

public class CreateFunctionHandler extends DefaultSqlHandler {

  private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CreateFunctionHandler.class);

  public CreateFunctionHandler(SqlHandlerConfig config) {
    super(config);
    //todo messageType is set 1, 2?
    //todo can be connected with enum RpcType?
    //context.getController().registerCustomHandler(1, new JarTransferHandler(context), SIMPLE_SERDE, SIMPLE_SERDE);
    //context.getController().registerCustomHandler(2, new AddFunctionHandler(context), SIMPLE_SERDE, SIMPLE_SERDE);
    //context.getController().registerCustomHandler(3, new DeleteFunctionHandler(context), SIMPLE_SERDE, SIMPLE_SERDE);
  }


  public static final Controller.CustomSerDe<String> SIMPLE_SERDE = new Controller.CustomSerDe<String>() {
    @Override
    public byte[] serializeToSend(String send) {
      return send.getBytes();
    }

    @Override
    public String deserializeReceived(byte[] bytes) throws Exception {
      return new String(bytes);
    }
  };

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

    /***************************************************/
    // create list of drillbits where we need to perform clean up in case of failure during function registering
    List<CoordinationProtos.DrillbitEndpoint> endpointsToCleanup = Lists.newArrayList();
    boolean needCleanup = false;

    //todo currently we don't have this logic for testing simplification
    // first register function on current endpoint if everything goes well start registering on other endpoints
    //CoordinationProtos.DrillbitEndpoint currentEndpoint = context.getCurrentEndpoint();
    //todo register on current drillbit


    // once done add to endpoints which will need to be cleaned up in case of failure
    //endpointsToCleanup.add(currentEndpoint);

    // copy files to all drillbits except of current
    Collection<CoordinationProtos.DrillbitEndpoint> activeEndpoints = context.getActiveEndpoints();
    for (CoordinationProtos.DrillbitEndpoint endpoint : activeEndpoints) {
      //todo currently we don't have this logic for testing simplification
     /* // we don't need to register on current endpoint as this already has been done as first step
      if (endpoint.equals(currentEndpoint)) {
        continue;
      }*/
      try {
        //todo think about RpcListener
        final ControlTunnel controlTunnel = context.getController().getTunnel(endpoint);
        final ControlTunnel.CustomTunnel<String, String> transferTunnel = controlTunnel.getCustomTunnel(1, SIMPLE_SERDE, SIMPLE_SERDE);
        transferTunnel.send(initialJar.getName(), getBytebufForFile(pathToJar)).get();
        transferTunnel.send(initialSources.getName(), getBytebufForFile(pathToSources)).get();
        final ControlTunnel.CustomTunnel<String, String> addFunctionTunnel = controlTunnel.getCustomTunnel(2, SIMPLE_SERDE, SIMPLE_SERDE);
        addFunctionTunnel.send(initialJar.getName()).get();
      } catch (Exception e) {
        logger.error("Error registering function", e);
        needCleanup = true;
      } finally {
        endpointsToCleanup.add(endpoint);
      }
    }

    try {
      if (needCleanup) {
        for (CoordinationProtos.DrillbitEndpoint endpoint : endpointsToCleanup) {
          final ControlTunnel controlTunnel = context.getController().getTunnel(endpoint);
          final ControlTunnel.CustomTunnel<String, String> cleanUpTunnel = controlTunnel.getCustomTunnel(3, SIMPLE_SERDE, SIMPLE_SERDE);
          cleanUpTunnel.send(initialJar.getName()).get();
        }
      }
    } catch (Exception e) {
      logger.warn("There were problems during cleanup", e);
    }
    // if needCleanup is true than we didn't register function, some problems have occurred

    return DirectPlan.createDirectPlan(context, !needCleanup,
        needCleanup ? "There were problem during adding function. Look in logs for more information" :
        "Dynamic function has been registered"); //todo return list of registered function
  }

  private static void addToClasspath(URL url, URL... urls) {
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

  private ByteBuf getBytebufForFile(String pathToFile) throws IOException {
    Path file = Paths.get(pathToFile);
    byte[] bytes = Files.readAllBytes(file);
    ByteBuf byteBuf = UnpooledByteBufAllocator.DEFAULT.buffer(bytes.length);
    byteBuf.writeBytes(bytes);
    byteBuf.retain();
    return byteBuf;
  }

  public static class JarTransferHandler implements Controller.CustomMessageHandler<String, String> {

    @Override
    public Controller.CustomResponse<String> onMessage(String pBody, DrillBuf dBody) throws UserRpcException {
      System.out.println("File name to create - " + pBody);
      // convert incoming byte buffer to byte array
      byte[] bytes = new byte[dBody.capacity()];
      dBody.getBytes(0, bytes);
      // create file with incoming content
      //todo get temp dir - any other approaches?
      String tempFolder = System.getProperty("java.io.tmpdir");
      Path destination = Paths.get(tempFolder, pBody);
      try {
        Files.write(destination, bytes);
      } catch (IOException e) {
        throw new RuntimeException(e); //todo add proper exception
      }

      return new Controller.CustomResponse<String>() {
        @Override
        public String getMessage() {
          return "OK";
        }

        @Override
        public ByteBuf[] getBodies() {
          return null;
        }
      };
    }
  }

  public static class AddFunctionHandler implements Controller.CustomMessageHandler<String, String> {

    private final FunctionImplementationRegistry registry;

    public AddFunctionHandler(FunctionImplementationRegistry registry) {
      this.registry = registry;
    }

    @Override
    public Controller.CustomResponse<String> onMessage(String pBody, DrillBuf dBody) throws UserRpcException {
      // create file for jar and sources
      //todo we need temp folder location
      String tempFolder = System.getProperty("java.io.tmpdir");
      File initialJar = new File(tempFolder, pBody);
      File initialSources = new File(tempFolder, pBody.replace(".jar", "-sources.jar"));

      // get path where to copy 3rd party libraries
      String pathToClasspath = System.getenv("DRILL_CP_3RD_PARTY");
      Preconditions.checkNotNull(pathToClasspath, "DRILL_CP_3RD_PARTY variable should be defined");
      File classpath = new File(pathToClasspath);

      // create path to jar and sources in classpath
      File jar = new File(classpath.getPath(), initialJar.getName());
      File sources = new File(classpath.getPath(), initialSources.getName());

      try {
        // copy to classpath jar and sources
        FileUtils.copyFile(initialJar, jar);
        FileUtils.copyFile(initialSources, sources);

        // add to classpath jar and sources
        addToClasspath(jar.toURI().toURL(), sources.toURI().toURL());

        // remove jar and sources from temp directory
        initialJar.delete();
        initialSources.delete();

        // look for marker file from our jar in classpath
        URL markerFile = null;
        for (URL resource : ClassPathScanner.getConfigURLs()) {
          if (resource.getPath().contains(jar.toURI().toURL().getPath())) {
            markerFile = resource;
            break;
          }
        }

        // make sure maker file was found
        Preconditions.checkNotNull(markerFile, String.format("%s should contain %s", jar.getName(), CommonConstants.DRILL_JAR_MARKER_FILE_RESOURCE_PATHNAME));

        // create config based on marker file information
        Config config = ConfigFactory.parseURL(markerFile);

        // wrap config into DrillConfig and receive scan result for our jar
        DrillConfig drillConfig = new DrillConfig(config.resolve(), true);
        ScanResult scanResult = RunTimeScan.dynamicPackageScan(drillConfig, Sets.newHashSet(jar.toURI().toURL()));

        // register function (-s) in function registry
        registry.dynamicallyRegister(scanResult);
      } catch (Exception e) {
        throw new RuntimeException(e); //todo add proper exception
      }

      return new Controller.CustomResponse<String>() {
        @Override
        public String getMessage() {
          return "OK";
        }

        @Override
        public ByteBuf[] getBodies() {
          return null;
        }
      };
    }
  }

  //todo rename as the same name has sql handler
  public static class DeleteFunctionHandler implements Controller.CustomMessageHandler<String, String> {


    private final FunctionImplementationRegistry registry;

    public DeleteFunctionHandler(FunctionImplementationRegistry registry) {
      this.registry = registry;
    }

    @Override
    public Controller.CustomResponse<String> onMessage(String pBody, DrillBuf dBody) throws UserRpcException {
      System.out.println("Deleting function by jar " + pBody);
      // 1. check if nothing is left in temp dir
      //todo we need temp folder location
      String tempFolder = System.getProperty("java.io.tmpdir");

      // pBody is Jar name
      File tmpJar = new File(tempFolder, pBody);
      File tmpSources = new File(tempFolder, pBody.replace(".jar", "-sources.jar"));
      tmpJar.delete();
      tmpSources.delete();

      // 2. unregister all functions from function registry

      // get path where to copy 3rd party libraries
      String pathToClasspath = System.getenv("DRILL_CP_3RD_PARTY");
      Preconditions.checkNotNull(pathToClasspath, "DRILL_CP_3RD_PARTY variable should be defined");
      File classpath = new File(pathToClasspath);

      // check if jar exists in classpath
      File jar =  new File(pathToClasspath, pBody);
      if (jar.exists()) {

        // look for marker file from our jar in classpath and clean up the classpath
        try {

          URL markerFile = null;
          for (URL resource : ClassPathScanner.getConfigURLs()) {
            if (resource.getPath().contains(jar.toURI().toURL().getPath())) {
              markerFile = resource;
              break;
            }
          }

          if (markerFile != null) {
            // create config based on marker file information
            Config config = ConfigFactory.parseURL(markerFile);

            // wrap config into DrillConfig and receive scan result for our jar
            DrillConfig drillConfig = new DrillConfig(config.resolve(), true);
            ScanResult scanResult = RunTimeScan.dynamicPackageScan(drillConfig, Sets.newHashSet(jar.toURI().toURL()));

            // unregister function (-s) in function registry
            registry.dynamicallyDelete(scanResult);
          }
        } catch (Exception e) {
          //todo
        }


        File sources = new File(pathToClasspath, pBody.replace(".jar", "-sources.jar"));
        jar.delete();
        sources.delete();
      }
      return new Controller.CustomResponse<String>() {
        @Override
        public String getMessage() {
          return "OK";
        }

        @Override
        public ByteBuf[] getBodies() {
          return null;
        }
      };
    }
  }

}
