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
package org.apache.drill;

import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import mockit.Mocked;
import mockit.NonStrictExpectations;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.common.scanner.RunTimeScan;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.expr.fn.DrillFunctionRegistry;
import org.apache.drill.exec.proto.GeneralRPCProtos;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.server.DrillbitContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

public class TestDynamicUdfUpload extends BaseTestQuery {

  @Mocked({"getenv"})
  private System system;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void init()
  {
    new NonStrictExpectations(system)
    {
      {
        invoke(System.class, "getenv", "DRILL_CP_UDF");
        returns(folder.getRoot().getPath());
      }
    };
  }

  @Test
  public void testDynamicUpload() throws Exception {
    // add sources
    String pathToSources = "/home/osboxes/git_repo/drillUDF/target/DrillUDF-1.0-sources.jar";
    File sources = new File(pathToSources);
    addSoftwareLibrary(sources);

    // add jar
    String pathToJar = "/home/osboxes/git_repo/drillUDF/target/DrillUDF-1.0.jar";
    File file = new File(pathToJar);
    addSoftwareLibrary(file);

    URL url = file.toURI().toURL();
    URL[] urls = new URL[]{url};
    ClassLoader cl = new URLClassLoader(urls);
    // URL res = cl.getResource("drill-module.conf");

    URL res = null;
    Enumeration<URL> e = cl.getResources("drill-module.conf");
    while (e.hasMoreElements()) {
      URL r = e.nextElement();
      if (r.getPath().contains(pathToJar)) {
        res = r;
        System.out.println(r);
        break;
      }
    }

    if (res != null) {
      //int index = res.toExternalForm().lastIndexOf("drill-module.conf");
      //URL u1 = new URL(res.toExternalForm().substring(0, index));
      //System.out.println(res); // jar:file:/home/osboxes/projects/DrillUDF/target/DrillUDF-1.0.jar!/drill-module.conf
      //System.out.println(u1); // jar:file:/home/osboxes/projects/DrillUDF/target/DrillUDF-1.0.jar!/

      Config fallback = ConfigFactory.parseURL(res);
      fallback.resolve();
      DrillConfig config = new DrillConfig(fallback, true); //constructor visible for testing
      // ScanResult scanResult = ClassPathScanner.fromPrescan(config);
      // FunctionImplementationRegistry registry = new FunctionImplementationRegistry(config);

      ScanResult scanResult = RunTimeScan.dynamicPackageScan(config, Sets.newHashSet(urls));
      DrillFunctionRegistry drillFunctionRegistry = new DrillFunctionRegistry(scanResult);

      getDrillbitContext().getFunctionImplementationRegistry().registerFunctions(scanResult);

      test("select arina_upper(n_name) from cp.`tpch/nation.parquet`");

      testBuilder()
          .sqlQuery("select arina_upper(full_name) as upper_name from cp.`employee.json` where full_name = 'Sheri Nowmer'")
          .unOrdered()
          .baselineColumns("upper_name")
          .baselineValues("SHERI NOWMER")
          .go();

      testBuilder()
          .sqlQuery("select arina_upper(full_name) as upper_name from cp.`employee.json` order by 1")
          .ordered()
          .sqlBaselineQuery("select upper(full_name) as upper_name from cp.`employee.json` order by 1")
          .go();

    }
  }

  @Test
  public void testCreateFunctionSyntaxWithoutSources() throws Exception {
    String jar = "/home/osboxes/git_repo/drillUDF/target/DrillUDF-2.0.jar";
    test(String.format("create function using jar '%s'", jar));
  }

  @Test
  public void testCreateFunctionSyntaxWithSources() throws Exception {
    String jar = "/home/osboxes/git_repo/drillUDF/target/DrillUDF-1.0.jar";
    String sources = "/home/osboxes/git_repo/drillUDF/target/DrillUDF-1.0-sources.jar";
    test(String.format("create function using jar '%s' sources '%s'", jar, sources));
  }

  @Test
  public void testDynamicallyUploadFunction() throws Exception {
    String jar = "/home/osboxes/projects/DrillUDF/target/DrillUDF-2.0.jar";
    //test(String.format("create function using jar '%s'", jar));

    testBuilder()
        .sqlQuery(String.format("create function using jar '%s'", jar))
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, "The following UDFs have been created [arina_upper, arina_lower]")
        .go();

    testBuilder()
        .sqlQuery("select arina_upper(full_name) as upper_name from cp.`employee.json` where full_name = 'Sheri Nowmer'")
        .unOrdered()
        .baselineColumns("upper_name")
        .baselineValues("SHERI NOWMER")
        .go();

    testBuilder()
        .sqlQuery("select arina_upper(full_name) as upper_name from cp.`employee.json` order by 1")
        .ordered()
        .sqlBaselineQuery("select upper(full_name) as upper_name from cp.`employee.json` order by 1")
        .go();
  }

  @Test
  public void testFunctionDelete() throws Exception {
    String jar = "/home/osboxes/projects/DrillUDF/target/DrillUDF-2.0.jar";
    test(String.format("create function using jar '%s'", jar));
    test("select arina_upper(full_name) as upper_name from cp.`employee.json` where full_name = 'Sheri Nowmer'");
    // test("delete function using jar 'DrillUDF-2.0.jar'");
    testBuilder()
        .sqlQuery("delete function using jar 'DrillUDF-2.0.jar'")
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, "UDFs have been deleted successfully - [arina_upper, arina_lower]")
        .go();

    try {
      test("select arina_upper(full_name) as upper_name from cp.`employee.json` where full_name = 'Sheri Nowmer'");
    } catch (UserRemoteException e) {
      assertThat(e.getMessage(), containsString("No match found for function signature arina_upper(<ANY>"));
    }
  }

  @Test
  public void testSendUsingProtosHandle() throws Exception {
    //UserProtos.JarHolder jarHolder = UserProtos.JarHolder.newBuilder().setName("some name").build();
    //UserProtos.JarWithSourcesHolder jarWithSourcesHolder = UserProtos.JarWithSourcesHolder.newBuilder().setJar(jarHolder).build();
    final DrillbitContext context = getDrillbitContext();
    //for (CoordinationProtos.DrillbitEndpoint e : getDrillbitContext().getBits()) {
      // GeneralRPCProtos.Ack a = getDrillbitContext().getController().getTunnel(e).addUDF(jarWithSourcesHolder).get();
      UserBitShared.QueryId id = QueryIdHelper.getQueryIdFromString("111");
      GeneralRPCProtos.Ack a = context.getController().getTunnel(context.getEndpoint()).requestCancelQuery(id).get();
      System.out.println(a.getOk());
    //}
  }

  /*
    QueryId id = QueryIdHelper.getQueryIdFromString(queryId);
  Ack a = work.getContext().getController().getTunnel(info.getForeman()).requestCancelQuery(id).checkedGet(2, TimeUnit.SECONDS);
   */


  private static void addSoftwareLibrary(File file) throws Exception {
    Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
    method.setAccessible(true);
    method.invoke(ClassLoader.getSystemClassLoader(), file.toURI().toURL());
  }

}
