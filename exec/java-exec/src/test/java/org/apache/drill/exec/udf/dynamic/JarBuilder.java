/*
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
package org.apache.drill.exec.udf.dynamic;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import org.apache.maven.cli.MavenCli;
import org.apache.maven.cli.logging.Slf4jLogger;
import org.codehaus.plexus.DefaultPlexusContainer;
import org.codehaus.plexus.PlexusContainer;
import org.codehaus.plexus.logging.BaseLoggerManager;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

public class JarBuilder {

  private final MavenCli cli;

  public JarBuilder() {
    this.cli = new MavenCli() {
      @Override
      protected void customizeContainer(PlexusContainer container) {
        ((DefaultPlexusContainer) container).setLoggerManager(new BaseLoggerManager() {
          @Override
          protected org.codehaus.plexus.logging.Logger createLogger(String s) {
            return new Slf4jLogger(setupLogger(JarBuilder.class.getName(), Level.INFO));
          }
        });
      }
    };
  }

  /**
   * Builds jars using embedded maven. Includes files / resources based given pattern,
   * otherwise using defaults provided in pom.xml.
   *
   * @param jarName jar name
   * @param projectDir project dir
   * @param includeFiles pattern indicating which files should be included
   * @param includeResources pattern indicating which resources should be included
   *
   * @return build exit code, 0 if build was successful
   */
  public int build(String jarName, String projectDir, String includeFiles, String includeResources) {
    System.setProperty("maven.multiModuleProjectDirectory", projectDir);
    List<String> params = new LinkedList<>();
    params.add("clean");
    params.add("package");
    params.add("-DskipTests");
    params.add("-Djar.finalName=" + jarName);
    if (includeFiles != null) {
      params.add("-Dinclude.files=" + includeFiles);
    }
    if (includeResources != null) {
      params.add("-Dinclude.resources=" + includeResources);
    }
    return cli.doMain(params.toArray(new String[params.size()]), projectDir, System.out, System.err);
  }

  private static Logger setupLogger(String string, Level logLevel) {
    LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
    ConsoleAppender<ILoggingEvent> consoleAppender = new ConsoleAppender<>();
    consoleAppender.setContext(loggerContext);
    Logger logger = (Logger) LoggerFactory.getLogger(string);
    logger.addAppender(consoleAppender);
    logger.setLevel(logLevel);
    return logger;
  }

}
