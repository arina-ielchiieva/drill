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

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.drill.exec.udf.dynamic.JarGenerator.JarCreator.createJars;
import static org.apache.drill.exec.udf.dynamic.JarGenerator.ParserUtil.getClassName;
import static org.apache.drill.exec.udf.dynamic.JarGenerator.ParserUtil.getDirectoriesFromPackage;
import static org.apache.drill.exec.udf.dynamic.JarGenerator.ParserUtil.getPackageName;

public class JarGenerator {

  /**
   * Generates source and binary jars in given target directory based on provided resource url.
   * Loads resource content into a string.
   *
   * @param classTemplateResource template resource
   * @param jarName jar name
   * @param targetDir target directory
   * @param confFileName config file name
   */
  public static void generate(URL classTemplateResource, String jarName, String targetDir, String confFileName) throws IOException {
    URI uri;
    try {
      uri = classTemplateResource.toURI();
    } catch (URISyntaxException e) {
      throw new RuntimeException("Error while obtaining resource URI.", e);
    }
    String classTemplate = new String(Files.readAllBytes(Paths.get(uri)));
    generate(classTemplate, jarName, targetDir, confFileName);
  }

  /**
   * Generates source and binary jars in given target directory based on provided class template.
   *
   * @param classTemplate class template
   * @param jarName jar name
   * @param targetDir target directory
   * @param confFileName config file name
   */
  public static void generate(String classTemplate, String jarName, String targetDir, String confFileName) throws IOException {
    // prepare compilation and resource directories
    String compilationDir = Files.createDirectories(Paths.get(targetDir, "compilation")).toString();
    String resourcesDir = Files.createDirectories(Paths.get(targetDir, "resources")).toString();

    if (confFileName != null) {
      createConfigFile(classTemplate, resourcesDir, confFileName);
    }

    compile(compilationDir, classTemplate);
    createJars(compilationDir, resourcesDir, targetDir, jarName);
  }

  /**
   * Creates config file with entry to define which package should be scanned during jar upload.
   * Package name is extracted from given template.
   *
   * @param sourceClass source class template
   * @param resourcesDir resource directory
   * @param confFileName config file name
   */
  private static void createConfigFile(String sourceClass, String resourcesDir, String confFileName) throws IOException {
    Path configPath = Paths.get(resourcesDir, confFileName);
    String pkg = getPackageName(sourceClass);
    Files.write(configPath, String.format("drill.classpath.scanning.packages += \"%s\"", pkg).getBytes());
  }

  /**
   * Creates java and class files based on given source data.
   * Directory structure is created based on package name.
   *
   * @param targetDir target directory
   * @param sourceData source data
   */
  private static void compile(String targetDir, String sourceData) throws IOException {
    String className = getClassName(sourceData);
    String javaPackage = getPackageName(sourceData);
    Path sourcePath = Paths.get(targetDir, getDirectoriesFromPackage(javaPackage), className + ".java");
    // creates .java files
    Files.createDirectories(sourcePath.getParent());
    Files.write(sourcePath, sourceData.getBytes());

    // compiles .java files into .class file(-s)
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    // pass nulls to use standard system in / out / err streams
    compiler.run(null, null, null, sourcePath.toString());
  }

  static class JarCreator {
    private static final String SEPARATOR = "/";

    static void createBinaryJar(String compilationDir, String resourcesDir, String targetDir, String targetName) {
      // matches all class files
      String classFilesMatcher = "glob:**.class";
      createJar(compilationDir, classFilesMatcher, resourcesDir, targetDir, targetName + ".jar");
    }

    static void createSourceJar(String compilationDir, String resourcesDir, String targetDir, String targetName) {
      // matches all java files
      String javaFilesMatcher = "glob:**.java";
      createJar(compilationDir, javaFilesMatcher, resourcesDir, targetDir, targetName + "-sources.jar");
    }

    static void createJars(String compilationDir, String resourcesDir, String targetDir, String targetName) {
      JarCreator.createSourceJar(compilationDir, resourcesDir, targetDir, targetName);
      JarCreator.createBinaryJar(compilationDir, resourcesDir, targetDir, targetName);
    }

    private static void createJar(String compilationDir, String compilationFileMatcher, String resourcesDir, String targetDir, String targetName) {
      try (JarOutputStream jarOut = new JarOutputStream(new FileOutputStream(Paths.get(targetDir, targetName).toFile()))) {
        // checks all files in compilation directory recursively and writes into jar only those that match the pattern
        Path rootCompilationDir = Paths.get(compilationDir);
        PathMatcher matcher = FileSystems.getDefault().getPathMatcher(compilationFileMatcher);
        Files.walk(rootCompilationDir)
            .filter(Files::isRegularFile)
            .filter(matcher::matches)
            .forEach(path -> addFilesToJar(jarOut, rootCompilationDir, path));

        // writes all resource files recursively into the jar
        Path rootResourcesDir = Paths.get(resourcesDir);
        Files.walk(rootResourcesDir)
            .filter(Files::isRegularFile)
            .forEach(path -> addFilesToJar(jarOut, rootResourcesDir, path));
      } catch (Exception e) {
        throw new RuntimeException("Jar creation failed.", e);
      }
    }

    private static void addFilesToJar(JarOutputStream jarOut, Path rootCompilationDir, Path filePath) {
      try {
        // leave only relative path to root compilation directory
        Path relativePath = rootCompilationDir.relativize(filePath);
        String normalizedPath = relativePath.toString().replaceAll("\\\\", SEPARATOR);
        jarOut.putNextEntry(new JarEntry(normalizedPath));
        jarOut.write(Files.readAllBytes(filePath));
        jarOut.closeEntry();
      } catch (Exception e) {
        throw new RuntimeException("Zip entry creation failed", e);
      }
    }
  }

  static class ParserUtil {

    static String getPackageName(String source) {
      Pattern pattern = Pattern.compile("package\\s+([a-zA-Z_][.\\w]*);");
      Matcher m = pattern.matcher(source);
      if (m.find()) {
        return m.group(1);
      }
      // package doesn't exist
      return "";
    }

    static String getClassName(String source) {
      Pattern pattern = Pattern.compile("public\\s+?(class|interface|enum)\\s+([a-zA-Z_$][\\w$]*)");
      Matcher m = pattern.matcher(source);
      if (m.find()) {
        return m.group(2);
      }
      throw new IllegalArgumentException("Class name not be found.");
    }

    static String getDirectoriesFromPackage(String javaPackage) {
      // backslash concat to escape WinOS separator
      return javaPackage.replaceAll("\\.", "\\".concat(FileSystems.getDefault().getSeparator()));
    }
  }


}
