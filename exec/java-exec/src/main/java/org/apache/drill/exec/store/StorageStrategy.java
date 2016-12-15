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
package org.apache.drill.exec.store;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;

/** Contains list of parameters that will be used to store path / files on file system. */
public class StorageStrategy {

  /**
   * Primary is used for persistent tables.
   * For directories: drwxrwxr-x (owner and group have full access, others can read and execute).
   * For files: -rw-r--r-- (owner can read and write, group and others can read).
   * Folders and files are not deleted on file system close.
   */
  public static final StorageStrategy PERSISTENT = new StorageStrategy("775", "644", false);

  /**
   * Primary is used for temporary tables.
   * For directories: drwx------ (owner has full access, group and others have no access).
   * For files: -rw------- (owner can read and write, group and others have no access).
   * Folders and files are deleted on file system close.
   */
  public static final StorageStrategy TEMPORARY = new StorageStrategy("700", "600", true);

  private final String folderPermission;
  private final String filePermission;
  private final boolean deleteOnExit;

  @JsonCreator
  public StorageStrategy(@JsonProperty("folderPermission") String folderPermission,
                         @JsonProperty("filePermission") String filePermission,
                         @JsonProperty("deleteOnExit") boolean deleteOnExit) {
    this.folderPermission = folderPermission;
    this.filePermission = filePermission;
    this.deleteOnExit = deleteOnExit;
  }

  public String getFolderPermission() {
    return folderPermission;
  }

  public String getFilePermission() { return filePermission; }

  public boolean isDeleteOnExit() {
    return deleteOnExit;
  }

  /**
   * Applies storage strategy to passed path on passed file system.
   * Sets appropriate permission
   * and adds to file system delete on exit list if needed.
   *
   * @param fs file system where path is located
   * @param path folder location
   * @throws IOException is thrown in case of problems while setting permission
   *         or adding path to delete on exit list
   */
  @JsonIgnore
  public void applyToFolder(FileSystem fs, Path path) throws IOException {
    fs.setPermission(path, new FsPermission(folderPermission));
    if (deleteOnExit) {
      fs.deleteOnExit(path);
    }
  }

  /**
   * Applies storage strategy to passed file on passed file system.
   * Sets appropriate permission
   * and adds to file system delete on exit list if needed.
   *
   * @param fs file system where file is located
   * @param path file location
   * @throws IOException is thrown in case of problems while setting permission
   *         or adding path to delete on exit list
   */
  @JsonIgnore
  public void applyToFile(FileSystem fs, Path path) throws IOException {
    fs.setPermission(path, new FsPermission(filePermission));
    if (deleteOnExit) {
      fs.deleteOnExit(path);
    }
  }
}