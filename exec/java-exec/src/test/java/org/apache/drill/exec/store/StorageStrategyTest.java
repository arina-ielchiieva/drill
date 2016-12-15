/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store;

import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StorageStrategyTest {

  private static final Configuration configuration = new Configuration();
  private static final FsPermission full_permission = new FsPermission("777");
  private FileSystem fs;

  @Before
  public void setup() throws Exception {
    initFileSystem();
  }

  @Test
  public void testPermissionAndDeleteOnExitFalse() throws Exception {
    Path path = prepareStorageDirectory();
    Path file = createFileWithFullPermission(path);

    StorageStrategy storageStrategy = new StorageStrategy("775", "644", false);
    storageStrategy.applyToFolder(fs, path);
    storageStrategy.applyToFile(fs, file);

    FsPermission folderPermission = new FsPermission(storageStrategy.getFolderPermission());
    FsPermission filePermission = new FsPermission(storageStrategy.getFilePermission());
    checkFolderAndFilePermission(fs, path, folderPermission, filePermission, 1);

    // close and open file system to check that path is present
    initFileSystem();
    checkFolderAndFilePermission(fs, path, folderPermission, filePermission, 1);
  }

  @Test
  public void testPermissionAndDeleteOnExitTrue() throws Exception {
    Path path = prepareStorageDirectory();
    Path file = createFileWithFullPermission(path);

    StorageStrategy storageStrategy = new StorageStrategy("700", "600", true);
    storageStrategy.applyToFolder(fs, path);
    storageStrategy.applyToFile(fs, file);

    FsPermission folderPermission = new FsPermission(storageStrategy.getFolderPermission());
    FsPermission filePermission = new FsPermission(storageStrategy.getFilePermission());
    checkFolderAndFilePermission(fs, path, folderPermission, filePermission, 1);

    // close and open file system to check that path is absent
    initFileSystem();
    assertFalse("Path should be absent", fs.exists(path));
  }

  private Path prepareStorageDirectory() throws IOException {
    File storageDirectory = Files.createTempDir();
    storageDirectory.deleteOnExit();
    Path path = new Path(storageDirectory.toURI().getPath());
    fs.setPermission(path, full_permission);
    return path;
  }

  private Path createFileWithFullPermission(Path path) throws IOException {
    File tempFile = File.createTempFile(getClass().getSimpleName(), null, new File(path.toUri().getPath()));
    Path tempFilePath = new Path(tempFile.toURI().getPath());
    fs.setPermission(tempFilePath, full_permission);
    return tempFilePath;
  }

  private void initFileSystem() throws IOException {
    if (fs != null) {
      try {
        fs.close();
      } catch (Exception e) {
        // do nothing
      }
    }
    fs = FileSystem.get(configuration);
  }

  private void checkFolderAndFilePermission(FileSystem fs,
                                            Path path,
                                            FsPermission expectedFolderPermission,
                                            FsPermission expectedFilePermission,
                                            int fileCount) throws IOException {
    assertTrue("Path should exist", fs.exists(path));
    assertEquals("Permission should match", expectedFolderPermission, fs.getFileStatus(path).getPermission());
    int count = 0;
    RemoteIterator<LocatedFileStatus> files = fs.listFiles(path, false);
    while (files.hasNext()) {
      count++;
      assertEquals("Permission should match", expectedFilePermission, files.next().getPermission());
    }
    assertEquals("Number of file should match", fileCount, count);
  }
}
