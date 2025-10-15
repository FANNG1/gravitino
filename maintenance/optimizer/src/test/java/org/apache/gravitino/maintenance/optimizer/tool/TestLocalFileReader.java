/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the
 * Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License.  You may obtain a copy
 * of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.maintenance.optimizer.tool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.gravitino.maintenance.optimizer.tool.FileContentReader.ReaderSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestLocalFileReader {

  @TempDir private Path tempDir;

  private LocalFileReader localFileReader;

  @BeforeEach
  public void setUp() {
    localFileReader = new LocalFileReader();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (localFileReader != null) {
      localFileReader.close();
    }
  }

  @Test
  public void testListSourcesSingleFile() throws IOException {
    Path testFile = tempDir.resolve("test.txt");
    Files.writeString(testFile, "test content");

    List<ReaderSource> sources = localFileReader.listSources(testFile.toString());

    assertEquals(1, sources.size());
    assertEquals(testFile.toString(), sources.get(0).getPath());
  }

  @Test
  public void testListSourcesDirectory() throws IOException {
    Path file1 = tempDir.resolve("file1.txt");
    Path file2 = tempDir.resolve("file2.txt");
    Path file3 = tempDir.resolve("file3.txt");

    Files.writeString(file1, "content1");
    Files.writeString(file2, "content2");
    Files.writeString(file3, "content3");

    List<ReaderSource> sources = localFileReader.listSources(tempDir.toString());

    assertEquals(3, sources.size());
    List<String> paths = sources.stream().map(ReaderSource::getPath).toList();
    assertTrue(paths.contains(file1.toString()));
    assertTrue(paths.contains(file2.toString()));
    assertTrue(paths.contains(file3.toString()));
  }

  @Test
  public void testListSourcesDirectoryFiltersOutSubdirectories() throws IOException {
    Path file1 = tempDir.resolve("file1.txt");
    Path subDir = tempDir.resolve("subdir");
    Path file2 = tempDir.resolve("file2.txt");

    Files.writeString(file1, "content1");
    Files.createDirectory(subDir);
    Files.writeString(file2, "content2");

    List<ReaderSource> sources = localFileReader.listSources(tempDir.toString());

    assertEquals(2, sources.size());
    assertTrue(sources.stream().noneMatch(s -> s.getPath().contains("subdir")));
  }

  @Test
  public void testListSourcesEmptyDirectory() {
    List<ReaderSource> sources = localFileReader.listSources(tempDir.toString());

    assertEquals(0, sources.size());
  }

  @Test
  public void testListSourcesNonExistentPath() {
    String nonExistentPath = tempDir.resolve("non-existent.txt").toString();
    assertThrows(
        IllegalArgumentException.class, () -> localFileReader.listSources(nonExistentPath));
  }

  @Test
  public void testGetReader() throws Exception {
    Path testFile = tempDir.resolve("test.txt");
    String content = "line1\nline2\nline3";
    Files.writeString(testFile, content);

    List<ReaderSource> sources = localFileReader.listSources(testFile.toString());

    try (ReaderSource autoCloseSource = sources.get(0)) {
      BufferedReader reader = localFileReader.getReader(autoCloseSource);
      assertNotNull(reader);

      String line1 = reader.readLine();
      assertEquals("line1", line1);

      String line2 = reader.readLine();
      assertEquals("line2", line2);

      String line3 = reader.readLine();
      assertEquals("line3", line3);
    }
  }

  @Test
  public void testGetReaderInvalidSource() {
    ReaderSource invalidSource =
        new ReaderSource() {
          @Override
          public String getPath() {
            return "invalid";
          }

          @Override
          public void close() {}
        };

    assertThrows(IllegalArgumentException.class, () -> localFileReader.getReader(invalidSource));
  }

  @Test
  public void testGetReaderNonExistentFile() throws IOException {
    Path testFile = tempDir.resolve("test.txt");
    Files.writeString(testFile, "content");

    List<ReaderSource> sources = localFileReader.listSources(testFile.toString());
    ReaderSource source = sources.get(0);

    Files.delete(testFile);

    assertThrows(IllegalArgumentException.class, () -> localFileReader.getReader(source));
  }

  @Test
  public void testClose() throws Exception {
    localFileReader.close();
  }

  @Test
  public void testEmptyFile() throws Exception {
    Path testFile = tempDir.resolve("empty.txt");
    Files.writeString(testFile, "");

    List<ReaderSource> sources = localFileReader.listSources(testFile.toString());

    try (ReaderSource autoCloseSource = sources.get(0)) {
      BufferedReader reader = localFileReader.getReader(autoCloseSource);
      String line = reader.readLine();
      assertNull(line);
    }
  }
}
