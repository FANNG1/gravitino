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

import com.google.common.base.Preconditions;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileReader implements FileContentReader {

  private static final Logger LOG = LoggerFactory.getLogger(LocalFileReader.class);

  @Override
  public List<ReaderSource> listSources(String path) {
    Path filePath = Path.of(path);
    Preconditions.checkArgument(Files.exists(filePath), "Path does not exist: %s", path);

    if (Files.isDirectory(filePath)) {
      return listLocalDirectory(filePath);
    } else {
      Preconditions.checkArgument(Files.isRegularFile(filePath), "Path is not a file: %s", path);
      return List.of(new LocalReaderSource(filePath));
    }
  }

  @Override
  public BufferedReader getReader(ReaderSource source) {
    Preconditions.checkArgument(
        source instanceof LocalReaderSource, "Source must be LocalReaderSource");
    LocalReaderSource localSource = (LocalReaderSource) source;
    return localSource.getReader();
  }

  private List<ReaderSource> listLocalDirectory(Path dirPath) {
    LOG.info("Listing local directory: {}", dirPath);

    try (Stream<Path> files = Files.list(dirPath)) {
      return files
          .filter(Files::isRegularFile)
          .sorted()
          .map(LocalReaderSource::new)
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to list directory " + dirPath, e);
    }
  }

  @Override
  public void close() throws Exception {
    // no-op
  }

  private static class LocalReaderSource implements ReaderSource {
    private final Path path;
    private BufferedReader reader;

    public LocalReaderSource(Path path) {
      this.path = path;
    }

    @Override
    public String getPath() {
      return path.toString();
    }

    public BufferedReader getReader() {
      LOG.info("Reading local file: {}", path);
      try {
        reader = Files.newBufferedReader(path);
        return reader;
      } catch (IOException e) {
        throw new IllegalArgumentException("Failed to read file " + path, e);
      }
    }

    @Override
    public void close() throws Exception {
      if (reader != null) {
        reader.close();
      }
    }
  }
}
