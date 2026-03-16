/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.maintenance.optimizer.updater.calculator.s3;

import com.google.common.base.Preconditions;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import org.apache.gravitino.maintenance.optimizer.common.reader.S3FileReader;
import org.apache.gravitino.maintenance.optimizer.common.util.S3Utils;
import org.apache.gravitino.maintenance.optimizer.updater.calculator.AbstractStatisticsImporter;

public class S3StatisticsImporter extends AbstractStatisticsImporter<S3FileReader.S3Source>
    implements Closeable {

  private final String s3Path;
  private final S3FileReader fileReader;

  public S3StatisticsImporter(String s3Path, String s3Region, String defaultCatalogName) {
    super(defaultCatalogName);
    this.s3Path = Objects.requireNonNull(s3Path, "s3Path cannot be null");
    Preconditions.checkArgument(S3Utils.isS3Path(s3Path), "Path must be an S3 path: %s", s3Path);
    this.fileReader = new S3FileReader(s3Region);
  }

  @Override
  protected List<S3FileReader.S3Source> listSources() {
    return fileReader.listSources(s3Path);
  }

  @Override
  protected BufferedReader openReader(S3FileReader.S3Source source) {
    return fileReader.getReader(source);
  }

  @Override
  public void close() throws IOException {
    fileReader.close();
  }
}
