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

package org.apache.gravitino.maintenance.optimizer.monitor.job;

import java.nio.file.Path;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.monitor.JobProvider;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;

/** A job provider that reads table-job mappings from a JSON-lines file. */
public class FileJobProvider implements JobProvider {

  public static final String NAME = "file-job-provider";

  public static final String JOB_FILE_PATH_CONFIG =
      OptimizerConfig.OPTIMIZER_PREFIX + "monitor." + NAME + ".file-path";

  private FileJobReader jobReader;

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    String path = optimizerEnv.config().getRawString(JOB_FILE_PATH_CONFIG);
    if (StringUtils.isBlank(path)) {
      throw new IllegalArgumentException(
          JOB_FILE_PATH_CONFIG + " must be provided for FileJobProvider");
    }
    String defaultCatalog =
        optimizerEnv.config().get(OptimizerConfig.GRAVITINO_DEFAULT_CATALOG_CONFIG);
    this.jobReader = new FileJobReader(Path.of(path), defaultCatalog);
  }

  @Override
  public List<NameIdentifier> jobIdentifiers(NameIdentifier tableIdentifier) {
    return jobReader.readJobNames(tableIdentifier);
  }

  @Override
  public void close() throws Exception {}
}
