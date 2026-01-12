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

package org.apache.gravitino.maintenance.optimizer.recommender.statistics;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.recommender.SupportTableStatistics;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;

/** A statistics provider that reads table statistics from a JSON-lines file. */
public class FileStatisticsProvider implements SupportTableStatistics {

  public static final String FILE_STATISTICS_PROVIDER_NAME = "file-stats-provider";

  public static final String STATISTICS_FILE_PATH_CONFIG =
      OptimizerConfig.OPTIMIZER_PREFIX
          + "recommender."
          + FILE_STATISTICS_PROVIDER_NAME
          + ".file-path";

  private FileStatisticsReader fileStatisticsReader;

  @Override
  public String name() {
    return FILE_STATISTICS_PROVIDER_NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    String path = optimizerEnv.config().getRawString(STATISTICS_FILE_PATH_CONFIG);
    if (StringUtils.isBlank(path)) {
      throw new IllegalArgumentException(
          STATISTICS_FILE_PATH_CONFIG + " must be provided for FileStatisticsProvider");
    }
    String defaultCatalog =
        optimizerEnv.config().get(OptimizerConfig.GRAVITINO_DEFAULT_CATALOG_CONFIG);
    this.fileStatisticsReader = new FileStatisticsReader(Path.of(path), defaultCatalog);
  }

  @Override
  public List<StatisticEntry<?>> tableStatistics(NameIdentifier tableIdentifier) {
    return fileStatisticsReader.readTableStatistics(tableIdentifier);
  }

  @Override
  public Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics(
      NameIdentifier tableIdentifier) {
    return Collections.emptyMap();
  }

  @Override
  public void close() throws Exception {}
}
