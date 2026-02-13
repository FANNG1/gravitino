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

package org.apache.gravitino.maintenance.optimizer.updater.calculator;

import com.google.common.base.Preconditions;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.TableStatisticsBundle;
import org.apache.gravitino.maintenance.optimizer.api.updater.SupportsCalculateBulkJobStatistics;
import org.apache.gravitino.maintenance.optimizer.api.updater.SupportsCalculateBulkTableStatistics;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.recommender.statistics.FileStatisticsReader;
import org.apache.gravitino.maintenance.optimizer.recommender.statistics.PayloadStatisticsReader;
import org.apache.gravitino.maintenance.optimizer.recommender.statistics.StatisticsReader;

/**
 * Statistics calculator that reads statistics from either a local file path or an inline payload.
 */
public class LocalStatisticsCalculator
    implements SupportsCalculateBulkTableStatistics, SupportsCalculateBulkJobStatistics {

  public static final String LOCAL_STATISTICS_CALCULATOR_NAME = "local-stats-calculator";
  public static final String STATISTICS_FILE_PATH_CONFIG =
      OptimizerConfig.OPTIMIZER_PREFIX
          + "updater."
          + LOCAL_STATISTICS_CALCULATOR_NAME
          + ".statisticsFilePath";
  public static final String STATISTICS_PAYLOAD_CONFIG =
      OptimizerConfig.OPTIMIZER_PREFIX
          + "updater."
          + LOCAL_STATISTICS_CALCULATOR_NAME
          + ".statisticsPayload";

  private StatisticsReader statisticsReader;

  @Override
  public String name() {
    return LOCAL_STATISTICS_CALCULATOR_NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    String defaultCatalog =
        optimizerEnv.config().get(OptimizerConfig.GRAVITINO_DEFAULT_CATALOG_CONFIG);
    String statisticsFilePath = optimizerEnv.config().getRawString(STATISTICS_FILE_PATH_CONFIG);
    String statisticsPayload = optimizerEnv.config().getRawString(STATISTICS_PAYLOAD_CONFIG);

    Preconditions.checkArgument(
        !(StringUtils.isNotBlank(statisticsFilePath) && StringUtils.isNotBlank(statisticsPayload)),
        "Only one of %s or %s can be provided",
        STATISTICS_FILE_PATH_CONFIG,
        STATISTICS_PAYLOAD_CONFIG);

    if (StringUtils.isNotBlank(statisticsFilePath)) {
      this.statisticsReader = new FileStatisticsReader(Path.of(statisticsFilePath), defaultCatalog);
      return;
    }

    Preconditions.checkArgument(
        StringUtils.isNotBlank(statisticsPayload),
        "Statistics payload must be provided by config key %s",
        STATISTICS_PAYLOAD_CONFIG);
    this.statisticsReader = new PayloadStatisticsReader(statisticsPayload, defaultCatalog);
  }

  @Override
  public TableStatisticsBundle calculateTableStatistics(NameIdentifier tableIdentifier) {
    List<StatisticEntry<?>> tableStatistics = statisticsReader.readTableStatistics(tableIdentifier);
    Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics =
        statisticsReader.readPartitionStatistics(tableIdentifier);
    return new TableStatisticsBundle(tableStatistics, partitionStatistics);
  }

  @Override
  public Map<NameIdentifier, TableStatisticsBundle> calculateBulkTableStatistics() {
    Map<NameIdentifier, List<StatisticEntry<?>>> tableStatistics =
        statisticsReader.readAllTableStatistics();
    Map<NameIdentifier, Map<PartitionPath, List<StatisticEntry<?>>>> partitionStatistics =
        statisticsReader.readAllPartitionStatistics();

    Map<NameIdentifier, TableStatisticsBundle> bundles = new HashMap<>();
    if (tableStatistics != null) {
      tableStatistics.forEach(
          (identifier, statistics) ->
              bundles.put(
                  identifier,
                  new TableStatisticsBundle(
                      statistics,
                      partitionStatistics != null
                          ? partitionStatistics.getOrDefault(identifier, Map.of())
                          : Map.of())));
    }
    if (partitionStatistics != null) {
      partitionStatistics.forEach(
          (identifier, partitions) ->
              bundles.putIfAbsent(identifier, new TableStatisticsBundle(List.of(), partitions)));
    }
    return bundles;
  }

  @Override
  public List<StatisticEntry<?>> calculateJobStatistics(NameIdentifier jobIdentifier) {
    return statisticsReader.readJobStatistics(jobIdentifier);
  }

  @Override
  public Map<NameIdentifier, List<StatisticEntry<?>>> calculateAllJobStatistics() {
    return statisticsReader.readAllJobStatistics();
  }
}
