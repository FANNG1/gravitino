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

package org.apache.gravitino.maintenance.optimizer.updater.computer;

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
import org.apache.gravitino.maintenance.optimizer.common.OptimizerContent;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.StatisticsComputerContent;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.recommender.statistics.FileStatisticsReader;
import org.apache.gravitino.maintenance.optimizer.recommender.statistics.PayloadStatisticsReader;
import org.apache.gravitino.maintenance.optimizer.recommender.statistics.StatisticsReader;

public class LocalStatisticsComputer
    implements SupportsCalculateBulkTableStatistics, SupportsCalculateBulkJobStatistics {

  public static final String LOCAL_STATISTICS_COMPUTER_NAME = "local-stats-computer";

  private StatisticsReader statisticsReader;

  @Override
  public String name() {
    return LOCAL_STATISTICS_COMPUTER_NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    OptimizerContent content = optimizerEnv.content();
    Preconditions.checkArgument(
        content instanceof StatisticsComputerContent,
        "StatisticsComputerContent is required for LocalStatisticsComputer");

    StatisticsComputerContent statisticsContent = (StatisticsComputerContent) content;
    String defaultCatalog =
        optimizerEnv.config().get(OptimizerConfig.GRAVITINO_DEFAULT_CATALOG_CONFIG);

    if (StringUtils.isNotBlank(statisticsContent.statisticsFilePath())) {
      this.statisticsReader =
          new FileStatisticsReader(Path.of(statisticsContent.statisticsFilePath()), defaultCatalog);
      return;
    }

    String payload = statisticsContent.statisticsPayload();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(payload), "Statistics payload must be provided");
    this.statisticsReader = new PayloadStatisticsReader(payload, defaultCatalog);
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
