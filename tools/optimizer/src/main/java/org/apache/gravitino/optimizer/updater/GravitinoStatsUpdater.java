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

package org.apache.gravitino.optimizer.updater;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.optimizer.api.common.PartitionStatistic;
import org.apache.gravitino.optimizer.api.common.SingleStatistic;
import org.apache.gravitino.optimizer.api.updater.StatsUpdater;
import org.apache.gravitino.optimizer.common.OptimizerEnv;
import org.apache.gravitino.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.optimizer.common.util.IdentifierUtils;
import org.apache.gravitino.optimizer.updater.util.PartitionUtils;
import org.apache.gravitino.stats.PartitionStatisticsUpdate;
import org.apache.gravitino.stats.StatisticValue;

public class GravitinoStatsUpdater implements StatsUpdater {

  public static final String GRAVITINO_STATS_UPDATER_NAME = "gravitino-status-updater";
  private GravitinoClient gravitinoClient;
  private String defaultCatalogName;

  @Override
  public String name() {
    return GRAVITINO_STATS_UPDATER_NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    OptimizerConfig config = optimizerEnv.config();
    String uri = config.get(OptimizerConfig.GRAVITINO_URI_CONFIG);
    String metalake = config.get(OptimizerConfig.GRAVITINO_METALAKE_CONFIG);
    this.gravitinoClient = GravitinoClient.builder(uri).withMetalake(metalake).build();
    this.defaultCatalogName = config.get(OptimizerConfig.GRAVITINO_DEFAULT_CATALOG_CONFIG);
  }

  @Override
  public void updateTableStatistics(
      NameIdentifier tableIdentifier, List<SingleStatistic<?>> statistics) {
    doUpdateTableStatistics(tableIdentifier, getTableStatisticsMap(statistics));
    doUpdatePartitionStatistics(tableIdentifier, getPartitionStatsUpdates(statistics));
  }

  private void doUpdateTableStatistics(
      NameIdentifier tableIdentifier, Map<String, StatisticValue<?>> tableStatsMap) {
    if (tableStatsMap.isEmpty()) {
      return;
    }
    gravitinoClient
        .loadCatalog(
            IdentifierUtils.getCatalogNameFromTableIdentifier(tableIdentifier, defaultCatalogName))
        .asTableCatalog()
        .loadTable(IdentifierUtils.removeCatalogFromIdentifier(tableIdentifier))
        .supportsStatistics()
        .updateStatistics(tableStatsMap);
  }

  private Map<String, StatisticValue<?>> getTableStatisticsMap(
      List<SingleStatistic<?>> statistics) {
    return statistics.stream()
        .filter(statistic -> !(statistic instanceof PartitionStatistic))
        .collect(Collectors.toMap(SingleStatistic::name, SingleStatistic::value));
  }

  private List<PartitionStatisticsUpdate> getPartitionStatsUpdates(
      List<SingleStatistic<?>> partitionStatistics) {
    return partitionStatistics.stream()
        .filter(statistic -> statistic instanceof PartitionStatistic)
        .map(statistic -> (PartitionStatistic) statistic)
        .map(
            partitionStatistic ->
                new PartitionStatisticsUpdate() {
                  @Override
                  public String partitionName() {
                    return PartitionUtils.getGravitinoPartitionName(
                        partitionStatistic.partitionName());
                  }

                  @Override
                  public Map<String, StatisticValue<?>> statistics() {
                    return Map.of(partitionStatistic.name(), partitionStatistic.value());
                  }
                })
        .collect(Collectors.toList());
  }

  private void doUpdatePartitionStatistics(
      NameIdentifier tableIdentifier, List<PartitionStatisticsUpdate> partitionStatisticsUpdates) {
    if (partitionStatisticsUpdates.isEmpty()) {
      return;
    }
    gravitinoClient
        .loadCatalog(
            IdentifierUtils.getCatalogNameFromTableIdentifier(tableIdentifier, defaultCatalogName))
        .asTableCatalog()
        .loadTable(IdentifierUtils.removeCatalogFromIdentifier(tableIdentifier))
        .supportsPartitionStatistics()
        .updatePartitionStatistics(partitionStatisticsUpdates);
  }
}
