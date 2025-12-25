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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.recommender.SupportTableStatistics;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.StatisticEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.common.util.IdentifierUtils;
import org.apache.gravitino.maintenance.optimizer.recommender.util.PartitionUtils;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.stats.PartitionRange;
import org.apache.gravitino.stats.PartitionStatistics;
import org.apache.gravitino.stats.Statistic;

public class GravitinoStatisticsProvider implements SupportTableStatistics {

  public static final String NAME = "gravitino-statistics-provider";
  private GravitinoClient gravitinoClient;

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    OptimizerConfig config = optimizerEnv.config();
    String uri = config.get(OptimizerConfig.GRAVITINO_URI_CONFIG);
    String metalake = config.get(OptimizerConfig.GRAVITINO_METALAKE_CONFIG);
    this.gravitinoClient = GravitinoClient.builder(uri).withMetalake(metalake).build();
  }

  @Override
  public List<StatisticEntry<?>> tableStatistics(NameIdentifier tableIdentifier) {
    Table t =
        gravitinoClient
            .loadCatalog(IdentifierUtils.getCatalogNameFromTableIdentifier(tableIdentifier))
            .asTableCatalog()
            .loadTable(IdentifierUtils.removeCatalogFromIdentifier(tableIdentifier));
    List<Statistic> statistics = t.supportsStatistics().listStatistics();
    return statistics.stream()
        .filter(statistic -> statistic.value().isPresent())
        .map(
            statistic ->
                (StatisticEntry<?>)
                    new StatisticEntryImpl(statistic.name(), statistic.value().get()))
        .collect(Collectors.toList());
  }

  @Override
  public Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics(
      NameIdentifier tableIdentifier) {
    Table t =
        gravitinoClient
            .loadCatalog(IdentifierUtils.getCatalogNameFromTableIdentifier(tableIdentifier))
            .asTableCatalog()
            .loadTable(IdentifierUtils.removeCatalogFromIdentifier(tableIdentifier));
    List<PartitionStatistics> partitionStatistics =
        t.supportsPartitionStatistics().listPartitionStatistics(PartitionRange.ALL_PARTITIONS);

    Map<PartitionPath, List<StatisticEntry<?>>> statisticsByPartition = new LinkedHashMap<>();
    partitionStatistics.forEach(
        statistic -> toPartitionStatistics(statistic, statisticsByPartition));
    return statisticsByPartition;
  }

  private void toPartitionStatistics(
      PartitionStatistics partitionStatistics,
      Map<PartitionPath, List<StatisticEntry<?>>> statisticsByPartition) {
    PartitionPath partitions =
        PartitionUtils.parseGravitinoPartitionName(partitionStatistics.partitionName());
    Arrays.stream(partitionStatistics.statistics())
        .filter(statistic -> statistic.value().isPresent())
        .forEach(
            statistic ->
                statisticsByPartition
                    .computeIfAbsent(partitions, key -> new java.util.ArrayList<>())
                    .add(new StatisticEntryImpl<>(statistic.name(), statistic.value().get())));
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void close() throws Exception {
    if (gravitinoClient != null) {
      gravitinoClient.close();
    }
  }
}
