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

package org.apache.gravitino.optimizer.recommender.stats;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.optimizer.api.common.PartitionEntry;
import org.apache.gravitino.optimizer.api.common.PartitionStatisticEntry;
import org.apache.gravitino.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.optimizer.api.recommender.SupportTableStats;
import org.apache.gravitino.optimizer.common.OptimizerEnv;
import org.apache.gravitino.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.optimizer.common.util.IdentifierUtils;
import org.apache.gravitino.optimizer.updater.PartitionStatisticEntryImpl;
import org.apache.gravitino.optimizer.updater.StatisticEntryImpl;
import org.apache.gravitino.optimizer.updater.util.PartitionUtils;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.stats.PartitionRange;
import org.apache.gravitino.stats.PartitionStatistics;
import org.apache.gravitino.stats.Statistic;

public class GravitinoStatsProvider implements SupportTableStats {

  public static final String GRAVITINO_STATS_PROVIDER_NAME = "gravitino";
  private GravitinoClient gravitinoClient;
  private String defaultCatalogName;

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    OptimizerConfig config = optimizerEnv.config();
    String uri = config.get(OptimizerConfig.GRAVITINO_URI_CONFIG);
    String metalake = config.get(OptimizerConfig.GRAVITINO_METALAKE_CONFIG);
    this.gravitinoClient = GravitinoClient.builder(uri).withMetalake(metalake).build();
    this.defaultCatalogName = config.get(OptimizerConfig.GRAVITINO_DEFAULT_CATALOG_CONFIG);
  }

  @Override
  public List<StatisticEntry<?>> getTableStats(NameIdentifier tableIdentifier) {
    Table t =
        gravitinoClient
            .loadCatalog(
                IdentifierUtils.getCatalogNameFromTableIdentifier(
                    tableIdentifier, defaultCatalogName))
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
  public List<PartitionStatisticEntry> getPartitionStats(NameIdentifier tableIdentifier) {
    Table t =
        gravitinoClient
            .loadCatalog(
                IdentifierUtils.getCatalogNameFromTableIdentifier(
                    tableIdentifier, defaultCatalogName))
            .asTableCatalog()
            .loadTable(IdentifierUtils.removeCatalogFromIdentifier(tableIdentifier));
    List<PartitionStatistics> partitionStatistics =
        t.supportsPartitionStatistics().listPartitionStatistics(PartitionRange.ALL_PARTITIONS);

    return partitionStatistics.stream()
        .flatMap(statistic -> toPartitionStatistics(statistic).stream())
        .collect(Collectors.toList());
  }

  private List<PartitionStatisticEntry> toPartitionStatistics(
      PartitionStatistics partitionStatistics) {
    List<PartitionEntry> partitions =
        PartitionUtils.parseGravitinoPartitionName(partitionStatistics.partitionName());
    return Arrays.stream(partitionStatistics.statistics())
        .map(
            statistic ->
                new PartitionStatisticEntryImpl(
                    statistic.name(), statistic.value().get(), partitions))
        .collect(Collectors.toList());
  }

  @Override
  public String name() {
    return GRAVITINO_STATS_PROVIDER_NAME;
  }
}
