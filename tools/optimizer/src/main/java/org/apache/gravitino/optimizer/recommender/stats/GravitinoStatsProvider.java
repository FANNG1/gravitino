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
import org.apache.gravitino.optimizer.api.common.PartitionStatistic;
import org.apache.gravitino.optimizer.api.common.SingleStatistic;
import org.apache.gravitino.optimizer.api.recommender.SupportTableStats;
import org.apache.gravitino.optimizer.common.OptimizerEnv;
import org.apache.gravitino.optimizer.common.SinglePartition;
import org.apache.gravitino.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.optimizer.common.util.IdentifierUtils;
import org.apache.gravitino.optimizer.updater.impl.PartitionStatisticImpl;
import org.apache.gravitino.optimizer.updater.impl.SingleStatisticImpl;
import org.apache.gravitino.optimizer.updater.impl.util.PartitionUtils;
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
  public List<SingleStatistic> getTableStats(NameIdentifier tableIdentifier) {
    Table t =
        gravitinoClient
            .loadCatalog(
                IdentifierUtils.getCatalogNameFromTableIdentifier(
                    tableIdentifier, defaultCatalogName))
            .asTableCatalog()
            .loadTable(tableIdentifier);
    List<Statistic> statistics = t.supportsStatistics().listStatistics();
    return statistics.stream()
        .filter(statistic -> statistic.value().isPresent())
        .map(statistic -> new SingleStatisticImpl(statistic.name(), statistic.value().get()))
        .collect(Collectors.toList());
  }

  @Override
  public List<PartitionStatistic> getPartitionStats(NameIdentifier tableIdentifier) {
    Table t =
        gravitinoClient
            .loadCatalog(
                IdentifierUtils.getCatalogNameFromTableIdentifier(
                    tableIdentifier, defaultCatalogName))
            .asTableCatalog()
            .loadTable(tableIdentifier);
    List<PartitionStatistics> partitionStatistics =
        t.supportsPartitionStatistics().listPartitionStatistics(PartitionRange.ALL_PARTITIONS);

    return partitionStatistics.stream()
        .flatMap(statistic -> toPartitionStatistics(statistic).stream())
        .collect(Collectors.toList());
  }

  private List<PartitionStatistic> toPartitionStatistics(PartitionStatistics partitionStatistics) {
    List<SinglePartition> partitions =
        PartitionUtils.parseGravitinoPartitionName(partitionStatistics.partitionName());
    return Arrays.stream(partitionStatistics.statistics())
        .map(
            statistic ->
                new PartitionStatisticImpl(statistic.name(), statistic.value().get(), partitions))
        .collect(Collectors.toList());
  }

  @Override
  public String name() {
    return GRAVITINO_STATS_PROVIDER_NAME;
  }
}
