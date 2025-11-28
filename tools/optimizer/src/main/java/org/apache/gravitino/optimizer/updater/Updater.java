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

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.optimizer.api.common.MetricsPoint;
import org.apache.gravitino.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.optimizer.api.updater.MetricsUpdater;
import org.apache.gravitino.optimizer.api.updater.StatsComputer;
import org.apache.gravitino.optimizer.api.updater.StatsUpdater;
import org.apache.gravitino.optimizer.api.updater.SupportComputeTableStats;
import org.apache.gravitino.optimizer.api.updater.SupportJobStats;
import org.apache.gravitino.optimizer.common.MetricPointImpl;
import org.apache.gravitino.optimizer.common.OptimizerEnv;
import org.apache.gravitino.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.optimizer.common.util.InstanceLoaderUtils;
import org.apache.gravitino.optimizer.common.util.ProviderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Updater {
  private static final Logger LOG = LoggerFactory.getLogger(Updater.class);

  private StatsUpdater statsUpdater;
  private MetricsUpdater metricsUpdater;
  private OptimizerEnv optimizerEnv;

  public Updater(OptimizerEnv optimizerEnv) {
    this.optimizerEnv = optimizerEnv;
    this.statsUpdater = loadStatsUpdater(optimizerEnv.config());
    statsUpdater.initialize(optimizerEnv);
    this.metricsUpdater = loadMetricsUpdater(optimizerEnv.config());
    metricsUpdater.initialize(optimizerEnv);
  }

  public void update(
      String statsComputerName, List<NameIdentifier> nameIdentifiers, UpdateType updateType) {
    StatsComputer computer = getStatsComputer(statsComputerName);
    for (NameIdentifier nameIdentifier : nameIdentifiers) {
      if (computer instanceof SupportComputeTableStats) {
        SupportComputeTableStats supportTableStats = ((SupportComputeTableStats) computer);
        List<StatisticEntry<?>> statistics = supportTableStats.computeTableStats(nameIdentifier);
        LOG.info(
            "Updating table stats/metrics: computer={}, updateType={}, identifier={}",
            statsComputerName,
            updateType,
            nameIdentifier);
        updateTable(statistics, nameIdentifier, updateType);
      }
      if (computer instanceof SupportJobStats && updateType.equals(UpdateType.METRICS)) {
        SupportJobStats supportJobStats = ((SupportJobStats) computer);
        List<StatisticEntry<?>> statistics = supportJobStats.computeJobStats(nameIdentifier);
        LOG.info(
            "Updating job metrics: computer={}, identifier={}", statsComputerName, nameIdentifier);
        updateJob(statistics, nameIdentifier);
      }
    }
  }

  public void updateAll(String statsComputerName, UpdateType updateType) {
    StatsComputer computer = getStatsComputer(statsComputerName);

    if (computer instanceof SupportComputeTableStats supportTableStats) {
      java.util.Map<NameIdentifier, List<StatisticEntry<?>>> allTableStats =
          supportTableStats.computeAllTableStats();
      allTableStats.forEach(
          (identifier, statistics) -> updateTable(statistics, identifier, updateType));
    }

    if (computer instanceof SupportJobStats supportJobStats
        && UpdateType.METRICS.equals(updateType)) {
      java.util.Map<NameIdentifier, List<StatisticEntry<?>>> allJobStats =
          supportJobStats.computeAllJobStats();
      allJobStats.forEach((identifier, statistics) -> updateJob(statistics, identifier));
    }
  }

  @VisibleForTesting
  public MetricsUpdater getMetricsUpdater() {
    return metricsUpdater;
  }

  private void updateTable(
      List<StatisticEntry<?>> statistics, NameIdentifier tableIdentifier, UpdateType updateType) {
    switch (updateType) {
      case STATS:
        LOG.info(
            "Persisting table stats: identifier={}, count={}, details={}",
            tableIdentifier,
            statistics != null ? statistics.size() : 0,
            summarize(statistics));
        statsUpdater.updateTableStatistics(tableIdentifier, statistics);
        break;
      case METRICS:
        LOG.info(
            "Persisting table metrics: identifier={}, count={}, details={}",
            tableIdentifier,
            statistics != null ? statistics.size() : 0,
            summarize(statistics));
        metricsUpdater.updateTableMetrics(tableIdentifier, toMetrics(statistics));
        break;
    }
  }

  private void updateJob(List<StatisticEntry<?>> statistics, NameIdentifier jobIdentifier) {
    LOG.info(
        "Persisting job metrics: identifier={}, count={}, details={}",
        jobIdentifier,
        statistics != null ? statistics.size() : 0,
        summarize(statistics));
    metricsUpdater.updateJobMetrics(jobIdentifier, toMetrics(statistics));
  }

  private String summarize(List<StatisticEntry<?>> statistics) {
    if (statistics == null || statistics.isEmpty()) {
      return "[]";
    }
    int limit = Math.min(statistics.size(), 20);
    String summary =
        statistics.stream()
            .limit(limit)
            .map(stat -> stat.name() + "=" + stat.value().value())
            .collect(java.util.stream.Collectors.joining(", ", "[", "]"));
    if (statistics.size() > limit) {
      summary = summary + " ... (" + statistics.size() + " total)";
    }
    return summary;
  }

  private List<MetricsPoint> toMetrics(List<StatisticEntry<?>> statistics) {
    return statistics.stream()
        .map(stat -> (MetricsPoint) new MetricPointImpl(System.currentTimeMillis() / 1000, stat))
        .toList();
  }

  private StatsComputer getStatsComputer(String statsComputerName) {
    StatsComputer computer = InstanceLoaderUtils.createStatsComputerInstance(statsComputerName);
    computer.initialize(optimizerEnv);
    return computer;
  }

  private StatsUpdater loadStatsUpdater(OptimizerConfig config) {
    String statsUpdaterName = config.get(OptimizerConfig.STATS_UPDATER_CONFIG);
    return ProviderUtils.createStatsUpdaterInstance(statsUpdaterName);
  }

  private MetricsUpdater loadMetricsUpdater(OptimizerConfig config) {
    String metricsUpdaterName = config.get(OptimizerConfig.METRICS_UPDATER_CONFIG);
    return ProviderUtils.createMetricsUpdaterInstance(metricsUpdaterName);
  }
}
