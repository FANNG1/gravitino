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
import org.apache.gravitino.optimizer.api.common.SingleMetric;
import org.apache.gravitino.optimizer.api.common.SingleStatistic;
import org.apache.gravitino.optimizer.api.updater.MetricsUpdater;
import org.apache.gravitino.optimizer.api.updater.StatsComputer;
import org.apache.gravitino.optimizer.api.updater.StatsUpdater;
import org.apache.gravitino.optimizer.api.updater.SupportJobStats;
import org.apache.gravitino.optimizer.api.updater.SupportTableStats;
import org.apache.gravitino.optimizer.common.OptimizerEnv;
import org.apache.gravitino.optimizer.common.SingleMetricImpl;
import org.apache.gravitino.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.optimizer.common.util.InstanceLoaderUtils;
import org.apache.gravitino.optimizer.common.util.ProviderUtils;

public class Updater {
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
      if (computer instanceof SupportTableStats) {
        SupportTableStats supportTableStats = ((SupportTableStats) computer);
        List<SingleStatistic<?>> statistics = supportTableStats.computeTableStats(nameIdentifier);
        updateTable(statistics, nameIdentifier, updateType);
      }
      if (computer instanceof SupportJobStats && updateType.equals(UpdateType.METRICS)) {
        SupportJobStats supportJobStats = ((SupportJobStats) computer);
        List<SingleStatistic<?>> statistics = supportJobStats.computeJobStats(nameIdentifier);
        updateJob(statistics, nameIdentifier);
      }
    }
  }

  @VisibleForTesting
  public MetricsUpdater getMetricsUpdater() {
    return metricsUpdater;
  }

  private void updateTable(
      List<SingleStatistic<?>> statistics, NameIdentifier tableIdentifier, UpdateType updateType) {
    switch (updateType) {
      case STATS:
        statsUpdater.updateTableStatistics(tableIdentifier, statistics);
        break;
      case METRICS:
        metricsUpdater.updateTableMetrics(tableIdentifier, toMetrics(statistics));
        break;
    }
  }

  private void updateJob(List<SingleStatistic<?>> statistics, NameIdentifier jobIdentifier) {
    metricsUpdater.updateJobMetrics(jobIdentifier, toMetrics(statistics));
  }

  private List<SingleMetric> toMetrics(List<SingleStatistic<?>> statistics) {
    return statistics.stream()
        .map(stat -> (SingleMetric) new SingleMetricImpl(System.currentTimeMillis(), stat))
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
