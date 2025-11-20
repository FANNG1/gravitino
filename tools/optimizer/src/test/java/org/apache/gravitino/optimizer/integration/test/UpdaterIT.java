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

package org.apache.gravitino.optimizer.integration.test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.optimizer.api.common.PartitionStatistic;
import org.apache.gravitino.optimizer.api.common.SingleMetric;
import org.apache.gravitino.optimizer.api.common.SingleStatistic;
import org.apache.gravitino.optimizer.monitor.metrics.GravitinoMetricsProvider;
import org.apache.gravitino.optimizer.recommender.stats.GravitinoStatsProvider;
import org.apache.gravitino.optimizer.updater.UpdateType;
import org.apache.gravitino.optimizer.updater.Updater;
import org.apache.gravitino.optimizer.updater.metrics.GravitinoMetricsUpdater;
import org.apache.gravitino.optimizer.updater.util.PartitionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@SuppressWarnings("UnusedVariable")
public class UpdaterIT extends GravitinoOptimizerEnvIT {

  private Updater updater;
  private GravitinoStatsProvider statsProvider;
  private GravitinoMetricsProvider metricsProvider;

  @Override
  protected Map<String, String> getSpecifyConfigs() {
    return Map.of();
  }

  @BeforeAll
  void init() {
    this.updater = new Updater(optimizerEnv);
    this.statsProvider = new GravitinoStatsProvider();
    statsProvider.initialize(optimizerEnv);
    this.metricsProvider = new GravitinoMetricsProvider();
    metricsProvider.initialize(optimizerEnv);

    GravitinoMetricsUpdater metricsUpdater = (GravitinoMetricsUpdater) updater.getMetricsUpdater();
    metricsUpdater.cleanupJobMetricsBefore(Long.MAX_VALUE);
    metricsUpdater.cleanupTableMetricsBefore(Long.MAX_VALUE);
  }

  @Test
  void testUpdateTableStats() {
    String tableName = "update-stats";
    createTable(tableName);
    NameIdentifier tableIdentifier = getTableIdentifier(tableName);
    updater.update(
        DummyTableStatsComputer.DUMMY_TABLE_STAT, Arrays.asList(tableIdentifier), UpdateType.STATS);

    List<SingleStatistic> tableStats = statsProvider.getTableStats(tableIdentifier);
    Assertions.assertEquals(1, tableStats.size());
    Assertions.assertEquals(DummyTableStatsComputer.TABLE_STAT_NAME, tableStats.get(0).name());
    Assertions.assertEquals(1L, tableStats.get(0).value().value());

    List<PartitionStatistic> partitionStats = statsProvider.getPartitionStats(tableIdentifier);
    Assertions.assertEquals(1, partitionStats.size());
    Assertions.assertEquals(DummyTableStatsComputer.TABLE_STAT_NAME, partitionStats.get(0).name());
    Assertions.assertEquals(2L, partitionStats.get(0).value().value());
    Assertions.assertEquals(
        PartitionUtils.getGravitinoPartitionName(DummyTableStatsComputer.getPartitionName()),
        PartitionUtils.getGravitinoPartitionName(partitionStats.get(0).partitionName()));
  }

  @Test
  void testUpdateTableMetrics() {
    String tableName = "update-metrics";
    createTable(tableName);
    NameIdentifier tableIdentifier = getTableIdentifier(tableName);
    updater.update(
        DummyTableStatsComputer.DUMMY_TABLE_STAT,
        Arrays.asList(tableIdentifier),
        UpdateType.METRICS);

    Map<String, List<SingleMetric>> tableMetrics =
        metricsProvider.listTableMetrics(tableIdentifier, Optional.empty(), 0, Long.MAX_VALUE);
    Assertions.assertEquals(1, tableMetrics.size());
    Assertions.assertTrue(tableMetrics.containsKey(DummyTableStatsComputer.TABLE_STAT_NAME));
    List<SingleMetric> tableMetricsList = tableMetrics.get(DummyTableStatsComputer.TABLE_STAT_NAME);
    Assertions.assertEquals(1, tableMetricsList.size());
    long diff = System.currentTimeMillis() - tableMetricsList.get(0).timestamp();
    Assertions.assertTrue(diff > 0 && diff <= 10000);
    Assertions.assertEquals(1L, tableMetricsList.get(0).statistic().value().value());

    Map<String, List<SingleMetric>> partitionMetrics =
        metricsProvider.listTableMetrics(
            tableIdentifier,
            Optional.of(DummyTableStatsComputer.getPartitionName()),
            0,
            Long.MAX_VALUE);
    Assertions.assertEquals(1, partitionMetrics.size());
    Assertions.assertTrue(partitionMetrics.containsKey(DummyTableStatsComputer.TABLE_STAT_NAME));
    List<SingleMetric> partitionMetricsList =
        partitionMetrics.get(DummyTableStatsComputer.TABLE_STAT_NAME);
    Assertions.assertEquals(1, partitionMetricsList.size());
    diff = System.currentTimeMillis() - partitionMetricsList.get(0).timestamp();
    Assertions.assertTrue(diff > 0 && diff <= 10000);
    Assertions.assertEquals(2L, partitionMetricsList.get(0).statistic().value().value());
    Assertions.assertEquals(
        PartitionUtils.getGravitinoPartitionName(DummyTableStatsComputer.getPartitionName()),
        PartitionUtils.getGravitinoPartitionName(
            ((PartitionStatistic) partitionMetricsList.get(0).statistic()).partitionName()));
  }

  @Test
  void testUpdateJobMetrics() {
    String jobName = "update-job-metrics";
    NameIdentifier jobIdentifier = NameIdentifier.of(jobName);
    updater.update(
        DummyJobMetricsComputer.DUMMY_JOB_METRICS,
        Arrays.asList(jobIdentifier),
        UpdateType.METRICS);

    Map<String, List<SingleMetric>> jobMetrics =
        metricsProvider.listJobMetrics(jobIdentifier, 0, Long.MAX_VALUE);
    Assertions.assertEquals(1, jobMetrics.size());
    Assertions.assertTrue(jobMetrics.containsKey(DummyJobMetricsComputer.JOB_STAT_NAME));
    List<SingleMetric> jobMetricsList = jobMetrics.get(DummyJobMetricsComputer.JOB_STAT_NAME);
    Assertions.assertEquals(1, jobMetricsList.size());
    long diff = System.currentTimeMillis() - jobMetricsList.get(0).timestamp();
    Assertions.assertTrue(diff > 0 && diff <= 10000);
    Assertions.assertEquals(1L, jobMetricsList.get(0).statistic().value().value());
  }
}
