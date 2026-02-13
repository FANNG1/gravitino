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

package org.apache.gravitino.maintenance.optimizer.integration.test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricSample;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionMetricSample;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.monitor.metrics.GravitinoMetricsProvider;
import org.apache.gravitino.maintenance.optimizer.recommender.statistics.GravitinoStatisticsProvider;
import org.apache.gravitino.maintenance.optimizer.recommender.util.PartitionUtils;
import org.apache.gravitino.maintenance.optimizer.updater.UpdateType;
import org.apache.gravitino.maintenance.optimizer.updater.Updater;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.GravitinoMetricsUpdater;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@SuppressWarnings("UnusedVariable")
public class UpdaterIT extends GravitinoOptimizerEnvIT {

  private Updater updater;
  private GravitinoStatisticsProvider statisticsProvider;
  private GravitinoMetricsProvider metricsProvider;

  @Override
  protected Map<String, String> getSpecifyConfigs() {
    return Map.of();
  }

  @BeforeAll
  void init() {
    this.updater = new Updater(optimizerEnv);
    this.statisticsProvider = new GravitinoStatisticsProvider();
    statisticsProvider.initialize(optimizerEnv);
    this.metricsProvider = new GravitinoMetricsProvider();
    metricsProvider.initialize(optimizerEnv);

    GravitinoMetricsUpdater metricsUpdater = (GravitinoMetricsUpdater) updater.getMetricsUpdater();
    metricsUpdater.cleanupJobMetricsBefore(Long.MAX_VALUE);
    metricsUpdater.cleanupTableMetricsBefore(Long.MAX_VALUE);
  }

  @Test
  void testUpdateTableStatistics() {
    String tableName = "update-stats";
    createTable(tableName);
    NameIdentifier tableIdentifier = getTableIdentifier(tableName);
    updater.update(
        DummyTableStatisticsComputer.DUMMY_TABLE_STAT,
        Arrays.asList(tableIdentifier),
        UpdateType.STATISTICS);

    List<StatisticEntry<?>> tableStats = statisticsProvider.tableStatistics(tableIdentifier);
    Assertions.assertEquals(1, tableStats.size());
    Assertions.assertEquals(DummyTableStatisticsComputer.TABLE_STAT_NAME, tableStats.get(0).name());
    Assertions.assertEquals(1L, tableStats.get(0).value().value());

    Map<PartitionPath, List<StatisticEntry<?>>> partitionStats =
        statisticsProvider.partitionStatistics(tableIdentifier);
    Assertions.assertEquals(1, partitionStats.size());
    List<StatisticEntry<?>> partitionEntries =
        partitionStats.values().stream().findFirst().orElseThrow(IllegalStateException::new);
    Assertions.assertEquals(1, partitionEntries.size());
    Assertions.assertEquals(
        DummyTableStatisticsComputer.TABLE_STAT_NAME, partitionEntries.get(0).name());
    Assertions.assertEquals(2L, partitionEntries.get(0).value().value());
    Assertions.assertEquals(
        PartitionUtils.encodePartitionPath(
            PartitionPath.of(DummyTableStatisticsComputer.getPartitionName())),
        PartitionUtils.encodePartitionPath(
            partitionStats.keySet().stream().findFirst().orElseThrow(IllegalStateException::new)));
  }

  @Test
  void testUpdateTableMetrics() {
    String tableName = "update-metrics";
    createTable(tableName);
    NameIdentifier tableIdentifier = getTableIdentifier(tableName);
    updater.update(
        DummyTableStatisticsComputer.DUMMY_TABLE_STAT,
        Arrays.asList(tableIdentifier),
        UpdateType.METRICS);

    Map<String, List<MetricSample>> tableMetrics =
        metricsProvider.tableMetrics(tableIdentifier, 0, Long.MAX_VALUE);
    Assertions.assertEquals(1, tableMetrics.size());
    Assertions.assertTrue(tableMetrics.containsKey(DummyTableStatisticsComputer.TABLE_STAT_NAME));
    List<MetricSample> tableMetricsList =
        tableMetrics.get(DummyTableStatisticsComputer.TABLE_STAT_NAME);
    Assertions.assertEquals(1, tableMetricsList.size());
    sleep(2);
    long diff = System.currentTimeMillis() / 1000 - tableMetricsList.get(0).timestamp();
    Assertions.assertTrue(diff > 0 && diff <= 10000);
    Assertions.assertEquals(1L, tableMetricsList.get(0).statistic().value().value());

    Map<String, List<MetricSample>> partitionMetrics =
        metricsProvider.partitionMetrics(
            tableIdentifier,
            PartitionPath.of(DummyTableStatisticsComputer.getPartitionName()),
            0,
            Long.MAX_VALUE);
    Assertions.assertEquals(1, partitionMetrics.size());
    Assertions.assertTrue(
        partitionMetrics.containsKey(DummyTableStatisticsComputer.TABLE_STAT_NAME));
    List<MetricSample> partitionMetricsList =
        partitionMetrics.get(DummyTableStatisticsComputer.TABLE_STAT_NAME);
    Assertions.assertEquals(1, partitionMetricsList.size());
    sleep(2);
    diff = System.currentTimeMillis() / 1000 - partitionMetricsList.get(0).timestamp();
    Assertions.assertTrue(diff > 0 && diff <= 10000);
    Assertions.assertEquals(2L, partitionMetricsList.get(0).statistic().value().value());
    Assertions.assertTrue(partitionMetricsList.get(0) instanceof PartitionMetricSample);
    Assertions.assertEquals(
        PartitionUtils.encodePartitionPath(
            PartitionPath.of(DummyTableStatisticsComputer.getPartitionName())),
        PartitionUtils.encodePartitionPath(
            ((PartitionMetricSample) partitionMetricsList.get(0)).partition()));
  }

  @Test
  void testUpdateJobMetrics() {
    String jobName = "update-job-metrics";
    NameIdentifier jobIdentifier = NameIdentifier.of(jobName);
    updater.update(
        DummyJobMetricsComputer.DUMMY_JOB_METRICS,
        Arrays.asList(jobIdentifier),
        UpdateType.METRICS);

    Map<String, List<MetricSample>> jobMetrics =
        metricsProvider.jobMetrics(jobIdentifier, 0, Long.MAX_VALUE);
    Assertions.assertEquals(1, jobMetrics.size());
    Assertions.assertTrue(jobMetrics.containsKey(DummyJobMetricsComputer.JOB_STAT_NAME));
    List<MetricSample> jobMetricsList = jobMetrics.get(DummyJobMetricsComputer.JOB_STAT_NAME);
    Assertions.assertEquals(1, jobMetricsList.size());
    sleep(2);
    long diff = System.currentTimeMillis() / 1000 - jobMetricsList.get(0).timestamp();
    Assertions.assertTrue(diff > 0 && diff <= 10000);
    Assertions.assertEquals(1L, jobMetricsList.get(0).statistic().value().value());
  }

  private void sleep(long secs) {
    try {
      Thread.sleep(secs * 1000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
