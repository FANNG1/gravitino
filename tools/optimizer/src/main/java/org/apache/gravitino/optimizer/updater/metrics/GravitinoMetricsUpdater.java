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

package org.apache.gravitino.optimizer.updater.metrics;

import java.util.List;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.optimizer.api.common.MetricsPoint;
import org.apache.gravitino.optimizer.api.common.PartitionStatisticEntry;
import org.apache.gravitino.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.optimizer.api.updater.MetricsUpdater;
import org.apache.gravitino.optimizer.common.OptimizerEnv;
import org.apache.gravitino.optimizer.common.util.StatisticValueUtils;
import org.apache.gravitino.optimizer.updater.metrics.storage.H2MetricsStorage;
import org.apache.gravitino.optimizer.updater.metrics.storage.MetricsStorage;
import org.apache.gravitino.optimizer.updater.metrics.storage.StorageMetricImpl;
import org.apache.gravitino.optimizer.updater.util.PartitionUtils;

// Update metrics to h2
public class GravitinoMetricsUpdater implements MetricsUpdater {

  public static final String GRAVITINO_METRICS_UPDATER_NAME = "gravitino-metrics-updater";

  private MetricsStorage metricsStorage;

  @Override
  public String name() {
    return GRAVITINO_METRICS_UPDATER_NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    this.metricsStorage = new H2MetricsStorage();
    metricsStorage.initialize(optimizerEnv.config().getAllConfig());
  }

  @Override
  public void updateTableMetrics(NameIdentifier nameIdentifier, List<MetricsPoint> metrics) {
    metrics.stream()
        .forEach(
            metric -> doUpdateTableMetrics(nameIdentifier, metric.timestamp(), metric.statistic()));
  }

  @Override
  public void updateJobMetrics(NameIdentifier nameIdentifier, List<MetricsPoint> metrics) {
    metrics.stream()
        .forEach(
            metric -> doUpdateJobMetrics(nameIdentifier, metric.timestamp(), metric.statistic()));
  }

  public int cleanupTableMetricsBefore(long timestamp) {
    return metricsStorage.cleanupTableMetricsBefore(timestamp);
  }

  public int cleanupJobMetricsBefore(long timestamp) {
    return metricsStorage.cleanupJobMetricsBefore(timestamp);
  }

  private void doUpdateJobMetrics(
      NameIdentifier nameIdentifier, long timestamp, StatisticEntry statistic) {
    metricsStorage.storeJobMetrics(
        nameIdentifier,
        statistic.name(),
        new StorageMetricImpl(timestamp, StatisticValueUtils.toString(statistic.value())));
  }

  private void doUpdateTableMetrics(
      NameIdentifier nameIdentifier, long timestamp, StatisticEntry statistic) {
    if (statistic instanceof PartitionStatisticEntry) {
      PartitionStatisticEntry partitionStatistic = (PartitionStatisticEntry) statistic;
      metricsStorage.storeTableMetrics(
          nameIdentifier,
          statistic.name(),
          Optional.of(PartitionUtils.getGravitinoPartitionName(partitionStatistic.partitionName())),
          new StorageMetricImpl(timestamp, StatisticValueUtils.toString(statistic.value())));
      return;
    }
    metricsStorage.storeTableMetrics(
        nameIdentifier,
        statistic.name(),
        Optional.empty(),
        new StorageMetricImpl(timestamp, StatisticValueUtils.toString(statistic.value())));
  }
}
