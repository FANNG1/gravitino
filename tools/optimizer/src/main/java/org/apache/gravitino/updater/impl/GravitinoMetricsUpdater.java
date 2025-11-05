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

package org.apache.gravitino.updater.impl;

import java.util.List;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.monitor.api.SingleMetric;
import org.apache.gravitino.updater.api.MetricsUpdater;
import org.apache.gravitino.updater.api.PartitionStatistic;
import org.apache.gravitino.updater.api.SingleStatistic;
import org.apache.gravitino.updater.impl.metrics.MetricsStorage;
import org.apache.gravitino.updater.impl.metrics.StorageMetricImpl;
import org.apache.gravitino.updater.impl.util.PartitionUtils;
import org.apache.gravitino.util.StatisticValueUtils;

// Update metrics to h2
public class GravitinoMetricsUpdater implements MetricsUpdater {

  private final MetricsStorage metricsStorage;

  public GravitinoMetricsUpdater(MetricsStorage metricsStorage) {
    this.metricsStorage = metricsStorage;
  }

  @Override
  public void updateTableMetrics(NameIdentifier nameIdentifier, List<SingleMetric> metrics) {
    metrics.stream()
        .forEach(
            metric -> doUpdateTableMetrics(nameIdentifier, metric.timestamp(), metric.statistic()));
  }

  @Override
  public void updateJobMetrics(NameIdentifier nameIdentifier, List<SingleMetric> metrics) {
    metrics.stream()
        .forEach(
            metric -> doUpdateJobMetrics(nameIdentifier, metric.timestamp(), metric.statistic()));
  }

  private void doUpdateJobMetrics(
      NameIdentifier nameIdentifier, long timestamp, SingleStatistic statistic) {
    metricsStorage.storeJobMetrics(
        nameIdentifier,
        statistic.name(),
        new StorageMetricImpl(timestamp, StatisticValueUtils.toString(statistic.value())));
  }

  private void doUpdateTableMetrics(
      NameIdentifier nameIdentifier, long timestamp, SingleStatistic statistic) {
    if (statistic instanceof PartitionStatistic) {
      PartitionStatistic partitionStatistic = (PartitionStatistic) statistic;
      metricsStorage.storeTableMetrics(
          nameIdentifier,
          statistic.name(),
          Optional.of(PartitionUtils.getGravitinoPartitionName(partitionStatistic.partitions())),
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
