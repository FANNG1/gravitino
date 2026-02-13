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

package org.apache.gravitino.maintenance.optimizer.updater.metrics;

import java.util.List;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricSample;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionMetricSample;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.updater.MetricsUpdater;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.util.StatisticValueUtils;
import org.apache.gravitino.maintenance.optimizer.recommender.util.PartitionUtils;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.H2MetricsStorage;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.MetricRecordImpl;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.MetricsRepository;

/** Metrics updater that persists table/job metrics into the configured metrics repository. */
public class GravitinoMetricsUpdater implements MetricsUpdater {

  public static final String NAME = "gravitino-metrics-updater";

  private MetricsRepository metricsStorage;

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    this.metricsStorage = new H2MetricsStorage();
    metricsStorage.initialize(optimizerEnv.config().getAllConfig());
  }

  @Override
  public void updateTableMetrics(NameIdentifier nameIdentifier, List<MetricSample> metrics) {
    for (MetricSample metric : metrics) {
      doUpdateTableMetrics(nameIdentifier, metric);
    }
  }

  @Override
  public void updateJobMetrics(NameIdentifier nameIdentifier, List<MetricSample> metrics) {
    for (MetricSample metric : metrics) {
      doUpdateJobMetrics(nameIdentifier, metric.timestamp(), metric.statistic());
    }
  }

  public int cleanupTableMetricsBefore(long timestamp) {
    return metricsStorage.cleanupTableMetricsBefore(timestamp);
  }

  public int cleanupJobMetricsBefore(long timestamp) {
    return metricsStorage.cleanupJobMetricsBefore(timestamp);
  }

  private void doUpdateJobMetrics(
      NameIdentifier nameIdentifier, long timestamp, StatisticEntry<?> statistic) {
    metricsStorage.storeJobMetric(
        nameIdentifier,
        statistic.name(),
        new MetricRecordImpl(timestamp, StatisticValueUtils.toString(statistic.value())));
  }

  private void doUpdateTableMetrics(NameIdentifier nameIdentifier, MetricSample metric) {
    StatisticEntry<?> statistic = metric.statistic();
    Optional<String> partition = getPartitionName(metric);
    metricsStorage.storeTableMetric(
        nameIdentifier,
        statistic.name(),
        partition,
        new MetricRecordImpl(metric.timestamp(), StatisticValueUtils.toString(statistic.value())));
  }

  private Optional<String> getPartitionName(MetricSample metricSample) {
    return metricSample instanceof PartitionMetricSample
        ? Optional.of(
            PartitionUtils.encodePartitionPath(((PartitionMetricSample) metricSample).partition()))
        : Optional.empty();
  }

  @Override
  public void close() throws Exception {
    if (metricsStorage != null) {
      metricsStorage.close();
    }
  }
}
