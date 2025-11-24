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

package org.apache.gravitino.optimizer.monitor.metrics;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.optimizer.api.common.MetricsPoint;
import org.apache.gravitino.optimizer.api.common.PartitionEntry;
import org.apache.gravitino.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.optimizer.api.monitor.MetricsProvider;
import org.apache.gravitino.optimizer.common.MetricPointImpl;
import org.apache.gravitino.optimizer.common.OptimizerEnv;
import org.apache.gravitino.optimizer.common.util.StatisticValueUtils;
import org.apache.gravitino.optimizer.updater.PartitionStatisticEntryImpl;
import org.apache.gravitino.optimizer.updater.StatisticEntryImpl;
import org.apache.gravitino.optimizer.updater.metrics.storage.H2MetricsStorage;
import org.apache.gravitino.optimizer.updater.metrics.storage.MetricsStorage;
import org.apache.gravitino.optimizer.updater.metrics.storage.StorageMetric;
import org.apache.gravitino.optimizer.updater.util.PartitionUtils;

public class GravitinoMetricsProvider implements MetricsProvider {

  public static final String GRAVITINO_METRICS_PROVIDER_NAME = "gravitino-metrics-provider";
  private MetricsStorage metricsStorage;

  @Override
  public String name() {
    return GRAVITINO_METRICS_PROVIDER_NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    this.metricsStorage = new H2MetricsStorage();
    metricsStorage.initialize(optimizerEnv.config().getAllConfig());
  }

  @Override
  public Map<String, List<MetricsPoint>> listJobMetrics(
      NameIdentifier jobIdentifier, long startTime, long endTime) {
    Map<String, List<StorageMetric>> metrics =
        metricsStorage.getJobMetrics(jobIdentifier, startTime, endTime);

    return toSingeMetrics(metrics, Optional.empty());
  }

  @Override
  public Map<String, List<MetricsPoint>> listTableMetrics(
      NameIdentifier tableIdentifier,
      Optional<List<PartitionEntry>> partitionName,
      long startTime,
      long endTime) {
    Map<String, List<StorageMetric>> metrics =
        metricsStorage.getTableMetrics(
            tableIdentifier,
            partitionName.map(PartitionUtils::getGravitinoPartitionName),
            startTime,
            endTime);

    return toSingeMetrics(metrics, partitionName);
  }

  private Map<String, List<MetricsPoint>> toSingeMetrics(
      Map<String, List<StorageMetric>> metrics, Optional<List<PartitionEntry>> partitions) {
    return metrics.entrySet().stream()
        .collect(
            Collectors.toMap(
                entry -> entry.getKey(),
                entry ->
                    entry.getValue().stream()
                        .map(
                            storageMetric ->
                                toSingleMetric(storageMetric, partitions, entry.getKey()))
                        .collect(Collectors.toList())));
  }

  private MetricsPoint toSingleMetric(
      StorageMetric metric, Optional<List<PartitionEntry>> partitions, String metricName) {
    return new MetricPointImpl(
        metric.getTimestamp(), toSingleStatistic(metric, partitions, metricName));
  }

  private StatisticEntry toSingleStatistic(
      StorageMetric metric, Optional<List<PartitionEntry>> partitions, String metricName) {
    if (partitions.isPresent()) {
      List<PartitionEntry> p = partitions.get();
      return new PartitionStatisticEntryImpl(
          metricName, StatisticValueUtils.fromString(metric.getValue()), p);
    }
    return new StatisticEntryImpl<>(metricName, StatisticValueUtils.fromString(metric.getValue()));
  }
}
