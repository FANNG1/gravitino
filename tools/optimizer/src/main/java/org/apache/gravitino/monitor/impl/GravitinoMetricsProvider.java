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

package org.apache.gravitino.monitor.impl;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.common.SingleMetricImpl;
import org.apache.gravitino.common.SinglePartition;
import org.apache.gravitino.monitor.api.MetricsProvider;
import org.apache.gravitino.monitor.api.SingleMetric;
import org.apache.gravitino.updater.api.SingleStatistic;
import org.apache.gravitino.updater.impl.PartitionStatisticImpl;
import org.apache.gravitino.updater.impl.SingleStatisticImpl;
import org.apache.gravitino.updater.impl.metrics.MetricsStorage;
import org.apache.gravitino.updater.impl.metrics.StorageMetric;
import org.apache.gravitino.updater.impl.util.PartitionUtils;
import org.apache.gravitino.util.StatisticValueUtils;

public class GravitinoMetricsProvider implements MetricsProvider {

  private MetricsStorage metricsStorage;

  void initialize(MetricsStorage metricsStorage) {
    this.metricsStorage = metricsStorage;
  }

  @Override
  public Map<String, List<SingleMetric>> jobMetricDetails(
      NameIdentifier jobIdentifier, long startTime, long endTime) {
    Map<String, List<StorageMetric>> metrics =
        metricsStorage.getJobMetrics(jobIdentifier, startTime, endTime);

    return toSingeMetrics(metrics, Optional.empty());
  }

  @Override
  public Map<String, List<SingleMetric>> tableMetricDetails(
      NameIdentifier tableIdentifier,
      Optional<List<SinglePartition>> partitions,
      long startTime,
      long endTime) {
    Map<String, List<StorageMetric>> metrics =
        metricsStorage.getAllTableMetrics(
            tableIdentifier,
            partitions.map(PartitionUtils::getGravitinoPartitionName),
            startTime,
            endTime);

    return toSingeMetrics(metrics, partitions);
  }

  private Map<String, List<SingleMetric>> toSingeMetrics(
      Map<String, List<StorageMetric>> metrics, Optional<List<SinglePartition>> partitions) {
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

  private SingleMetric toSingleMetric(
      StorageMetric metric, Optional<List<SinglePartition>> partitions, String metricName) {
    return new SingleMetricImpl(
        metric.getTimestamp(), toSingleStatistic(metric, partitions, metricName));
  }

  private SingleStatistic toSingleStatistic(
      StorageMetric metric, Optional<List<SinglePartition>> partitions, String metricName) {
    if (partitions.isPresent()) {
      List<SinglePartition> p = partitions.get();
      return new PartitionStatisticImpl(
          metricName, StatisticValueUtils.fromString(metric.getValue()), p);
    }
    return new SingleStatisticImpl<>(metricName, StatisticValueUtils.fromString(metric.getValue()));
  }
}
