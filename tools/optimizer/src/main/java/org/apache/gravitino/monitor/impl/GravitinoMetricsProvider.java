package org.apache.gravitino.monitor.impl;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.common.BaseMetric;
import org.apache.gravitino.common.SinglePartition;
import org.apache.gravitino.monitor.api.MetricsProvider;
import org.apache.gravitino.monitor.api.SingleMetric;
import org.apache.gravitino.updater.api.BaseStatistic;
import org.apache.gravitino.updater.impl.SimplePartitionStatistic;
import org.apache.gravitino.updater.impl.SimpleStatistic;
import org.apache.gravitino.updater.impl.metrics.MetricsStorage;
import org.apache.gravitino.updater.impl.metrics.StorageMetric;
import org.apache.gravitino.updater.impl.util.PartitionUtils;
import org.apache.gravitino.util.StatisticValueUtils;

public class GravitinoMetricsProvider implements MetricsProvider {

  private MetricsStorage metricsStorage;

  @Override
  public List<SingleMetric> jobMetricDetails(
      NameIdentifier jobIdentifier, long startTime, long endTime) {
    return null;
  }

  @Override
  public Map<String, List<SingleMetric>> tableMetricDetails(
      NameIdentifier tableIdentifier,
      Optional<SinglePartition> partition,
      long startTime,
      long endTime) {
    Map<String, List<StorageMetric>> metrics =
        metricsStorage.getAllTableMetrics(
            tableIdentifier,
            partition.map(PartitionUtils::getGravitinoPartitionName),
            startTime,
            endTime);

    return metrics.entrySet().stream()
        .collect(
            Collectors.toMap(
                entry -> entry.getKey(),
                entry ->
                    entry.getValue().stream()
                        .map(
                            storageMetric -> toBaseMetric(storageMetric, partition, entry.getKey()))
                        .collect(Collectors.toList())));
  }

  private BaseMetric toBaseMetric(
      StorageMetric metric, Optional<SinglePartition> partition, String metricName) {
    return new BaseMetric(metric.getTimestamp(), toBaseStatistic(metric, partition, metricName));
  }

  private BaseStatistic toBaseStatistic(
      StorageMetric metric, Optional<SinglePartition> partition, String metricName) {
    if (partition.isPresent()) {
      SinglePartition p = partition.get();
      return new SimplePartitionStatistic(
          metricName, StatisticValueUtils.fromString(metric.getValue()), p);
    }
    return new SimpleStatistic<>(metricName, StatisticValueUtils.fromString(metric.getValue()));
  }
}
