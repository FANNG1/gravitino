package org.apache.gravitino.monitor.impl;

import java.util.List;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.common.SinglePartition;
import org.apache.gravitino.monitor.api.Metrics;
import org.apache.gravitino.monitor.api.MetricsProvider;
import org.apache.gravitino.updater.impl.metrics.MetricsStorage;
import org.apache.gravitino.updater.impl.util.PartitionUtils;

public class GravitinoMetricsProvider implements MetricsProvider {

  private MetricsStorage metricsStorage;

  @Override
  public List<Metrics> jobMetricDetails(
      NameIdentifier jobIdentifier, long startTime, long endTime) {
    return null;
  }

  @Override
  public List<Metrics> tableMetricDetails(
      NameIdentifier tableIdentifier,
      Optional<SinglePartition> partition,
      long startTime,
      long endTime) {
    metricsStorage.getTableMetrics(
        tableIdentifier,
        partition.map(PartitionUtils::getGravitinoPartitionName),
        startTime,
        endTime);
    return null;
    /*
    return metricValues.stream()
        .map(metric -> new BaseMetrics(metric.getTimestamp(), metric.getValue()))
        .collect(Collectors.toList());
     */
  }
}
