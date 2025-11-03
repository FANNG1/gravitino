package org.apache.gravitino.updater.impl;

import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.monitor.api.Metrics;
import org.apache.gravitino.updater.api.BaseStatistic;
import org.apache.gravitino.updater.api.MetricsUpdater;
import org.apache.gravitino.updater.api.PartitionStatistic;
import org.apache.gravitino.updater.impl.metrics.BaseStorageMetric;
import org.apache.gravitino.updater.impl.metrics.MetricsStorage;
import org.apache.gravitino.updater.impl.util.PartitionUtils;
import org.apache.gravitino.util.StatisticValueUtils;

// Update metrics to h2
public class GravitinoMetricsUpdater implements MetricsUpdater {

  private final MetricsStorage metricsStorage;

  public GravitinoMetricsUpdater(MetricsStorage metricsStorage) {
    this.metricsStorage = metricsStorage;
  }

  @Override
  public void updateTableMetrics(NameIdentifier nameIdentifier, Metrics metrics) {
    metrics
        .statistics()
        .forEach(
            statistic -> {
              doUpdateTableMetrics(nameIdentifier, metrics.timestamp(), statistic);
            });
  }

  @Override
  public void updateJobMetrics(NameIdentifier nameIdentifier, Metrics metrics) {}

  private void doUpdateTableMetrics(
      NameIdentifier nameIdentifier, long timestamp, BaseStatistic statistic) {
    if (statistic instanceof PartitionStatistic) {
      PartitionStatistic partitionStatistic = (PartitionStatistic) statistic;
      metricsStorage.storeTableMetrics(
          nameIdentifier,
          statistic.name(),
          Optional.of(PartitionUtils.getGravitinoPartitionName(partitionStatistic.partition())),
          new BaseStorageMetric(timestamp, StatisticValueUtils.toString(statistic.value())));
      return;
    }
    metricsStorage.storeTableMetrics(
        nameIdentifier,
        statistic.name(),
        Optional.empty(),
        new BaseStorageMetric(timestamp, StatisticValueUtils.toString(statistic.value())));
  }
}
