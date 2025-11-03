package org.apache.gravitino.updater.impl.metrics;

import java.util.List;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;

public interface MetricsStorage {

  void storeTableMetrics(
      NameIdentifier nameIdentifier,
      String metricName,
      Optional<String> partition,
      StorageMetric metric);

  List<StorageMetric> getTableMetrics(
      NameIdentifier nameIdentifier,
      Optional<String> partition,
      long fromTimestamp,
      long toTimestamp);

  void storeJobMetrics(
      NameIdentifier nameIdentifier, String metricName, StorageMetric metric);

  List<StorageMetric> getJobMetrics(
      NameIdentifier nameIdentifier, long fromTimestamp, long toTimestamp);
}
