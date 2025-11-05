package org.apache.gravitino.updater.impl.metrics;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;

public interface MetricsStorage extends AutoCloseable {

  void storeTableMetrics(
      NameIdentifier nameIdentifier,
      String metricName,
      Optional<String> partition,
      StorageMetric metric);

  Map<String, List<StorageMetric>> getAllTableMetrics(
      NameIdentifier nameIdentifier,
      Optional<String> partition,
      long fromTimestamp,
      long toTimestamp);

  void storeJobMetrics(NameIdentifier nameIdentifier, String metricName, StorageMetric metric);

  Map<String, List<StorageMetric>> getJobMetrics(
      NameIdentifier nameIdentifier, long fromTimestamp, long toTimestamp);
}
