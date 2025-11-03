package org.apache.gravitino.updater.impl.metrics;

public interface StorageMetric {
  long getTimestamp();

  String getValue();
}
