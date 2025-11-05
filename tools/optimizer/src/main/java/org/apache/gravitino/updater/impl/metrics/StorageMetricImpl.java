package org.apache.gravitino.updater.impl.metrics;

public class StorageMetricImpl implements StorageMetric {
  private long timestamp;
  private String value;

  public StorageMetricImpl(long timestamp, String value) {
    this.timestamp = timestamp;
    this.value = value;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public String getValue() {
    return value;
  }
}
