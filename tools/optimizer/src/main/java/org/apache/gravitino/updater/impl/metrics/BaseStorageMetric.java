package org.apache.gravitino.updater.impl.metrics;

public class BaseStorageMetric implements StorageMetric {
  private long timestamp;
  private String value;

  public BaseStorageMetric(long timestamp, String value) {
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
