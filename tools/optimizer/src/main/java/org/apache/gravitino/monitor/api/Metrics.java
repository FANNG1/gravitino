package org.apache.gravitino.monitor.api;

public interface Metrics {
  enum MetricName {
    TABLE_STORAGE_COST,
    DATA_FILE_SIZE,
    DATA_FILE_NUMBER,
    POSITION_FILE_NUMBER,
    JOB_COST,
    JOB_DURATION,
  }

  String name();

  Metric[] metrics();
}
