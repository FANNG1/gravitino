package org.apache.gravitino.common;

import org.apache.gravitino.monitor.api.SingleMetric;
import org.apache.gravitino.updater.api.SingleStatistic;

public class SingleMetricImpl implements SingleMetric {
  private long timestamp;
  private SingleStatistic<?> statistic;

  public SingleMetricImpl(long timestamp, SingleStatistic<?> statistic) {
    this.timestamp = timestamp;
    this.statistic = statistic;
  }

  @Override
  public long timestamp() {
    return timestamp;
  }

  @Override
  public SingleStatistic<?> statistic() {
    return statistic;
  }

  @Override
  public String toString() {
    return "BaseMetric{" + "timestamp=" + timestamp + ", statistic=" + statistic + '}';
  }
}
