package org.apache.gravitino.common;

import org.apache.gravitino.monitor.api.SingleMetric;
import org.apache.gravitino.updater.api.BaseStatistic;

public class BaseMetric implements SingleMetric {
  private long timestamp;
  private BaseStatistic<?> statistic;

  public BaseMetric(long timestamp, BaseStatistic<?> statistic) {
    this.timestamp = timestamp;
    this.statistic = statistic;
  }

  @Override
  public long timestamp() {
    return timestamp;
  }

  @Override
  public BaseStatistic<?> statistic() {
    return statistic;
  }

  @Override
  public String toString() {
    return "BaseMetric{" + "timestamp=" + timestamp + ", statistic=" + statistic + '}';
  }
}
