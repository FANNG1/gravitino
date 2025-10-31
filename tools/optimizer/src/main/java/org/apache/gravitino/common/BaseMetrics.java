package org.apache.gravitino.common;

import java.util.List;
import org.apache.gravitino.monitor.api.Metrics;
import org.apache.gravitino.updater.api.BaseStatistic;

public class BaseMetrics implements Metrics {
  private long timestamp;
  private List<BaseStatistic<?>> statistics;

  public BaseMetrics(long timestamp, List<BaseStatistic<?>> statistics) {
    this.timestamp = timestamp;
    this.statistics = statistics;
  }

  @Override
  public long timestamp() {
    return timestamp;
  }

  @Override
  public List<BaseStatistic<?>> statistics() {
    return statistics;
  }
}
