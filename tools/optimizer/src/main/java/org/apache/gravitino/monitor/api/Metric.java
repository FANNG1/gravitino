package org.apache.gravitino.monitor.api;

import org.apache.gravitino.stats.StatisticValue;

public interface Metric {

  long timestamp();

  /**
   * Get the value of the metric.
   *
   * @return The value of the metric.
   */
  StatisticValue value();
}
