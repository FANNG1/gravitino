package org.apache.gravitino.monitor.api;

import org.apache.gravitino.updater.api.SingleStatistic;

// A single metric data point with a timestamp and associated statistic.
public interface SingleMetric {

  long timestamp();

  SingleStatistic<?> statistic();
}
