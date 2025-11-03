package org.apache.gravitino.monitor.api;

import org.apache.gravitino.updater.api.BaseStatistic;

public interface SingleMetric {

  long timestamp();

  BaseStatistic<?> statistic();
}
