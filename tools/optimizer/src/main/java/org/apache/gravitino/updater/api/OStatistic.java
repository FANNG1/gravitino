package org.apache.gravitino.updater.api;

import org.apache.gravitino.stats.StatisticValue;

public interface OStatistic<T> {

  String name();

  StatisticValue<T> value();
}
