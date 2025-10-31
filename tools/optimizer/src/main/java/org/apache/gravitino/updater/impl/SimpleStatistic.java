package org.apache.gravitino.updater.impl;

import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.updater.api.BaseStatistic;

public class SimpleStatistic<T> implements BaseStatistic<T> {
  private String name;
  private StatisticValue<T> value;

  public SimpleStatistic(String name, StatisticValue<T> value) {
    this.name = name;
    this.value = value;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public StatisticValue<T> value() {
    return value;
  }
}
