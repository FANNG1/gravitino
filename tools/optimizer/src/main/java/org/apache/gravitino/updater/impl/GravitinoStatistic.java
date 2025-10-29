package org.apache.gravitino.updater.impl;

import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.updater.api.OStatistic;

public class GravitinoStatistic implements OStatistic {
  private String name;
  private StatisticValue<?> value;

  public GravitinoStatistic(String name, StatisticValue<?> value) {
    this.name = name;
    this.value = value;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public StatisticValue<?> value() {
    return value;
  }
}
