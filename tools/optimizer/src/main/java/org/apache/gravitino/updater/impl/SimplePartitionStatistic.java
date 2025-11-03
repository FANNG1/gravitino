package org.apache.gravitino.updater.impl;

import org.apache.gravitino.common.SinglePartition;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.updater.api.PartitionStatistic;

public class SimplePartitionStatistic extends SimpleStatistic implements PartitionStatistic {
  private SinglePartition singlePartition;

  public SimplePartitionStatistic(String name, StatisticValue value, SinglePartition partition) {
    super(name, value);
  }

  @Override
  public SinglePartition partition() {
    return singlePartition;
  }
}
