package org.apache.gravitino.updater.impl;

import java.util.List;
import org.apache.gravitino.common.SinglePartition;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.updater.api.PartitionStatistic;

public class PartitionStatisticImpl extends SingleStatisticImpl implements PartitionStatistic {
  private List<SinglePartition> partitions;

  public PartitionStatisticImpl(
      String name, StatisticValue value, List<SinglePartition> partitions) {
    super(name, value);
    this.partitions = partitions;
  }

  @Override
  public List<SinglePartition> partitions() {
    return partitions;
  }
}
