package org.apache.gravitino.updater.api;

import org.apache.gravitino.common.SinglePartition;

public interface PartitionStatistic extends BaseStatistic {
  SinglePartition partition();
}
