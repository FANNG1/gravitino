package org.apache.gravitino.updater.api;

import java.util.List;
import org.apache.gravitino.common.SinglePartition;

public interface PartitionStatistic extends SingleStatistic {
  List<SinglePartition> partitions();
}
