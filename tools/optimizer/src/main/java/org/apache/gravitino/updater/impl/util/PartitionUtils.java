package org.apache.gravitino.updater.impl.util;

import org.apache.gravitino.common.SinglePartition;

public class PartitionUtils {
  public static String getGravitinoPartitionName(SinglePartition partition) {
    return partition.partitionName().replace("=", "_") + "=" + partition.partitionValue();
  }
}
