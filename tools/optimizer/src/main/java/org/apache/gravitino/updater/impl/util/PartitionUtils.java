package org.apache.gravitino.updater.impl.util;

import java.util.List;
import org.apache.gravitino.common.SinglePartition;

public class PartitionUtils {
  public static String getGravitinoPartitionName(List<SinglePartition> partitions) {
    // Support multi partitions by joining with "/"
    StringBuilder sb = new StringBuilder();
    for (SinglePartition partition : partitions) {
      sb.append(partition.partitionName().replace("=", "_") + "=" + partition.partitionValue());
      sb.append("/");
    }
    return sb.toString();
  }
}
