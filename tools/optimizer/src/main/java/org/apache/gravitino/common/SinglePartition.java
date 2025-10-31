package org.apache.gravitino.common;

public interface SinglePartition {
  String partitionName();

  String partitionValue();
}
