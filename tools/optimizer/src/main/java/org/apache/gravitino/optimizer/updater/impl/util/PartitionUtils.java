/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.optimizer.updater.impl.util;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.gravitino.optimizer.common.PartitionImpl;
import org.apache.gravitino.optimizer.common.SinglePartition;

public class PartitionUtils {
  public static String getGravitinoPartitionName(List<SinglePartition> partitions) {
    return partitions.stream()
        .map(
            partition ->
                partition.partitionName().replace("=", "_") + "=" + partition.partitionValue())
        .collect(Collectors.joining("/"));
  }

  public static List<SinglePartition> parseGravitinoPartitionName(String gravitinoPartitionName) {
    // Support multi partitions by splitting with "/"
    return List.of(gravitinoPartitionName.split("/")).stream()
        .map(
            partition -> {
              String[] keyValue = partition.split("=");
              return new PartitionImpl(keyValue[0], keyValue[1]);
            })
        .collect(Collectors.toList());
  }
}
