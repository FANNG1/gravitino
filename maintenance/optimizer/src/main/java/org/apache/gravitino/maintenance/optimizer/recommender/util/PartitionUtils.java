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

package org.apache.gravitino.maintenance.optimizer.recommender.util;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;

public class PartitionUtils {
  private static final String PARTITION_ENTRY_SEPARATOR = "/";
  private static final String PARTITION_KV_SEPARATOR = "=";

  private PartitionUtils() {}

  public static String getGravitinoPartitionName(PartitionPath partitionPath) {
    return partitionPath.entries().stream()
        .map(
            partition ->
                partition.partitionName().replace(PARTITION_KV_SEPARATOR, "_")
                    + PARTITION_KV_SEPARATOR
                    + partition.partitionValue())
        .collect(Collectors.joining(PARTITION_ENTRY_SEPARATOR));
  }

  public static PartitionPath parseGravitinoPartitionName(String gravitinoPartitionName) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(gravitinoPartitionName), "gravitinoPartitionName must not be blank");
    List<PartitionEntry> entries =
        Arrays.stream(gravitinoPartitionName.split(PARTITION_ENTRY_SEPARATOR))
            .map(
                partition -> {
                  String[] keyValue = partition.split(PARTITION_KV_SEPARATOR, 2);
                  Preconditions.checkArgument(
                      keyValue.length == 2,
                      "Invalid partition entry '%s', expected key=value format",
                      partition);
                  return new PartitionEntryImpl(keyValue[0], keyValue[1]);
                })
            .collect(Collectors.toList());
    return PartitionPath.of(entries);
  }
}
