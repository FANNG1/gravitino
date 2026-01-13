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

package org.apache.gravitino.maintenance.optimizer.recommender.actor.compaction;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.ToString;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.recommender.util.PartitionUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.transforms.Transform;

@ToString
public class CompactionJobContext implements JobExecutionContext {
  static final String TARGET_FILE_SIZE_BYTES = "target-file-size-bytes";
  private final NameIdentifier name;
  private final Map<String, String> config;
  private final String jobTemplateName;
  @Getter private final Column[] columns;
  @Getter private final Transform[] partitioning;
  @Getter private final List<PartitionPath> partitions;

  public CompactionJobContext(
      NameIdentifier name,
      Map<String, String> jobOptions,
      String jobTemplateName,
      Column[] columns,
      Transform[] partitioning,
      List<PartitionPath> partitions) {
    this.name = name;
    this.config = jobOptions;
    this.jobTemplateName = jobTemplateName;
    this.columns = columns;
    this.partitioning = partitioning;
    this.partitions = partitions;
  }

  @Override
  public NameIdentifier nameIdentifier() {
    return name;
  }

  @Override
  public Map<String, String> jobConfig() {
    return config;
  }

  @Override
  public String jobTemplateName() {
    return jobTemplateName;
  }

  public Optional<Long> targetFileSize() {
    if (!config.containsKey(TARGET_FILE_SIZE_BYTES)) {
      return Optional.empty();
    }
    try {
      return Optional.of(Long.parseLong(config.get(TARGET_FILE_SIZE_BYTES).trim()));
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
  }

  public Optional<List<String>> partitionNames() {
    if (partitions == null || partitions.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(partitions.stream().map(PartitionUtils::encodePartitionPath).toList());
  }
}
