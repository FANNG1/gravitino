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
import org.apache.gravitino.maintenance.optimizer.api.common.Strategy;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.recommender.util.PartitionUtils;
import org.apache.gravitino.rel.Table;
import org.apache.iceberg.actions.RewriteDataFiles;

@ToString
public class CompactionJobContext implements JobExecutionContext {
  private static final String TARGET_FILE_BYTES = "target-file-size-bytes";
  private final NameIdentifier name;
  private final Map<String, String> config;
  @Getter
  private final Strategy strategy;
  @Getter
  private final Table tableMetadata;
  @Getter
  private final List<PartitionPath> partitions;

  public CompactionJobContext(
      NameIdentifier name, Map<String, String> config, Strategy strategy, Table tableMetadata) {
    this(name, config, strategy, tableMetadata, List.of());
  }

  public CompactionJobContext(
      NameIdentifier name,
      Map<String, String> config,
      Strategy strategy,
      Table tableMetadata,
      List<PartitionPath> partitions) {
    this.strategy = strategy;
    this.name = name;
    this.config = config;
    this.tableMetadata = tableMetadata;
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
    return strategy.jobTemplateName();
  }

  public Optional<Long> targetFileSize() {
    if (!config.containsKey(TARGET_FILE_BYTES)) {
      return Optional.empty();
    }
    try {
      return Optional.of(Long.parseLong(config.get(TARGET_FILE_BYTES).trim()));
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
