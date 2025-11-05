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

package org.apache.gravitino.recommender.impl.actor;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.ToString;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.recommender.api.PolicyActor.JobExecuteContext;
import org.apache.gravitino.rel.Table;
import org.apache.iceberg.actions.RewriteDataFiles;

@ToString
public class CompactionJobContext implements JobExecuteContext {
  private NameIdentifier name;
  private Map<String, Object> config;
  private Policy policy;
  private Table tableMetadata;

  public CompactionJobContext(
      NameIdentifier name, Map<String, Object> config, Policy policy, Table tableMetadata) {
    this.policy = policy;
    this.name = name;
    this.config = config;
    this.tableMetadata = tableMetadata;
  }

  @Override
  public NameIdentifier name() {
    return name;
  }

  @Override
  public Map<String, Object> config() {
    return config;
  }

  @Override
  public Policy policy() {
    return policy;
  }

  public Optional<Long> targetFileSize() {
    return Optional.ofNullable((long) config.get(RewriteDataFiles.TARGET_FILE_SIZE_BYTES));
  }

  public Optional<List<String>> partitionNames() {
    return Optional.empty();
  }
}
