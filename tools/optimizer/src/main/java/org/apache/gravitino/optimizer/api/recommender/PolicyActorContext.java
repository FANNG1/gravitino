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

package org.apache.gravitino.optimizer.api.recommender;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.optimizer.api.common.PartitionStatistic;
import org.apache.gravitino.optimizer.api.common.RecommenderPolicy;
import org.apache.gravitino.optimizer.api.common.SingleStatistic;
import org.apache.gravitino.rel.Table;

/**
 * Immutable context passed to a {@link PolicyActor} containing the data requested through {@link
 * PolicyActor#requiredData()}.
 */
@DeveloperApi
public final class PolicyActorContext {
  private final NameIdentifier identifier;
  private final RecommenderPolicy policy;
  private final Optional<Table> tableMetadata;
  private final List<SingleStatistic<?>> tableStatistics;
  private final List<PartitionStatistic> partitionStatistics;

  private PolicyActorContext(Builder builder) {
    this.identifier = builder.identifier;
    this.policy = builder.policy;
    this.tableMetadata = builder.tableMetadata;
    this.tableStatistics = builder.tableStatistics;
    this.partitionStatistics = builder.partitionStatistics;
  }

  public NameIdentifier identifier() {
    return identifier;
  }

  public RecommenderPolicy policy() {
    return policy;
  }

  public Optional<Table> tableMetadata() {
    return tableMetadata;
  }

  public List<SingleStatistic<?>> tableStatistics() {
    return tableStatistics;
  }

  public List<PartitionStatistic> partitionStatistics() {
    return partitionStatistics;
  }

  public static Builder builder(NameIdentifier identifier, RecommenderPolicy policy) {
    return new Builder(identifier, policy);
  }

  public static final class Builder {
    private final NameIdentifier identifier;
    private final RecommenderPolicy policy;
    private Optional<Table> tableMetadata = Optional.empty();
    private List<SingleStatistic<?>> tableStatistics = List.of();
    private List<PartitionStatistic> partitionStatistics = List.of();

    private Builder(NameIdentifier identifier, RecommenderPolicy policy) {
      this.identifier = Objects.requireNonNull(identifier, "identifier must not be null");
      this.policy = Objects.requireNonNull(policy, "policy must not be null");
    }

    public Builder withTableMetadata(Table metadata) {
      this.tableMetadata = Optional.ofNullable(metadata);
      return this;
    }

    public Builder withTableStatistics(List<SingleStatistic<?>> stats) {
      this.tableStatistics =
          Collections.unmodifiableList(
              stats == null ? List.of() : List.copyOf(stats)); // avoid accidental mutation
      return this;
    }

    public Builder withPartitionStatistics(List<PartitionStatistic> partitionStats) {
      this.partitionStatistics =
          Collections.unmodifiableList(
              partitionStats == null ? List.of() : List.copyOf(partitionStats));
      return this;
    }

    public PolicyActorContext build() {
      return new PolicyActorContext(this);
    }
  }
}
