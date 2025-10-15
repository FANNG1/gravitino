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

package org.apache.gravitino.maintenance.optimizer.api.monitor;

import java.util.Objects;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;

/** Scope of metrics being evaluated (table, partition, or job). */
public final class MetricScope {
  public enum Type {
    TABLE,
    PARTITION,
    JOB
  }

  private final NameIdentifier identifier;
  private final Type type;
  private final Optional<PartitionPath> partition;

  private MetricScope(NameIdentifier identifier, Type type, Optional<PartitionPath> partition) {
    this.identifier = Objects.requireNonNull(identifier, "identifier must not be null");
    this.type = Objects.requireNonNull(type, "type must not be null");
    this.partition = Objects.requireNonNull(partition, "partition must not be null");
  }

  public static MetricScope forTable(NameIdentifier identifier) {
    return new MetricScope(identifier, Type.TABLE, Optional.empty());
  }

  public static MetricScope forPartition(NameIdentifier identifier, PartitionPath partition) {
    return new MetricScope(identifier, Type.PARTITION, Optional.of(partition));
  }

  public static MetricScope forJob(NameIdentifier identifier) {
    return new MetricScope(identifier, Type.JOB, Optional.empty());
  }

  public NameIdentifier identifier() {
    return identifier;
  }

  public Type type() {
    return type;
  }

  public Optional<PartitionPath> partition() {
    return partition;
  }
}
