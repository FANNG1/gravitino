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

package org.apache.gravitino.optimizer.api.monitor;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.optimizer.api.common.MetricsPoint;
import org.apache.gravitino.optimizer.api.common.PartitionEntry;
import org.apache.gravitino.optimizer.api.common.Provider;

/** Represents a provider that provides table and job related metrics. */
@DeveloperApi
public interface MetricsProvider extends Provider {
  /**
   * Retrieves metrics for a specific job within a specified time range.
   *
   * @param jobIdentifier The identifier of the job to retrieve metrics for
   * @param startTime The start timestamp (in seconds) of the time range to consider when fetching
   *     metrics
   * @param endTime The end timestamp (in seconds) of the time range to consider when fetching
   *     metrics
   * @return A list of {@link MetricsPoint} objects containing the job metrics for the specified
   *     criteria
   */
  Map<String, List<MetricsPoint>> listJobMetrics(
      NameIdentifier jobIdentifier, long startTime, long endTime);

  /**
   * Retrieve metrics for a table, optionally scoped to specific partitions.
   *
   * @param tableIdentifier catalog/schema/table identifier
   * @param partitionName when present, limit metrics to these partitions
   * @param startTime start timestamp (seconds)
   * @param endTime end timestamp (seconds)
   * @return map keyed by metric name, each containing a time-ordered series
   */
  Map<String, List<MetricsPoint>> listTableMetrics(
      NameIdentifier tableIdentifier,
      Optional<List<PartitionEntry>> partitionName,
      long startTime,
      long endTime);
}
