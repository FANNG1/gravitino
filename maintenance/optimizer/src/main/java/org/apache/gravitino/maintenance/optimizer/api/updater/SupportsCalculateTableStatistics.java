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

package org.apache.gravitino.maintenance.optimizer.api.updater;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;

/** Represents a provider that supports table statistics. */
@DeveloperApi
public interface SupportsCalculateTableStatistics extends StatisticsCalculator {
  /**
   * Calculate table-level statistics to be persisted.
   *
   * @param tableIdentifier catalog/schema/table identifier
   * @return list of statistics; empty when none are produced
   */
  List<StatisticEntry<?>> calculateTableStatistics(NameIdentifier tableIdentifier);

  /**
   * Calculate partition-level statistics to be persisted.
   *
   * @param tableIdentifier catalog/schema/table identifier
   * @return map keyed by partition identifiers (outer to inner) to statistic entries; empty when
   *     none are produced
   */
  default Map<PartitionPath, List<StatisticEntry<?>>> calculatePartitionStatistics(
      NameIdentifier tableIdentifier) {
    return Map.of();
  }

  /**
   * Calculate table-level statistics for all identifiers discoverable by this calculator.
   *
   * <p>Implementations should override to provide bulk computation. The default implementation
   * throws to keep existing implementations unchanged.
   *
   * @return map of table identifier to its statistics; empty when none are produced
   */
  default Map<NameIdentifier, List<StatisticEntry<?>>> calculateAllTableStatistics() {
    throw new UnsupportedOperationException("Bulk table statistics computation not supported");
  }

  default Map<NameIdentifier, Map<PartitionPath, List<StatisticEntry<?>>>>
      calculateAllPartitionStatistics() {
    throw new UnsupportedOperationException("Bulk partition statistics computation not supported");
  }
}
