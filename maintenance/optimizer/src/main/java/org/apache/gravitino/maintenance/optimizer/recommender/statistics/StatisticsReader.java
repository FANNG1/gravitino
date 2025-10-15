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

package org.apache.gravitino.maintenance.optimizer.recommender.statistics;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;

public interface StatisticsReader {
  List<StatisticEntry<?>> readTableStatistics(NameIdentifier tableIdentifier);

  Map<NameIdentifier, List<StatisticEntry<?>>> readAllTableStatistics();

  List<StatisticEntry<?>> readJobStatistics(NameIdentifier jobIdentifier);

  Map<NameIdentifier, List<StatisticEntry<?>>> readAllJobStatistics();

  /**
   * Read partition-level statistics for a given table.
   *
   * @param tableIdentifier table identifier
   * @return map keyed by partition path with statistics for that partition
   */
  Map<PartitionPath, List<StatisticEntry<?>>> readPartitionStatistics(
      NameIdentifier tableIdentifier);

  /**
   * Read partition-level statistics for all tables.
   *
   * @return map keyed by table identifier then partition path
   */
  Map<NameIdentifier, Map<PartitionPath, List<StatisticEntry<?>>>> readAllPartitionStatistics();
}
