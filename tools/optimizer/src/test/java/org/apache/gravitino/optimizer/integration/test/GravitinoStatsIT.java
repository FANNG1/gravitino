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

package org.apache.gravitino.optimizer.integration.test;

import java.util.Arrays;
import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.optimizer.api.common.PartitionStatistic;
import org.apache.gravitino.optimizer.api.common.SingleStatistic;
import org.apache.gravitino.optimizer.api.common.SingleStatistic.Name;
import org.apache.gravitino.optimizer.common.PartitionImpl;
import org.apache.gravitino.optimizer.recommender.stats.GravitinoStatsProvider;
import org.apache.gravitino.optimizer.updater.GravitinoStatsUpdater;
import org.apache.gravitino.optimizer.updater.PartitionStatisticImpl;
import org.apache.gravitino.optimizer.updater.SingleStatisticImpl;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class GravitinoStatsIT extends GravitinoOptimizerEnvIT {

  private static final String TEST_TABLE = "test_stats_table";
  private static final String TEST_PARTITION_TABLE = "test_stats_partition_table";
  private GravitinoStatsUpdater statsUpdater;
  private GravitinoStatsProvider statsProvider;
  private static final String STATS_PREFIX = "custom-";

  @BeforeAll
  void init() {
    this.statsUpdater = new GravitinoStatsUpdater();
    statsUpdater.initialize(optimizerEnv);
    this.statsProvider = new GravitinoStatsProvider();
    statsProvider.initialize(optimizerEnv);
    createTable(TEST_TABLE);
    createPartitionTable(TEST_PARTITION_TABLE);
  }

  @Test
  void testTableStatsUpdaterAndProvider() {
    statsUpdater.updateTableStatistics(
        NameIdentifier.of(TEST_SCHEMA, TEST_TABLE),
        Arrays.asList(
            new SingleStatisticImpl(STATS_PREFIX + "row_count", StatisticValues.longValue(1000)),
            new SingleStatisticImpl(
                STATS_PREFIX + "size_in_bytes", StatisticValues.longValue(1000000)),
            new PartitionStatisticImpl(
                STATS_PREFIX + "partition_row_count",
                StatisticValues.longValue(500),
                Arrays.asList(new PartitionImpl("col1", "1"), new PartitionImpl("col2", "2"))),
            new SingleStatisticImpl(
                STATS_PREFIX + Name.DATAFILE_SIZE_MSE.name(),
                StatisticValues.doubleValue(10000.1))));

    List<SingleStatistic<?>> stats =
        statsProvider.getTableStats(NameIdentifier.of(TEST_SCHEMA, TEST_TABLE));
    Assertions.assertEquals(3, stats.size());
    stats.stream()
        .forEach(
            stat -> {
              if (stat.name().equals(STATS_PREFIX + "row_count")) {
                Assertions.assertEquals(1000L, ((StatisticValues.LongValue) stat.value()).value());
              } else if (stat.name().equals(STATS_PREFIX + "size_in_bytes")) {
                Assertions.assertEquals(
                    1000000L, ((StatisticValues.LongValue) stat.value()).value());
              } else if (stat.name().equals(STATS_PREFIX + Name.DATAFILE_SIZE_MSE.name())) {
                Assertions.assertEquals(
                    10000.1, ((StatisticValues.DoubleValue) stat.value()).value());
              } else {
                Assertions.fail("Unexpected statistic name: " + stat.name());
              }
            });
  }

  @Test
  void testTablePartitionStatsUpdaterAndProvider() {
    statsUpdater.updateTableStatistics(
        NameIdentifier.of(TEST_SCHEMA, TEST_PARTITION_TABLE),
        Arrays.asList(
            new SingleStatisticImpl(
                STATS_PREFIX + "size_in_bytes", StatisticValues.longValue(1000000)),
            new PartitionStatisticImpl(
                STATS_PREFIX + "partition_row_count",
                StatisticValues.longValue(500),
                Arrays.asList(new PartitionImpl("col1", "1"), new PartitionImpl("col2", "2"))),
            new PartitionStatisticImpl(
                STATS_PREFIX + "partition_size_in_bytes",
                StatisticValues.longValue(500000),
                Arrays.asList(new PartitionImpl("col1", "1"), new PartitionImpl("col2", "2")))));

    List<PartitionStatistic> stats =
        statsProvider.getPartitionStats(NameIdentifier.of(TEST_SCHEMA, TEST_PARTITION_TABLE));
    Assertions.assertEquals(2, stats.size());
    stats.stream()
        .forEach(
            stat -> {
              if (stat.name().equals(STATS_PREFIX + "partition_row_count")) {
                Assertions.assertEquals(500L, ((StatisticValues.LongValue) stat.value()).value());
              } else if (stat.name().equals(STATS_PREFIX + "partition_size_in_bytes")) {
                Assertions.assertEquals(
                    500000L, ((StatisticValues.LongValue) stat.value()).value());
              } else {
                Assertions.fail("Unexpected statistic name: " + stat.name());
              }
            });
  }
}
