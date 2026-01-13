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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.Strategy;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyHandlerContext;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.StatisticEntryImpl;
import org.apache.gravitino.maintenance.optimizer.recommender.actor.BaseExpressionStrategyHandler;
import org.apache.gravitino.maintenance.optimizer.recommender.util.ExpressionEvaluator;
import org.apache.gravitino.maintenance.optimizer.recommender.util.QLExpressionEvaluator;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

class TestCompactionStrategyHandler {

  private final Strategy strategy = new CompactionStrategyForTest();

  private final ExpressionEvaluator evaluator = new QLExpressionEvaluator();

  @Test
  void testShouldTriggerAction() {
    List<StatisticEntry<?>> stats =
        Arrays.asList(new StatisticEntryImpl("datafile_mse", StatisticValues.longValue(2000L)));
    Assertions.assertEquals(
        true, BaseExpressionStrategyHandler.shouldTriggerAction(strategy, stats, evaluator));

    stats = Arrays.asList(new StatisticEntryImpl("datafile_mse", StatisticValues.longValue(10L)));
    Assertions.assertEquals(
        false, BaseExpressionStrategyHandler.shouldTriggerAction(strategy, stats, evaluator));

    // todo: handle not exists statistic
    // CompactionPolicyHandler.shouldTriggerCompaction(policy, Arrays.asList(), evaluator);
  }

  @Test
  void testShouldTriggerActionWithPartitions() {
    NameIdentifier tableId = NameIdentifier.of("db", "table");
    Table tableMetadata = Mockito.mock(Table.class);
    Mockito.when(tableMetadata.partitioning())
        .thenReturn(
            new org.apache.gravitino.rel.expressions.transforms.Transform[] {
              Transforms.identity("p")
            });

    Map<PartitionPath, List<StatisticEntry<?>>> partitionStats =
        Map.of(
            PartitionPath.of(Arrays.asList(new PartitionEntryImpl("p", "1"))),
            List.of(new StatisticEntryImpl("datafile_mse", StatisticValues.longValue(10L))),
            PartitionPath.of(Arrays.asList(new PartitionEntryImpl("p", "2"))),
            List.of(new StatisticEntryImpl("datafile_mse", StatisticValues.longValue(2000L))));

    StrategyHandlerContext context =
        StrategyHandlerContext.builder(tableId, strategy)
            .withTableMetadata(tableMetadata)
            .withTableStatistics(Arrays.asList())
            .withPartitionStatistics(partitionStats)
            .build();

    CompactionStrategyHandler handler = new CompactionStrategyHandler();
    handler.initialize(context);
    Assertions.assertTrue(handler.shouldTrigger());

    Map<PartitionPath, List<StatisticEntry<?>>> allLowStats =
        Map.of(
            PartitionPath.of(Arrays.asList(new PartitionEntryImpl("p", "1"))),
            List.of(new StatisticEntryImpl("datafile_mse", StatisticValues.longValue(1L))),
            PartitionPath.of(Arrays.asList(new PartitionEntryImpl("p", "2"))),
            List.of(new StatisticEntryImpl("datafile_mse", StatisticValues.longValue(5L))));
    StrategyHandlerContext lowContext =
        StrategyHandlerContext.builder(tableId, strategy)
            .withTableMetadata(tableMetadata)
            .withTableStatistics(Arrays.asList())
            .withPartitionStatistics(allLowStats)
            .build();

    CompactionStrategyHandler lowHandler = new CompactionStrategyHandler();
    lowHandler.initialize(lowContext);
    Assertions.assertFalse(lowHandler.shouldTrigger());
  }

  @Test
  void testJobConfig() {
    NameIdentifier tableId = NameIdentifier.of("db", "table");
    Table tableMetadata = Mockito.mock(Table.class);
    JobExecutionContext config =
        BaseExpressionStrategyHandler.getJobConfigFromStrategy(
            tableId,
            strategy,
            tableMetadata,
            (nameIdentifier, handlerStrategy, metadata, jobConfig) ->
                new CompactionJobContext(nameIdentifier, jobConfig, handlerStrategy, metadata));
    Assertions.assertTrue(config instanceof CompactionJobContext);
    CompactionJobContext compactionConfig = (CompactionJobContext) config;
    Assertions.assertEquals(Optional.of(1024L), compactionConfig.targetFileSize());
    Assertions.assertEquals(tableId, compactionConfig.nameIdentifier());
    Assertions.assertEquals(
        ImmutableMap.of(CompactionJobContext.TARGET_FILE_SIZE_BYTES, "1024"),
        compactionConfig.jobConfig());
    Assertions.assertTrue(compactionConfig.partitionNames().isEmpty());
  }
}
