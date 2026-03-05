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

package org.apache.gravitino.maintenance.optimizer.recommender;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.maintenance.optimizer.api.common.Strategy;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobSubmitter;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StatisticsProvider;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyEvaluation;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyHandler;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyProvider;
import org.apache.gravitino.maintenance.optimizer.api.recommender.TableMetadataProvider;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TestRecommender {

  @Test
  void testRecommendForStrategyTypeWithNonExistingTable() {
    NameIdentifier nonExistingTable = NameIdentifier.of("catalog", "db", "non_existing_table");
    String strategyType = "compaction";

    StrategyProvider strategyProvider = Mockito.mock(StrategyProvider.class);
    StatisticsProvider statisticsProvider = Mockito.mock(StatisticsProvider.class);
    TableMetadataProvider tableMetadataProvider = Mockito.mock(TableMetadataProvider.class);
    JobSubmitter jobSubmitter = Mockito.mock(JobSubmitter.class);
    OptimizerEnv optimizerEnv = Mockito.mock(OptimizerEnv.class);
    StrategyHandler strategyHandler = Mockito.mock(StrategyHandler.class);

    Mockito.when(tableMetadataProvider.tableMetadata(nonExistingTable))
        .thenThrow(new NoSuchTableException("Table %s does not exist", nonExistingTable));

    Recommender recommender =
        createRecommenderWithMockedHandler(
            strategyProvider,
            statisticsProvider,
            tableMetadataProvider,
            jobSubmitter,
            optimizerEnv,
            strategyHandler);

    recommender.recommendForStrategyType(List.of(nonExistingTable), strategyType);

    Mockito.verify(tableMetadataProvider, Mockito.times(1)).tableMetadata(nonExistingTable);
    Mockito.verify(jobSubmitter, Mockito.never()).submitJob(Mockito.anyString(), Mockito.any());
    Mockito.verify(jobSubmitter, Mockito.never())
        .submitBatchJob(Mockito.anyString(), Mockito.anyList());
    Mockito.verify(strategyHandler, Mockito.never()).evaluate();
  }

  @Test
  void testRecommendForStrategyTypeWithEmptyEvaluations() {
    NameIdentifier table1 = NameIdentifier.of("catalog", "db", "table1");
    String strategyType = "compaction";
    String strategyName = "compaction-strategy";

    StrategyProvider strategyProvider = Mockito.mock(StrategyProvider.class);
    StatisticsProvider statisticsProvider = Mockito.mock(StatisticsProvider.class);
    TableMetadataProvider tableMetadataProvider = Mockito.mock(TableMetadataProvider.class);
    JobSubmitter jobSubmitter = Mockito.mock(JobSubmitter.class);
    OptimizerEnv optimizerEnv = Mockito.mock(OptimizerEnv.class);

    Strategy strategy = createMockStrategy(strategyName, strategyType);
    Mockito.when(strategyProvider.strategies(table1)).thenReturn(List.of(strategy));
    Mockito.when(strategyProvider.strategy(strategyName)).thenReturn(strategy);

    StrategyHandler strategyHandler = Mockito.mock(StrategyHandler.class);
    Mockito.when(strategyHandler.dataRequirements()).thenReturn(Collections.emptySet());
    Mockito.when(strategyHandler.shouldTrigger()).thenReturn(false);

    Recommender recommender =
        createRecommenderWithMockedHandler(
            strategyProvider,
            statisticsProvider,
            tableMetadataProvider,
            jobSubmitter,
            optimizerEnv,
            strategyHandler);

    recommender.recommendForStrategyType(List.of(table1), strategyType);

    Mockito.verify(strategyProvider, Mockito.times(1)).strategies(table1);
    Mockito.verify(strategyHandler, Mockito.times(1)).shouldTrigger();
    Mockito.verify(strategyHandler, Mockito.never()).evaluate();
    Mockito.verify(jobSubmitter, Mockito.never()).submitJob(Mockito.anyString(), Mockito.any());
    Mockito.verify(jobSubmitter, Mockito.never())
        .submitBatchJob(Mockito.anyString(), Mockito.anyList());
  }

  @Test
  void testRecommendForStrategyTypeWithOnlySingleJobs() {
    NameIdentifier table1 = NameIdentifier.of("catalog", "db", "table1");
    NameIdentifier table2 = NameIdentifier.of("catalog", "db", "table2");
    String strategyType = "compaction";
    String strategyName = "compaction-strategy";
    String jobTemplateName = "single-job-template";

    StrategyProvider strategyProvider = Mockito.mock(StrategyProvider.class);
    StatisticsProvider statisticsProvider = Mockito.mock(StatisticsProvider.class);
    TableMetadataProvider tableMetadataProvider = Mockito.mock(TableMetadataProvider.class);
    JobSubmitter jobSubmitter = Mockito.mock(JobSubmitter.class);
    OptimizerEnv optimizerEnv = Mockito.mock(OptimizerEnv.class);

    Strategy strategy = createMockStrategy(strategyName, strategyType);
    Mockito.when(strategyProvider.strategies(table1)).thenReturn(List.of(strategy));
    Mockito.when(strategyProvider.strategies(table2)).thenReturn(List.of(strategy));
    Mockito.when(strategyProvider.strategy(strategyName)).thenReturn(strategy);

    Mockito.when(jobSubmitter.supportsBatchJob(jobTemplateName)).thenReturn(false);
    Mockito.when(jobSubmitter.submitJob(Mockito.eq(jobTemplateName), Mockito.any()))
        .thenReturn("job-id-1", "job-id-2");

    JobExecutionContext context1 = createJobExecutionContext(table1, jobTemplateName);
    JobExecutionContext context2 = createJobExecutionContext(table2, jobTemplateName);
    StrategyEvaluation eval1 = createStrategyEvaluation(context1, 100L);
    StrategyEvaluation eval2 = createStrategyEvaluation(context2, 200L);

    StrategyHandler strategyHandler = Mockito.mock(StrategyHandler.class);
    Mockito.when(strategyHandler.dataRequirements()).thenReturn(Collections.emptySet());
    Mockito.when(strategyHandler.shouldTrigger()).thenReturn(true);
    Mockito.when(strategyHandler.evaluate()).thenReturn(eval1, eval2);

    Recommender recommender =
        createRecommenderWithMockedHandler(
            strategyProvider,
            statisticsProvider,
            tableMetadataProvider,
            jobSubmitter,
            optimizerEnv,
            strategyHandler);

    recommender.recommendForStrategyType(List.of(table1, table2), strategyType);

    Mockito.verify(jobSubmitter, Mockito.times(2))
        .submitJob(Mockito.eq(jobTemplateName), Mockito.any());
    Mockito.verify(jobSubmitter, Mockito.never())
        .submitBatchJob(Mockito.anyString(), Mockito.anyList());

    ArgumentCaptor<JobExecutionContext> contextCaptor =
        ArgumentCaptor.forClass(JobExecutionContext.class);
    Mockito.verify(jobSubmitter, Mockito.times(2))
        .submitJob(Mockito.eq(jobTemplateName), contextCaptor.capture());

    List<JobExecutionContext> capturedContexts = contextCaptor.getAllValues();
    Assertions.assertEquals(2, capturedContexts.size());
    Assertions.assertTrue(
        capturedContexts.stream().anyMatch(ctx -> ctx.nameIdentifier().equals(table1)));
    Assertions.assertTrue(
        capturedContexts.stream().anyMatch(ctx -> ctx.nameIdentifier().equals(table2)));
  }

  @Test
  void testRecommendForStrategyTypeWithOnlyBatchJobs() {
    NameIdentifier table1 = NameIdentifier.of("catalog", "db", "table1");
    NameIdentifier table2 = NameIdentifier.of("catalog", "db", "table2");
    NameIdentifier table3 = NameIdentifier.of("catalog", "db", "table3");
    String strategyType = "compaction";
    String strategyName = "compaction-strategy";
    String jobTemplateName = "batch-job-template";

    StrategyProvider strategyProvider = Mockito.mock(StrategyProvider.class);
    StatisticsProvider statisticsProvider = Mockito.mock(StatisticsProvider.class);
    TableMetadataProvider tableMetadataProvider = Mockito.mock(TableMetadataProvider.class);
    JobSubmitter jobSubmitter = Mockito.mock(JobSubmitter.class);
    OptimizerEnv optimizerEnv = Mockito.mock(OptimizerEnv.class);

    Strategy strategy = createMockStrategy(strategyName, strategyType);
    Mockito.when(strategyProvider.strategies(table1)).thenReturn(List.of(strategy));
    Mockito.when(strategyProvider.strategies(table2)).thenReturn(List.of(strategy));
    Mockito.when(strategyProvider.strategies(table3)).thenReturn(List.of(strategy));
    Mockito.when(strategyProvider.strategy(strategyName)).thenReturn(strategy);

    Mockito.when(jobSubmitter.supportsBatchJob(jobTemplateName)).thenReturn(true);
    Mockito.when(jobSubmitter.submitBatchJob(Mockito.eq(jobTemplateName), Mockito.anyList()))
        .thenReturn("batch-job-id");

    JobExecutionContext context1 = createJobExecutionContext(table1, jobTemplateName);
    JobExecutionContext context2 = createJobExecutionContext(table2, jobTemplateName);
    JobExecutionContext context3 = createJobExecutionContext(table3, jobTemplateName);
    StrategyEvaluation eval1 = createStrategyEvaluation(context1, 100L);
    StrategyEvaluation eval2 = createStrategyEvaluation(context2, 200L);
    StrategyEvaluation eval3 = createStrategyEvaluation(context3, 150L);

    StrategyHandler strategyHandler = Mockito.mock(StrategyHandler.class);
    Mockito.when(strategyHandler.dataRequirements()).thenReturn(Collections.emptySet());
    Mockito.when(strategyHandler.shouldTrigger()).thenReturn(true);
    Mockito.when(strategyHandler.evaluate()).thenReturn(eval1, eval2, eval3);

    Recommender recommender =
        createRecommenderWithMockedHandler(
            strategyProvider,
            statisticsProvider,
            tableMetadataProvider,
            jobSubmitter,
            optimizerEnv,
            strategyHandler);

    recommender.recommendForStrategyType(List.of(table1, table2, table3), strategyType);

    Mockito.verify(jobSubmitter, Mockito.never()).submitJob(Mockito.anyString(), Mockito.any());
    Mockito.verify(jobSubmitter, Mockito.times(1))
        .submitBatchJob(Mockito.eq(jobTemplateName), Mockito.anyList());

    ArgumentCaptor<List<JobExecutionContext>> batchContextCaptor =
        ArgumentCaptor.forClass(List.class);
    Mockito.verify(jobSubmitter, Mockito.times(1))
        .submitBatchJob(Mockito.eq(jobTemplateName), batchContextCaptor.capture());

    List<JobExecutionContext> capturedBatchContexts = batchContextCaptor.getValue();
    Assertions.assertEquals(3, capturedBatchContexts.size());
    Assertions.assertTrue(
        capturedBatchContexts.stream().anyMatch(ctx -> ctx.nameIdentifier().equals(table1)));
    Assertions.assertTrue(
        capturedBatchContexts.stream().anyMatch(ctx -> ctx.nameIdentifier().equals(table2)));
    Assertions.assertTrue(
        capturedBatchContexts.stream().anyMatch(ctx -> ctx.nameIdentifier().equals(table3)));
  }

  @Test
  void testRecommendForStrategyTypeWithMixedSingleAndBatchJobs() {
    NameIdentifier table1 = NameIdentifier.of("catalog", "db", "table1");
    NameIdentifier table2 = NameIdentifier.of("catalog", "db", "table2");
    NameIdentifier table3 = NameIdentifier.of("catalog", "db", "table3");
    String strategyType = "compaction";
    String strategyName = "compaction-strategy";
    String singleJobTemplate = "single-job-template";
    String batchJobTemplate = "batch-job-template";

    StrategyProvider strategyProvider = Mockito.mock(StrategyProvider.class);
    StatisticsProvider statisticsProvider = Mockito.mock(StatisticsProvider.class);
    TableMetadataProvider tableMetadataProvider = Mockito.mock(TableMetadataProvider.class);
    JobSubmitter jobSubmitter = Mockito.mock(JobSubmitter.class);
    OptimizerEnv optimizerEnv = Mockito.mock(OptimizerEnv.class);

    Strategy strategy = createMockStrategy(strategyName, strategyType);
    Mockito.when(strategyProvider.strategies(table1)).thenReturn(List.of(strategy));
    Mockito.when(strategyProvider.strategies(table2)).thenReturn(List.of(strategy));
    Mockito.when(strategyProvider.strategies(table3)).thenReturn(List.of(strategy));
    Mockito.when(strategyProvider.strategy(strategyName)).thenReturn(strategy);

    Mockito.when(jobSubmitter.supportsBatchJob(singleJobTemplate)).thenReturn(false);
    Mockito.when(jobSubmitter.supportsBatchJob(batchJobTemplate)).thenReturn(true);
    Mockito.when(jobSubmitter.submitJob(Mockito.eq(singleJobTemplate), Mockito.any()))
        .thenReturn("single-job-id");
    Mockito.when(jobSubmitter.submitBatchJob(Mockito.eq(batchJobTemplate), Mockito.anyList()))
        .thenReturn("batch-job-id");

    JobExecutionContext context1 = createJobExecutionContext(table1, singleJobTemplate);
    JobExecutionContext context2 = createJobExecutionContext(table2, batchJobTemplate);
    JobExecutionContext context3 = createJobExecutionContext(table3, batchJobTemplate);
    StrategyEvaluation eval1 = createStrategyEvaluation(context1, 100L);
    StrategyEvaluation eval2 = createStrategyEvaluation(context2, 200L);
    StrategyEvaluation eval3 = createStrategyEvaluation(context3, 150L);

    StrategyHandler strategyHandler = Mockito.mock(StrategyHandler.class);
    Mockito.when(strategyHandler.dataRequirements()).thenReturn(Collections.emptySet());
    Mockito.when(strategyHandler.shouldTrigger()).thenReturn(true);
    Mockito.when(strategyHandler.evaluate()).thenReturn(eval1, eval2, eval3);

    Recommender recommender =
        createRecommenderWithMockedHandler(
            strategyProvider,
            statisticsProvider,
            tableMetadataProvider,
            jobSubmitter,
            optimizerEnv,
            strategyHandler);

    recommender.recommendForStrategyType(List.of(table1, table2, table3), strategyType);

    Mockito.verify(jobSubmitter, Mockito.times(1))
        .submitJob(Mockito.eq(singleJobTemplate), Mockito.any());
    Mockito.verify(jobSubmitter, Mockito.times(1))
        .submitBatchJob(Mockito.eq(batchJobTemplate), Mockito.anyList());
  }

  private Recommender createRecommenderWithMockedHandler(
      StrategyProvider strategyProvider,
      StatisticsProvider statisticsProvider,
      TableMetadataProvider tableMetadataProvider,
      JobSubmitter jobSubmitter,
      OptimizerEnv optimizerEnv,
      StrategyHandler strategyHandler) {
    return new Recommender(
        strategyProvider, statisticsProvider, tableMetadataProvider, jobSubmitter, optimizerEnv) {
      @Override
      protected StrategyHandler createStrategyHandler(String strategyType) {
        return strategyHandler;
      }
    };
  }

  private Strategy createMockStrategy(String name, String type) {
    return new Strategy() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public String strategyType() {
        return type;
      }

      @Override
      public Map<String, String> properties() {
        return Map.of();
      }

      @Override
      public Map<String, Object> rules() {
        return Map.of();
      }

      @Override
      public Map<String, String> jobOptions() {
        return Map.of();
      }

      @Override
      public String jobTemplateName() {
        return "";
      }
    };
  }

  private JobExecutionContext createJobExecutionContext(
      NameIdentifier identifier, String templateName) {
    return new JobExecutionContext() {
      @Override
      public NameIdentifier nameIdentifier() {
        return identifier;
      }

      @Override
      public Map<String, String> jobOptions() {
        return Collections.emptyMap();
      }

      @Override
      public String jobTemplateName() {
        return templateName;
      }
    };
  }

  private StrategyEvaluation createStrategyEvaluation(JobExecutionContext context, long score) {
    return new StrategyEvaluation() {
      @Override
      public long score() {
        return score;
      }

      @Override
      public Optional<JobExecutionContext> jobExecutionContext() {
        return Optional.of(context);
      }
    };
  }
}
