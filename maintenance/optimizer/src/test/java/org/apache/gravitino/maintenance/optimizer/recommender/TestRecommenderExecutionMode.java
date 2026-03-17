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
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyHandlerContext;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyProvider;
import org.apache.gravitino.maintenance.optimizer.api.recommender.SupportTableStatistics;
import org.apache.gravitino.maintenance.optimizer.api.recommender.TableMetadataProvider;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TestRecommenderExecutionMode {
  private static final String STRATEGY_TYPE = "COMPACTION";

  @Test
  void testRecommendForStrategyTypeDoesNotSubmitJobs() {
    NameIdentifier identifier = NameIdentifier.of("catalog", "db", "table");
    Strategy strategyOne = new TestStrategy("s1", STRATEGY_TYPE, "tpl-1");
    Strategy strategyTwo = new TestStrategy("s2", STRATEGY_TYPE, "tpl-2");

    StrategyProvider strategyProvider = Mockito.mock(StrategyProvider.class);
    Mockito.when(strategyProvider.strategies(identifier))
        .thenReturn(List.of(strategyOne, strategyTwo));
    Mockito.when(strategyProvider.strategy("s1")).thenReturn(strategyOne);
    Mockito.when(strategyProvider.strategy("s2")).thenReturn(strategyTwo);
    JobSubmitter jobSubmitter = Mockito.mock(JobSubmitter.class);

    Recommender recommender =
        createRecommender(
            strategyProvider,
            Mockito.mock(StatisticsProvider.class),
            Mockito.mock(TableMetadataProvider.class),
            jobSubmitter);
    List<Recommender.RecommendationResult> results =
        recommender.recommendForStrategyType(List.of(identifier), STRATEGY_TYPE);

    Assertions.assertEquals(2, results.size());
    Assertions.assertTrue(results.stream().allMatch(result -> result.jobId().isEmpty()));
    Mockito.verify(jobSubmitter, Mockito.never())
        .submitJob(Mockito.anyString(), Mockito.any(JobExecutionContext.class));
  }

  @Test
  void testSubmitForStrategyNameSubmitsOnlySelectedStrategy() {
    NameIdentifier identifier = NameIdentifier.of("catalog", "db", "table");
    Strategy strategyOne = new TestStrategy("s1", STRATEGY_TYPE, "tpl-1");
    Strategy strategyTwo = new TestStrategy("s2", STRATEGY_TYPE, "tpl-2");

    StrategyProvider strategyProvider = Mockito.mock(StrategyProvider.class);
    Mockito.when(strategyProvider.strategies(identifier))
        .thenReturn(List.of(strategyOne, strategyTwo));
    Mockito.when(strategyProvider.strategy("s1")).thenReturn(strategyOne);
    Mockito.when(strategyProvider.strategy("s2")).thenReturn(strategyTwo);

    JobSubmitter jobSubmitter = Mockito.mock(JobSubmitter.class);
    Mockito.when(
            jobSubmitter.submitJob(Mockito.anyString(), Mockito.any(JobExecutionContext.class)))
        .thenReturn("job-1");

    Recommender recommender =
        createRecommender(
            strategyProvider,
            Mockito.mock(StatisticsProvider.class),
            Mockito.mock(TableMetadataProvider.class),
            jobSubmitter);
    List<Recommender.RecommendationResult> results =
        recommender.submitForStrategyName(List.of(identifier), "s1");

    Assertions.assertEquals(1, results.size());
    Assertions.assertEquals("s1", results.get(0).strategyName());
    Assertions.assertEquals("job-1", results.get(0).jobId());
    Mockito.verify(jobSubmitter, Mockito.times(1))
        .submitJob(Mockito.eq("tpl-1"), Mockito.any(JobExecutionContext.class));
    Mockito.verify(jobSubmitter, Mockito.never())
        .submitJob(Mockito.eq("tpl-2"), Mockito.any(JobExecutionContext.class));
  }

  @Test
  void testRecommendForStrategyNameDoesNotSubmitJobs() {
    NameIdentifier identifier = NameIdentifier.of("catalog", "db", "table");
    Strategy strategy = new TestStrategy("s1", STRATEGY_TYPE, "tpl-1");

    StrategyProvider strategyProvider = Mockito.mock(StrategyProvider.class);
    Mockito.when(strategyProvider.strategies(identifier)).thenReturn(List.of(strategy));
    Mockito.when(strategyProvider.strategy("s1")).thenReturn(strategy);

    JobSubmitter jobSubmitter = Mockito.mock(JobSubmitter.class);
    Recommender recommender =
        createRecommender(
            strategyProvider,
            Mockito.mock(StatisticsProvider.class),
            Mockito.mock(TableMetadataProvider.class),
            jobSubmitter);

    List<Recommender.RecommendationResult> results =
        recommender.recommendForStrategyName(List.of(identifier), "s1", 10);

    Assertions.assertEquals(1, results.size());
    Assertions.assertTrue(results.get(0).jobId().isEmpty());
    Mockito.verify(jobSubmitter, Mockito.never())
        .submitJob(Mockito.anyString(), Mockito.any(JobExecutionContext.class));
  }

  @Test
  void testSubmitForStrategyNameRespectsLimit() {
    NameIdentifier identifier1 = NameIdentifier.of("catalog", "db", "table1");
    NameIdentifier identifier2 = NameIdentifier.of("catalog", "db", "table2");
    NameIdentifier identifier3 = NameIdentifier.of("catalog", "db", "table3");
    Strategy strategy = new TestStrategy("s1", STRATEGY_TYPE, "tpl-1");

    StrategyProvider strategyProvider = Mockito.mock(StrategyProvider.class);
    Mockito.when(strategyProvider.strategies(identifier1)).thenReturn(List.of(strategy));
    Mockito.when(strategyProvider.strategies(identifier2)).thenReturn(List.of(strategy));
    Mockito.when(strategyProvider.strategies(identifier3)).thenReturn(List.of(strategy));
    Mockito.when(strategyProvider.strategy("s1")).thenReturn(strategy);

    JobSubmitter jobSubmitter = Mockito.mock(JobSubmitter.class);
    Mockito.when(
            jobSubmitter.submitJob(Mockito.anyString(), Mockito.any(JobExecutionContext.class)))
        .thenReturn("job-1");

    Recommender recommender =
        createRecommender(
            strategyProvider,
            Mockito.mock(StatisticsProvider.class),
            Mockito.mock(TableMetadataProvider.class),
            jobSubmitter);

    List<Recommender.RecommendationResult> results =
        recommender.submitForStrategyName(List.of(identifier1, identifier2, identifier3), "s1", 2);

    Assertions.assertEquals(2, results.size());
    Mockito.verify(jobSubmitter, Mockito.times(2))
        .submitJob(Mockito.eq("tpl-1"), Mockito.any(JobExecutionContext.class));
  }

  @Test
  void testSubmitForStrategyNameFailsWhenNoIdentifierMatches() {
    NameIdentifier identifier = NameIdentifier.of("catalog", "db", "table");
    Strategy strategy = new TestStrategy("s1", STRATEGY_TYPE, "tpl-1");

    StrategyProvider strategyProvider = Mockito.mock(StrategyProvider.class);
    Mockito.when(strategyProvider.strategies(identifier)).thenReturn(List.of(strategy));

    Recommender recommender =
        createRecommender(
            strategyProvider,
            Mockito.mock(StatisticsProvider.class),
            Mockito.mock(TableMetadataProvider.class),
            Mockito.mock(JobSubmitter.class));

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> recommender.submitForStrategyName(List.of(identifier), "not-exist-strategy"));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("No identifiers matched strategy name 'not-exist-strategy'"));
  }

  @Test
  void testSubmitForStrategyNameWithOnlyNonExistingTablesAndSkipEnabled() {
    NameIdentifier table1 = NameIdentifier.of("catalog", "db", "non_existing_table1");
    NameIdentifier table2 = NameIdentifier.of("catalog", "db", "non_existing_table2");
    NameIdentifier table3 = NameIdentifier.of("catalog", "db", "non_existing_table3");

    StrategyProvider strategyProvider = Mockito.mock(StrategyProvider.class);
    StatisticsProvider statisticsProvider = Mockito.mock(StatisticsProvider.class);
    TableMetadataProvider tableMetadataProvider = Mockito.mock(TableMetadataProvider.class);
    JobSubmitter jobSubmitter = Mockito.mock(JobSubmitter.class);

    OptimizerConfig config =
        new OptimizerConfig(
            Map.of("gravitino.optimizer.recommender.skipNonExistingTables", "true"));
    OptimizerEnv optimizerEnv = new OptimizerEnv(config);

    Mockito.when(tableMetadataProvider.tableMetadata(table1))
        .thenThrow(new NoSuchTableException("Table %s does not exist", table1));
    Mockito.when(tableMetadataProvider.tableMetadata(table2))
        .thenThrow(new NoSuchTableException("Table %s does not exist", table2));
    Mockito.when(tableMetadataProvider.tableMetadata(table3))
        .thenThrow(new NoSuchTableException("Table %s does not exist", table3));

    Recommender recommender =
        new Recommender(
            strategyProvider,
            statisticsProvider,
            tableMetadataProvider,
            jobSubmitter,
            optimizerEnv);

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> recommender.submitForStrategyName(List.of(table1, table2, table3), "s1"));
    Assertions.assertTrue(
        exception.getMessage().contains("No identifiers matched strategy name 's1'"));

    Mockito.verify(tableMetadataProvider, Mockito.times(1)).tableMetadata(table1);
    Mockito.verify(tableMetadataProvider, Mockito.times(1)).tableMetadata(table2);
    Mockito.verify(tableMetadataProvider, Mockito.times(1)).tableMetadata(table3);
    Mockito.verify(strategyProvider, Mockito.never()).strategies(Mockito.any());
    Mockito.verify(jobSubmitter, Mockito.never()).submitJob(Mockito.anyString(), Mockito.any());
  }

  @Test
  void testSubmitForStrategyNameWithOnlyNonExistingTableAndSkipDisabled() {
    NameIdentifier nonExistingTable = NameIdentifier.of("catalog", "db", "non_existing_table");

    StrategyProvider strategyProvider = Mockito.mock(StrategyProvider.class);
    StatisticsProvider statisticsProvider = Mockito.mock(StatisticsProvider.class);
    TableMetadataProvider tableMetadataProvider = Mockito.mock(TableMetadataProvider.class);
    JobSubmitter jobSubmitter = Mockito.mock(JobSubmitter.class);

    OptimizerConfig config =
        new OptimizerConfig(
            Map.of("gravitino.optimizer.recommender.skipNonExistingTables", "false"));
    OptimizerEnv optimizerEnv = new OptimizerEnv(config);

    Mockito.when(strategyProvider.strategies(nonExistingTable)).thenReturn(List.of());

    Recommender recommender =
        new Recommender(
            strategyProvider,
            statisticsProvider,
            tableMetadataProvider,
            jobSubmitter,
            optimizerEnv);

    // Should not check table existence when skip is disabled
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> recommender.submitForStrategyName(List.of(nonExistingTable), "s1"));
    Assertions.assertTrue(
        exception.getMessage().contains("No identifiers matched strategy name 's1'"));

    Mockito.verify(tableMetadataProvider, Mockito.never()).tableMetadata(nonExistingTable);
    Mockito.verify(strategyProvider, Mockito.times(1)).strategies(nonExistingTable);
    Mockito.verify(jobSubmitter, Mockito.never()).submitJob(Mockito.anyString(), Mockito.any());
  }

  @Test
  void testSubmitForStrategyNameWithBothExistingAndNonExistingTablesAndSkipEnabled() {
    NameIdentifier existingTable1 = NameIdentifier.of("catalog", "db", "existing_table1");
    NameIdentifier nonExistingTable = NameIdentifier.of("catalog", "db", "non_existing_table");
    NameIdentifier existingTable2 = NameIdentifier.of("catalog", "db", "existing_table2");
    String strategyName = "compaction-strategy-1";

    TableMetadataProvider tableMetadataProvider = Mockito.mock(TableMetadataProvider.class);
    JobSubmitter jobSubmitter = Mockito.mock(JobSubmitter.class);

    String templateName = "compaction-job-template";
    Strategy mockStrategy = new TestStrategy(strategyName, STRATEGY_TYPE, templateName);

    // Mock statistics provider required by strategy handler
    SupportTableStatistics statisticsProvider = Mockito.mock(SupportTableStatistics.class);
    Mockito.when(statisticsProvider.tableStatistics(Mockito.any())).thenReturn(List.of());
    Mockito.when(statisticsProvider.partitionStatistics(Mockito.any())).thenReturn(Map.of());

    OptimizerConfig config = Mockito.mock(OptimizerConfig.class);
    Mockito.when(config.getStrategyHandlerClassName(STRATEGY_TYPE))
        .thenReturn(TestStrategyHandler.class.getName());
    Mockito.when(config.get(OptimizerConfig.SKIP_NON_EXISTING_TABLES_CONFIG)).thenReturn(true);
    OptimizerEnv optimizerEnv = Mockito.mock(OptimizerEnv.class);
    Mockito.when(optimizerEnv.config()).thenReturn(config);

    // Mock table metadata
    Table mockTable = Mockito.mock(Table.class);
    Mockito.when(mockTable.name()).thenReturn("existing_table");
    Mockito.when(mockTable.partitioning()).thenReturn(new Transform[0]);

    // Mock existing tables return successfully
    Mockito.when(tableMetadataProvider.tableMetadata(existingTable1)).thenReturn(mockTable);
    Mockito.when(tableMetadataProvider.tableMetadata(existingTable2)).thenReturn(mockTable);
    // Mock non-existing table throws exception
    Mockito.when(tableMetadataProvider.tableMetadata(nonExistingTable))
        .thenThrow(new NoSuchTableException("Table %s does not exist", nonExistingTable));

    // Mock strategy provider returns non-empty strategies for existing tables
    StrategyProvider strategyProvider = Mockito.mock(StrategyProvider.class);
    Mockito.when(strategyProvider.strategies(existingTable1)).thenReturn(List.of(mockStrategy));
    Mockito.when(strategyProvider.strategies(existingTable2)).thenReturn(List.of(mockStrategy));
    Mockito.when(strategyProvider.strategy(strategyName)).thenReturn(mockStrategy);

    // Mock job submitter
    Mockito.when(jobSubmitter.submitJob(Mockito.anyString(), Mockito.any())).thenReturn("job-123");

    Recommender recommender =
        new Recommender(
            strategyProvider,
            statisticsProvider,
            tableMetadataProvider,
            jobSubmitter,
            optimizerEnv);
    createRecommender(strategyProvider, statisticsProvider, tableMetadataProvider, jobSubmitter);

    // Should skip non-existing table and process existing tables
    List<Recommender.RecommendationResult> recommendationResults =
        recommender.submitForStrategyName(
            List.of(existingTable1, nonExistingTable, existingTable2), strategyName);

    Assertions.assertEquals(2, recommendationResults.size());
    List<NameIdentifier> recommendationIdentifiers =
        recommendationResults.stream().map(Recommender.RecommendationResult::identifier).toList();
    Assertions.assertTrue(recommendationIdentifiers.contains(existingTable1));
    Assertions.assertTrue(recommendationIdentifiers.contains(existingTable2));

    // Verify all tables were checked for existence (may be called multiple times for existing
    // tables)
    Mockito.verify(tableMetadataProvider, Mockito.atLeast(1)).tableMetadata(existingTable1);
    Mockito.verify(tableMetadataProvider, Mockito.atLeast(1)).tableMetadata(nonExistingTable);
    Mockito.verify(tableMetadataProvider, Mockito.atLeast(1)).tableMetadata(existingTable2);

    // Verify only existing tables were processed for strategies
    Mockito.verify(strategyProvider, Mockito.times(1)).strategies(existingTable1);
    Mockito.verify(strategyProvider, Mockito.never()).strategies(nonExistingTable);
    Mockito.verify(strategyProvider, Mockito.times(1)).strategies(existingTable2);

    // Verify strategy was looked up by name
    Mockito.verify(strategyProvider, Mockito.times(1)).strategy(strategyName);
  }

  @Test
  void testSubmitForStrategyNameRejectsNonPositiveLimit() {
    NameIdentifier identifier = NameIdentifier.of("catalog", "db", "table");
    Strategy strategy = new TestStrategy("s1", STRATEGY_TYPE, "tpl-1");
    StrategyProvider strategyProvider = Mockito.mock(StrategyProvider.class);
    Mockito.when(strategyProvider.strategies(identifier)).thenReturn(List.of(strategy));
    Mockito.when(strategyProvider.strategy("s1")).thenReturn(strategy);

    Recommender recommender =
        createRecommender(
            strategyProvider,
            Mockito.mock(StatisticsProvider.class),
            Mockito.mock(TableMetadataProvider.class),
            Mockito.mock(JobSubmitter.class));

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> recommender.submitForStrategyName(List.of(identifier), "s1", 0));
    Assertions.assertTrue(exception.getMessage().contains("limit must be > 0"));
  }

  private static Recommender createRecommender(
      StrategyProvider strategyProvider,
      StatisticsProvider statisticsProvider,
      TableMetadataProvider tableMetadataProvider,
      JobSubmitter jobSubmitter) {
    OptimizerConfig config = Mockito.mock(OptimizerConfig.class);
    Mockito.when(config.getStrategyHandlerClassName(STRATEGY_TYPE))
        .thenReturn(TestStrategyHandler.class.getName());
    Mockito.when(config.get(OptimizerConfig.SKIP_NON_EXISTING_TABLES_CONFIG)).thenReturn(false);
    OptimizerEnv optimizerEnv = Mockito.mock(OptimizerEnv.class);
    Mockito.when(optimizerEnv.config()).thenReturn(config);
    return new Recommender(
        strategyProvider, statisticsProvider, tableMetadataProvider, jobSubmitter, optimizerEnv);
  }

  public static class TestStrategyHandler implements StrategyHandler {
    private StrategyHandlerContext context;

    @Override
    public void initialize(StrategyHandlerContext context) {
      this.context = context;
    }

    @Override
    public String strategyType() {
      return STRATEGY_TYPE;
    }

    @Override
    public boolean shouldTrigger() {
      return true;
    }

    @Override
    public StrategyEvaluation evaluate() {
      Strategy strategy = context.strategy();
      JobExecutionContext jobExecutionContext =
          new JobExecutionContext() {
            @Override
            public NameIdentifier nameIdentifier() {
              return context.nameIdentifier();
            }

            @Override
            public Map<String, String> jobOptions() {
              return strategy.jobOptions();
            }

            @Override
            public String jobTemplateName() {
              return strategy.jobTemplateName();
            }
          };
      return new StrategyEvaluation() {
        @Override
        public long score() {
          return 100L;
        }

        @Override
        public Optional<JobExecutionContext> jobExecutionContext() {
          return Optional.of(jobExecutionContext);
        }
      };
    }
  }

  private static final class TestStrategy implements Strategy {
    private final String name;
    private final String strategyType;
    private final String templateName;

    private TestStrategy(String name, String strategyType, String templateName) {
      this.name = name;
      this.strategyType = strategyType;
      this.templateName = templateName;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String strategyType() {
      return strategyType;
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
      return Map.of("opt", "v");
    }

    @Override
    public String jobTemplateName() {
      return templateName;
    }
  }
}
