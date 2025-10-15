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
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.Strategy;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobSubmitter;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StatisticsProvider;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyEvaluation;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyHandler;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyHandlerContext;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyProvider;
import org.apache.gravitino.maintenance.optimizer.api.recommender.TableMetadataProvider;
import org.apache.gravitino.rel.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestRecommenderOrdering {

  @Test
  void recommendForOneStrategyOrdersByScoreDescending() {
    NameIdentifier tableA = NameIdentifier.of("db", "t1");
    NameIdentifier tableB = NameIdentifier.of("db", "t2");
    NameIdentifier tableC = NameIdentifier.of("db", "t3");

    Strategy strategy = new StubStrategy("strategy-1", "COMPACTION");
    StrategyProvider strategyProvider = new StubStrategyProvider(strategy);
    Map<NameIdentifier, Long> scores = Map.of(tableA, 10L, tableB, 50L, tableC, 30L);

    Recommender recommender =
        new OrderingTestRecommender(
            strategyProvider,
            new NoopStatisticsProvider(),
            new NoopTableMetadataProvider(),
            new NoopJobSubmitter(),
            scores);

    List<JobExecutionContext> jobs =
        recommender.recommendForOneStrategy(List.of(tableA, tableB, tableC), strategy.name());

    Assertions.assertEquals(3, jobs.size(), "All tables should produce a job context");
    Assertions.assertEquals(
        tableB, jobs.get(0).nameIdentifier(), "Highest score should come first");
    Assertions.assertEquals(tableC, jobs.get(1).nameIdentifier(), "Second highest score expected");
    Assertions.assertEquals(tableA, jobs.get(2).nameIdentifier(), "Lowest score expected last");
  }

  private static final class OrderingTestRecommender extends Recommender {
    private final Map<NameIdentifier, Long> scores;

    OrderingTestRecommender(
        StrategyProvider strategyProvider,
        StatisticsProvider statisticsProvider,
        TableMetadataProvider tableMetadataProvider,
        JobSubmitter jobSubmitter,
        Map<NameIdentifier, Long> scores) {
      super(strategyProvider, statisticsProvider, tableMetadataProvider, jobSubmitter);
      this.scores = scores;
    }

    @Override
    protected StrategyHandler createStrategyHandler(String strategyType) {
      return new ScoringStrategyHandler(scores);
    }
  }

  private static final class ScoringStrategyHandler implements StrategyHandler {
    private final Map<NameIdentifier, Long> scores;
    private StrategyHandlerContext context;

    ScoringStrategyHandler(Map<NameIdentifier, Long> scores) {
      this.scores = scores;
    }

    @Override
    public void initialize(StrategyHandlerContext context) {
      this.context = context;
    }

    @Override
    public String strategyType() {
      return "COMPACTION";
    }

    @Override
    public boolean shouldTrigger() {
      return true;
    }

    @Override
    public StrategyEvaluation evaluate() {
      long score = scores.getOrDefault(context.nameIdentifier(), 0L);
      JobExecutionContext jobContext =
          new JobExecutionContext() {
            @Override
            public NameIdentifier nameIdentifier() {
              return context.nameIdentifier();
            }

            @Override
            public Map<String, String> jobConfig() {
              return Map.of();
            }

            @Override
            public String jobTemplateName() {
              return context.strategy().jobTemplateName();
            }
          };
      return new StrategyEvaluation() {
        @Override
        public long score() {
          return score;
        }

        @Override
        public JobExecutionContext jobExecutionContext() {
          return jobContext;
        }
      };
    }
  }

  private static final class StubStrategy implements Strategy {
    private final String name;
    private final String strategyType;

    StubStrategy(String name, String strategyType) {
      this.name = name;
      this.strategyType = strategyType;
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
      return Map.of();
    }

    @Override
    public String jobTemplateName() {
      return "template";
    }
  }

  private static final class StubStrategyProvider implements StrategyProvider {
    private final Strategy strategy;

    StubStrategyProvider(Strategy strategy) {
      this.strategy = strategy;
    }

    @Override
    public String name() {
      return "stub-strategy-provider";
    }

    @Override
    public void initialize(
        org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv optimizerEnv) {}

    @Override
    public List<Strategy> strategies(NameIdentifier nameIdentifier) {
      return List.of(strategy);
    }

    @Override
    public Strategy strategy(String strategyName) {
      return strategy;
    }

    @Override
    public void close() throws Exception {}
  }

  private static final class NoopStatisticsProvider implements StatisticsProvider {
    @Override
    public String name() {
      return "noop-stats";
    }

    @Override
    public void initialize(
        org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv optimizerEnv) {}

    @Override
    public void close() throws Exception {}
  }

  private static final class NoopTableMetadataProvider implements TableMetadataProvider {
    @Override
    public String name() {
      return "noop-metadata";
    }

    @Override
    public void initialize(
        org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv optimizerEnv) {}

    @Override
    public Table tableMetadata(NameIdentifier tableIdentifier) {
      return null;
    }

    @Override
    public void close() throws Exception {}
  }

  private static final class NoopJobSubmitter implements JobSubmitter {
    @Override
    public String name() {
      return "noop-job-submitter";
    }

    @Override
    public void initialize(
        org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv optimizerEnv) {}

    @Override
    public String submitJob(String jobTemplateName, JobExecutionContext jobExecutionContext) {
      return "job-1";
    }

    @Override
    public void close() throws Exception {}
  }
}
