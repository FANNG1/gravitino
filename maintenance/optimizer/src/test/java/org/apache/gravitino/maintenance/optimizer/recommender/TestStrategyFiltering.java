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

import java.util.ArrayList;
import java.util.HashMap;
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

class TestStrategyFiltering {

  @Test
  void recommendOnlyTablesWithStrategy() {
    NameIdentifier tableWithPolicy = NameIdentifier.of("db", "t_with_policy");
    NameIdentifier tableWithoutPolicy = NameIdentifier.of("db", "t_without_policy");

    StubStrategy compactionStrategy = new StubStrategy("strategy-1", "COMPACTION", new HashMap<>());
    StrategyProvider strategyProvider =
        new StubStrategyProvider(
            Map.of(
                tableWithPolicy, List.of(compactionStrategy),
                tableWithoutPolicy, List.of()),
            Map.of(compactionStrategy.name(), compactionStrategy));

    RecordingJobSubmitter jobSubmitter = new RecordingJobSubmitter();
    Recommender recommender =
        new StubRecommender(
            strategyProvider,
            new NoopStatisticsProvider(),
            new NoopTableMetadataProvider(),
            jobSubmitter);

    recommender.recommendForStrategyType(
        List.of(tableWithPolicy, tableWithoutPolicy), compactionStrategy.strategyType());

    Assertions.assertEquals(
        1, jobSubmitter.submitted.size(), "Only table with policy should be submitted");
    Assertions.assertEquals(
        tableWithPolicy, jobSubmitter.submitted.get(0).nameIdentifier(), "Wrong table submitted");
  }

  private static final class StubRecommender extends Recommender {
    StubRecommender(
        StrategyProvider strategyProvider,
        StatisticsProvider statisticsProvider,
        TableMetadataProvider tableMetadataProvider,
        JobSubmitter jobSubmitter) {
      super(strategyProvider, statisticsProvider, tableMetadataProvider, jobSubmitter);
    }

    @Override
    protected StrategyHandler createStrategyHandler(String strategyType) {
      return new StubStrategyHandler();
    }
  }

  private static final class StubStrategyHandler implements StrategyHandler {
    private StrategyHandlerContext context;

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
              return "template";
            }
          };

      return new StrategyEvaluation() {
        @Override
        public long score() {
          return 1;
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
    private final Map<String, Object> content;

    StubStrategy(String name, String strategyType, Map<String, Object> content) {
      this.name = name;
      this.strategyType = strategyType;
      this.content = content;
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
      return content;
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

  private static final class RecordingJobSubmitter implements JobSubmitter {
    private final List<JobExecutionContext> submitted = new ArrayList<>();

    @Override
    public String name() {
      return "job-submit-recorder";
    }

    @Override
    public void initialize(
        org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv optimizerEnv) {}

    @Override
    public String submitJob(String jobTemplateName, JobExecutionContext jobExecutionContext) {
      submitted.add(jobExecutionContext);
      return "job-" + submitted.size();
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

  private static final class StubStrategyProvider implements StrategyProvider {
    private final Map<NameIdentifier, List<Strategy>> strategiesByTable;
    private final Map<String, Strategy> strategiesByName;

    StubStrategyProvider(
        Map<NameIdentifier, List<Strategy>> strategiesByTable, Map<String, Strategy> strategies) {
      this.strategiesByTable = strategiesByTable;
      this.strategiesByName = strategies;
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
      return strategiesByTable.getOrDefault(nameIdentifier, List.of());
    }

    @Override
    public Strategy strategy(String strategyName) {
      return strategiesByName.get(strategyName);
    }

    @Override
    public void close() throws Exception {}
  }
}
