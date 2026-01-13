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

package org.apache.gravitino.maintenance.optimizer.recommender.actor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.Strategy;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyEvaluation;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyHandler;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyHandlerContext;
import org.apache.gravitino.maintenance.optimizer.recommender.util.ExpressionEvaluator;
import org.apache.gravitino.maintenance.optimizer.recommender.util.QLExpressionEvaluator;
import org.apache.gravitino.maintenance.optimizer.recommender.util.StatisticsUtils;
import org.apache.gravitino.maintenance.optimizer.recommender.util.StrategyUtils;
import org.apache.gravitino.rel.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base strategy handler that provides common expression evaluation and statistics handling.
 *
 * <p>Subclasses supply strategy-specific logic while relying on the shared context, statistics
 * normalization, and expression evaluation utilities.
 */
public abstract class BaseExpressionStrategyHandler implements StrategyHandler {
  private static final Logger LOG = LoggerFactory.getLogger(BaseExpressionStrategyHandler.class);

  private static final java.util.Comparator<PartitionScore> PARTITION_SCORE_ORDER =
      (a, b) -> Long.compare(b.score(), a.score());
  private final ExpressionEvaluator expressionEvaluator;
  private Strategy strategy;
  private List<StatisticEntry<?>> tableStatistics;
  private Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics;
  private Table tableMetadata;
  private NameIdentifier nameIdentifier;

  protected BaseExpressionStrategyHandler() {
    this.expressionEvaluator = new QLExpressionEvaluator();
  }

  @Override
  public void initialize(StrategyHandlerContext context) {
    Preconditions.checkArgument(context.tableMetadata().isPresent(), "Table metadata is null");
    this.nameIdentifier = context.nameIdentifier();
    this.strategy = context.strategy();
    this.tableMetadata = context.tableMetadata().get();
    this.tableStatistics = context.tableStatistics();
    this.partitionStatistics = context.partitionStatistics();
  }

  @Override
  public boolean shouldTrigger() {
    if (isPartitioned()) {
      if (partitionStatistics.isEmpty()) {
        LOG.info("No partition statistics available for table {}", nameIdentifier);
        return false;
      }
      return partitionStatistics.values().stream()
          .anyMatch(stats -> evaluateBool(triggerExpression(strategy), stats));
    }
    return evaluateBool(triggerExpression(strategy), tableStatistics);
  }

  @Override
  public StrategyEvaluation evaluate() {
    if (!isPartitioned()) {
      long score = evaluateLong(scoreExpression(strategy), tableStatistics);
      JobExecutionContext jobContext =
          buildJobExecutionContext(
              nameIdentifier, strategy, tableMetadata, List.of(), jobOptions(strategy));
      return new BasicStrategyEvaluation(score, jobContext);
    }

    List<PartitionScore> partitionScores = getTopPartitionScores(partitionScoreLimit());
    Preconditions.checkState(!partitionScores.isEmpty(), "No partition scores available");
    long score = partitionScores.get(0).score();
    List<PartitionPath> partitions =
        partitionScores.stream().map(PartitionScore::partition).collect(Collectors.toList());
    JobExecutionContext jobContext =
        buildJobExecutionContext(
            nameIdentifier, strategy, tableMetadata, partitions, jobOptions(strategy));
    return new BasicStrategyEvaluation(score, jobContext);
  }

  private String triggerExpression(Strategy strategy) {
    return StrategyUtils.getTriggerExpression(strategy);
  }

  private String scoreExpression(Strategy strategy) {
    return StrategyUtils.getScoreExpression(strategy);
  }

  private Map<String, String> jobOptions(Strategy strategy) {
    return StrategyUtils.getJobOptionsFromStrategy(strategy);
  }

  @VisibleForTesting
  public static boolean shouldTriggerAction(
      Strategy strategy, List<StatisticEntry<?>> statistics, ExpressionEvaluator evaluator) {
    String triggerExpression = StrategyUtils.getTriggerExpression(strategy);
    Map<String, Object> context = buildExpressionContext(strategy, statistics);
    try {
      return evaluator.evaluateBool(triggerExpression, context);
    } catch (RuntimeException e) {
      LOG.warn(
          "Failed to evaluate trigger expression '{}' with context {}",
          triggerExpression,
          context,
          e);
      return false;
    }
  }

  @VisibleForTesting
  public static JobExecutionContext getJobConfigFromStrategy(
      NameIdentifier nameIdentifier,
      Strategy strategy,
      Table tableMetadata,
      JobContextFactory factory) {
    Map<String, String> jobOptions = StrategyUtils.getJobOptionsFromStrategy(strategy);
    return factory.create(nameIdentifier, strategy, tableMetadata, jobOptions);
  }

  @FunctionalInterface
  public interface JobContextFactory {
    JobExecutionContext create(
        NameIdentifier nameIdentifier,
        Strategy strategy,
        Table tableMetadata,
        Map<String, String> jobOptions);
  }

  protected abstract JobExecutionContext buildJobExecutionContext(
      NameIdentifier nameIdentifier,
      Strategy strategy,
      Table tableMetadata,
      List<PartitionPath> partitions,
      Map<String, String> jobOptions);

  private int partitionScoreLimit() {
    return 1;
  }

  private boolean isPartitioned() {
    Preconditions.checkState(tableMetadata != null, "Table metadata must be provided");
    return tableMetadata.partitioning().length > 0;
  }

  private long evaluateLong(String expression, List<StatisticEntry<?>> statistics) {
    Map<String, Object> context = buildExpressionContext(strategy, statistics);
    try {
      return expressionEvaluator.evaluateLong(expression, context);
    } catch (RuntimeException e) {
      LOG.warn("Failed to evaluate expression '{}' with context {}", expression, context, e);
      return -1L;
    }
  }

  private boolean evaluateBool(String expression, List<StatisticEntry<?>> statistics) {
    Map<String, Object> context = buildExpressionContext(strategy, statistics);
    try {
      return expressionEvaluator.evaluateBool(expression, context);
    } catch (RuntimeException e) {
      LOG.warn("Failed to evaluate expression '{}' with context {}", expression, context, e);
      return false;
    }
  }

  private static Map<String, Object> buildExpressionContext(
      Strategy strategy, List<StatisticEntry<?>> statistics) {
    Map<String, Object> context = new HashMap<>();
    context.putAll(StatisticsUtils.buildStatisticsContext(statistics));
    strategy
        .rules()
        .forEach(
            (k, v) -> {
              try {
                context.put(k, Long.parseLong(v.toString()));
              } catch (NumberFormatException e) {
                // Ignore non-numeric rule values when building numeric expression inputs.
              }
            });
    return context;
  }

  private List<PartitionScore> getTopPartitionScores(int limit) {
    PriorityQueue<PartitionScore> scoreQueue = new PriorityQueue<>(PARTITION_SCORE_ORDER);
    partitionStatistics.forEach(
        (partitionPath, statistics) -> {
          long partitionScore = evaluateLong(scoreExpression(strategy), statistics);
          scoreQueue.add(new PartitionScore(partitionPath, partitionScore));
        });

    Preconditions.checkState(
        scoreQueue.size() > 0,
        "Number of scored partitions is zero, which is unexpected",
        scoreQueue.size());

    List<PartitionScore> results = new java.util.ArrayList<>();
    while (results.size() < limit && !scoreQueue.isEmpty()) {
      results.add(scoreQueue.poll());
    }
    return results;
  }

  private static final class PartitionScore {
    private final PartitionPath partition;
    private final long score;

    private PartitionScore(PartitionPath partition, long score) {
      this.partition = partition;
      this.score = score;
    }

    private PartitionPath partition() {
      return partition;
    }

    private long score() {
      return score;
    }
  }

  private static final class BasicStrategyEvaluation implements StrategyEvaluation {
    private final long score;
    private final JobExecutionContext jobConfig;

    private BasicStrategyEvaluation(long score, JobExecutionContext jobConfig) {
      this.score = score;
      this.jobConfig = jobConfig;
    }

    @Override
    public long score() {
      return score;
    }

    @Override
    public JobExecutionContext jobExecutionContext() {
      return jobConfig;
    }
  }
}
