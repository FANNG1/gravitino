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

import com.google.common.base.Preconditions;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
  private static final Comparator<PartitionScore> PARTITION_SCORE_ORDER =
      (a, b) -> Long.compare(b.score(), a.score());

  private final ExpressionEvaluator expressionEvaluator;
  private Strategy strategy;
  private List<StatisticEntry<?>> tableStatistics;
  private Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics;
  private Table tableMetadata;
  private NameIdentifier nameIdentifier;

  /** Create a handler that evaluates expressions with the default QL evaluator. */
  protected BaseExpressionStrategyHandler() {
    this.expressionEvaluator = new QLExpressionEvaluator();
  }

  @Override
  public void initialize(StrategyHandlerContext context) {
    Preconditions.checkArgument(context.tableMetadata().isPresent(), "Table metadata is null");
    this.tableMetadata = context.tableMetadata().get();
    this.nameIdentifier = context.nameIdentifier();
    this.strategy = context.strategy();
    this.tableStatistics = context.tableStatistics();
    this.partitionStatistics = context.partitionStatistics();
  }

  @Override
  public boolean shouldTrigger() {
    if (isPartitionTable()) {
      return shouldTriggerForPartitionTable();
    }
    return shouldTriggerForNonPartitionTable();
  }

  @Override
  public StrategyEvaluation evaluate() {
    if (isPartitionTable()) {
      return evaluateForPartitionTable();
    }
    return evaluateForNonPartitionTable();
  }

  /**
   * Build the execution context for the selected partitions.
   *
   * @param nameIdentifier target table identifier
   * @param strategy strategy being evaluated
   * @param tableMetadata table metadata requested by the handler
   * @param partitions selected partitions, empty for non-partitioned tables
   * @param jobOptions job options derived from the strategy
   * @return job execution context
   */
  protected abstract JobExecutionContext buildJobExecutionContext(
      NameIdentifier nameIdentifier,
      Strategy strategy,
      Table tableMetadata,
      List<PartitionPath> partitions,
      Map<String, String> jobOptions);

  private int maxPartitionNum() {
    return StrategyUtils.getMaxPartitionNum(strategy);
  }

  private boolean isPartitionTable() {
    return tableMetadata.partitioning().length > 0;
  }

  private boolean shouldTriggerForPartitionTable() {
    if (partitionStatistics.isEmpty()) {
      LOG.info("No partition statistics available for table {}", nameIdentifier);
      return false;
    }
    String triggerExpression = triggerExpression(strategy);
    return partitionStatistics.values().stream()
        .anyMatch(partitionStats -> evaluateBool(triggerExpression, partitionStats));
  }

  private boolean shouldTriggerForNonPartitionTable() {
    return evaluateBool(triggerExpression(strategy), tableStatistics);
  }

  private StrategyEvaluation evaluateForNonPartitionTable() {
    long score = evaluateLong(scoreExpression(strategy), tableStatistics);
    if (score <= 0) {
      return StrategyEvaluation.NO_EXECUTION;
    }
    JobExecutionContext jobContext =
        buildJobExecutionContext(
            nameIdentifier, strategy, tableMetadata, List.of(), jobOptions(strategy));
    return new BasicStrategyEvaluation(score, jobContext);
  }

  /**
   * Aggregate partition scores into a table score. The score mode is controlled by {@link
   * StrategyUtils#PARTITION_TABLE_SCORE_MODE} and defaults to {@code avg}.
   */
  private long getTableScoreFromPartitions(List<PartitionScore> partitionScores) {
    if (partitionScores.isEmpty()) {
      return -1L;
    }
    ScoreMode scoreMode = StrategyUtils.getPartitionTableScoreMode(strategy);
    switch (scoreMode) {
      case SUM:
        return partitionScores.stream().mapToLong(PartitionScore::score).sum();
      case MAX:
        return partitionScores.stream().mapToLong(PartitionScore::score).max().orElse(-1L);
      case AVG:
        return partitionScores.stream().mapToLong(PartitionScore::score).sum()
            / partitionScores.size();
      default:
        LOG.warn(
            "Unsupported partition table score mode '{}' for strategy {}, defaulting to avg",
            scoreMode,
            strategy.name());
        return partitionScores.stream().mapToLong(PartitionScore::score).sum()
            / partitionScores.size();
    }
  }

  private StrategyEvaluation evaluateForPartitionTable() {
    List<PartitionScore> partitionScores = getTopPartitionScores(maxPartitionNum());
    if (partitionScores.isEmpty()) {
      return StrategyEvaluation.NO_EXECUTION;
    }
    List<PartitionPath> partitions =
        partitionScores.stream().map(PartitionScore::partition).collect(Collectors.toList());
    JobExecutionContext jobContext =
        buildJobExecutionContext(
            nameIdentifier, strategy, tableMetadata, partitions, jobOptions(strategy));
    long tableScore = getTableScoreFromPartitions(partitionScores);
    return new BasicStrategyEvaluation(tableScore, jobContext);
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

  private String triggerExpression(Strategy strategy) {
    return StrategyUtils.getTriggerExpression(strategy);
  }

  private String scoreExpression(Strategy strategy) {
    return StrategyUtils.getScoreExpression(strategy);
  }

  private Map<String, String> jobOptions(Strategy strategy) {
    return StrategyUtils.getJobOptionsFromStrategy(strategy);
  }

  /**
   * Return the highest-scoring partitions in descending order.
   *
   * @param limit max partitions to return
   * @return top partition scores, empty when none score above zero
   */
  private List<PartitionScore> getTopPartitionScores(int limit) {
    PriorityQueue<PartitionScore> scoreQueue = new PriorityQueue<>(PARTITION_SCORE_ORDER);
    partitionStatistics.forEach(
        (partitionPath, statistics) -> {
          boolean trigger = evaluateBool(triggerExpression(strategy), statistics);
          if (trigger) {
            long partitionScore = evaluateLong(scoreExpression(strategy), statistics);
            if (partitionScore > 0) {
              scoreQueue.add(new PartitionScore(partitionPath, partitionScore));
            }
          }
        });

    return Stream.generate(scoreQueue::poll)
        .takeWhile(Objects::nonNull)
        .limit(limit)
        .collect(Collectors.toList());
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
