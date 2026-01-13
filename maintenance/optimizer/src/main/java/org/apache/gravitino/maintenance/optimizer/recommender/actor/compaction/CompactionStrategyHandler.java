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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.Accessors;
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

public class CompactionStrategyHandler implements StrategyHandler {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionStrategyHandler.class);
  public static final String NAME = "compaction";

  private ExpressionEvaluator expressionEvaluator;
  private Strategy strategy;
  private List<StatisticEntry<?>> tableStatistics;
  private Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics;
  private Table tableMetadata;
  private NameIdentifier nameIdentifier;
  private static final java.util.Comparator<PartitionScore> PARTITION_SCORE_ORDER =
      (a, b) -> Long.compare(b.score(), a.score());

  @Override
  public Set<DataRequirement> dataRequirements() {
    return EnumSet.of(
        DataRequirement.TABLE_METADATA,
        DataRequirement.TABLE_STATISTICS,
        DataRequirement.PARTITION_STATISTICS);
  }

  @Override
  public void initialize(StrategyHandlerContext context) {
    this.nameIdentifier = context.nameIdentifier();
    this.strategy = context.strategy();
    this.tableMetadata = context.tableMetadata().orElse(null);
    this.tableStatistics = context.tableStatistics();
    this.partitionStatistics = context.partitionStatistics();
    this.expressionEvaluator = new QLExpressionEvaluator();
  }

  @Override
  public String strategyType() {
    return StrategyUtils.COMPACTION_STRATEGY_TYPE;
  }

  @Override
  public boolean shouldTrigger() {
    if (isPartitioned()) {
      if (partitionStatistics.isEmpty()) {
        LOG.info("No partition statistics available for table {}", nameIdentifier);
        return false;
      }
      return partitionStatistics.values().stream()
          .anyMatch(
              statistics -> shouldTriggerCompaction(strategy, statistics, expressionEvaluator));
    }
    return shouldTriggerCompaction(strategy, tableStatistics, expressionEvaluator);
  }

  @Override
  public StrategyEvaluation evaluate() {
    if (!isPartitioned()) {
      long score = getScore(strategy, tableStatistics);
      JobExecutionContext jobContext =
          getJobConfigFromStrategy(nameIdentifier, strategy, tableMetadata);
      return new BasicStrategyEvaluation(score, jobContext);
    }

    List<PartitionScore> partitionScores = getTopPartitionScores(1);
    Preconditions.checkState(!partitionScores.isEmpty(), "No partition scores available");
    long score = partitionScores.get(0).score();

    Map<String, Object> jobConfig = StrategyUtils.getJobConfigFromStrategy(strategy);
    List<PartitionPath> partitions =
        partitionScores.stream().map(PartitionScore::partition).collect(Collectors.toList());
    JobExecutionContext jobContext =
        new CompactionJobContext(
            nameIdentifier, toStringMap(jobConfig), strategy, tableMetadata, partitions);
    return new BasicStrategyEvaluation(score, jobContext);
  }

  private long getScore(Strategy strategy, List<StatisticEntry<?>> statistics) {
    Map<String, Object> context = buildExpressionContext(strategy, statistics);
    String scoreExpression = StrategyUtils.getScoreExpression(strategy);
    return expressionEvaluator.evaluateLong(scoreExpression, context);
  }

  @VisibleForTesting
  static boolean shouldTriggerCompaction(
      Strategy strategy, List<StatisticEntry<?>> statistics, ExpressionEvaluator evaluator) {
    String triggerExpression = StrategyUtils.getTriggerExpression(strategy);
    Map<String, Object> context = buildExpressionContext(strategy, statistics);
    return evaluator.evaluateBool(triggerExpression, context);
  }

  private boolean isPartitioned() {
    Preconditions.checkState(tableMetadata != null, "Table metadata must be provided");
    return tableMetadata.partitioning().length > 0;
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

  @VisibleForTesting
  static JobExecutionContext getJobConfigFromStrategy(
      NameIdentifier nameIdentifier, Strategy strategy, Table tableMetadata) {
    Map<String, Object> jobConfig = StrategyUtils.getJobConfigFromStrategy(strategy);
    return new CompactionJobContext(
        nameIdentifier, toStringMap(jobConfig), strategy, tableMetadata);
  }

  private List<PartitionScore> getTopPartitionScores(int limit) {
    PriorityQueue<PartitionScore> scoreQueue = new PriorityQueue<>(PARTITION_SCORE_ORDER);
    partitionStatistics.forEach(
        (partitionPath, statistics) -> {
          long partitionScore = getScore(strategy, statistics);
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

  private static Map<String, String> toStringMap(Map<String, Object> jobConfig) {
    return jobConfig.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> String.valueOf(entry.getValue())));
  }

  @Getter
  @Accessors(fluent = true)
  @AllArgsConstructor
  private static final class PartitionScore {
    private final PartitionPath partition;
    private final long score;
  }

  private static final class BasicStrategyEvaluation implements StrategyEvaluation {
    private final long score;
    private final JobExecutionContext jobConfig;

    BasicStrategyEvaluation(long score, JobExecutionContext jobConfig) {
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
