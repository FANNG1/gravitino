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

package org.apache.gravitino.optimizer.recommender.actor;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.optimizer.api.common.SingleStatistic;
import org.apache.gravitino.optimizer.api.recommender.PolicyActor;
import org.apache.gravitino.optimizer.recommender.util.ExpressionEvaluator;
import org.apache.gravitino.optimizer.recommender.util.PolicyUtils;
import org.apache.gravitino.optimizer.recommender.util.QLExpressionEvaluator;
import org.apache.gravitino.optimizer.recommender.util.StatsUtils;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.stats.PartitionStatistics;

@SuppressWarnings("UnusedVariable")
public class CompactionPolicyActor
    implements PolicyActor,
        PolicyActor.requirePartitionStats,
        PolicyActor.requireTableStats,
        PolicyActor.requireTableMetadata {
  private ExpressionEvaluator expressionEvaluator;
  private Policy policy;
  private List<SingleStatistic> tableStats;
  private List<PartitionStatistics> partitionStats;
  private Table tableMetadata;
  private NameIdentifier nameIdentifier;

  @Override
  public void initialize(NameIdentifier nameIdentifier, Policy policy) {
    this.nameIdentifier = nameIdentifier;
    this.policy = policy;
    this.expressionEvaluator = new QLExpressionEvaluator();
  }

  @Override
  public String policyType() {
    return "compaction";
  }

  @Override
  public long score() {
    if (!isPartitioned()) {
      Map<String, Object> context = buildExpressionContext(policy, tableStats);
      String scoreExpression = PolicyUtils.getScoreExpression(policy);
      return expressionEvaluator.evaluateLong(scoreExpression, context);
    }
    // todo choose the partitions with the largest datafile size
    return 0;
  }

  @Override
  public boolean shouldTrigger() {
    // check whether the data size mse > xx
    return shouldTriggerCompaction(policy, tableStats, expressionEvaluator);
  }

  @Override
  public JobExecuteContext jobConfig() {
    if (!isPartitioned()) {
      return getJobConfigFromPolicy(nameIdentifier, policy, tableMetadata);
    }
    // todo : choose the partitions with the topN datafile_msg size
    // for partition table, return the partition names additionally
    return null;
  }

  @Override
  public void setPartitionStats(List<PartitionStatistics> partitionStats) {
    this.partitionStats = partitionStats;
  }

  @Override
  public void setTableMetadata(Table tableMetadata) {
    this.tableMetadata = tableMetadata;
  }

  @Override
  public void setTableStats(List<SingleStatistic> tableStats) {
    this.tableStats = tableStats;
  }

  @VisibleForTesting
  static boolean shouldTriggerCompaction(
      Policy policy, List<SingleStatistic> stats, ExpressionEvaluator evaluator) {
    String triggerExpression = PolicyUtils.getTriggerExpression(policy);
    Map<String, Object> context = buildExpressionContext(policy, stats);
    return evaluator.evaluateBool(triggerExpression, context);
  }

  private boolean isPartitioned() {
    return tableMetadata.partitioning().length > 0;
  }

  @SuppressWarnings("EmptyCatch")
  private static Map<String, Object> buildExpressionContext(
      Policy policy, List<SingleStatistic> stats) {
    Map<String, Object> context = new HashMap<>();
    context.putAll(StatsUtils.buildStatsContext(stats));
    policy
        .content()
        // Todo: use rules not properties after https://github.com/apache/gravitino/issues/8863 is
        // merged
        .properties()
        .forEach(
            (k, v) -> {
              try {
                context.put(k, Long.parseLong(v));
              } catch (Exception e) {
              }
            });
    return context;
  }

  @VisibleForTesting
  static JobExecuteContext getJobConfigFromPolicy(
      NameIdentifier nameIdentifier, Policy policy, Table tableMetadata) {
    Map<String, Object> jobConfig = PolicyUtils.getJobConfigFromPolicy(policy);
    return new CompactionJobContext(nameIdentifier, jobConfig, policy, tableMetadata);
  }
}
