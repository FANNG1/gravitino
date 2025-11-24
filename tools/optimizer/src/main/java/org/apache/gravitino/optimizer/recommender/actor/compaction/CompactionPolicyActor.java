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

package org.apache.gravitino.optimizer.recommender.actor.compaction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.optimizer.api.common.PartitionStatistic;
import org.apache.gravitino.optimizer.api.common.RecommenderPolicy;
import org.apache.gravitino.optimizer.api.common.SingleStatistic;
import org.apache.gravitino.optimizer.api.recommender.PolicyActor;
import org.apache.gravitino.optimizer.api.recommender.PolicyActorContext;
import org.apache.gravitino.optimizer.recommender.util.ExpressionEvaluator;
import org.apache.gravitino.optimizer.recommender.util.PolicyUtils;
import org.apache.gravitino.optimizer.recommender.util.QLExpressionEvaluator;
import org.apache.gravitino.optimizer.recommender.util.StatsUtils;
import org.apache.gravitino.rel.Table;

@SuppressWarnings("UnusedVariable")
public class CompactionPolicyActor implements PolicyActor {
  private ExpressionEvaluator expressionEvaluator;
  private RecommenderPolicy policy;
  private List<SingleStatistic<?>> tableStats;
  private List<PartitionStatistic> partitionStats;
  private Table tableMetadata;
  private NameIdentifier nameIdentifier;

  @Override
  public Set<DataRequirement> requiredData() {
    return EnumSet.of(
        DataRequirement.TABLE_METADATA,
        DataRequirement.TABLE_STATISTICS,
        DataRequirement.PARTITION_STATISTICS);
  }

  @Override
  public void initialize(PolicyActorContext context) {
    this.nameIdentifier = context.identifier();
    this.policy = context.policy();
    this.tableMetadata = context.tableMetadata().orElse(null);
    this.tableStats = context.tableStatistics();
    this.partitionStats = context.partitionStatistics();
    this.expressionEvaluator = new QLExpressionEvaluator();
  }

  @Override
  public String policyType() {
    return PolicyUtils.COMPACTION_POLICY_TYPE;
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

  @VisibleForTesting
  static boolean shouldTriggerCompaction(
      RecommenderPolicy policy, List<SingleStatistic<?>> stats, ExpressionEvaluator evaluator) {
    String triggerExpression = PolicyUtils.getTriggerExpression(policy);
    Map<String, Object> context = buildExpressionContext(policy, stats);
    return evaluator.evaluateBool(triggerExpression, context);
  }

  private boolean isPartitioned() {
    Preconditions.checkState(tableMetadata != null, "Table metadata must be provided");
    return tableMetadata.partitioning().length > 0;
  }

  @SuppressWarnings("EmptyCatch")
  private static Map<String, Object> buildExpressionContext(
      RecommenderPolicy policy, List<SingleStatistic<?>> stats) {
    Map<String, Object> context = new HashMap<>();
    context.putAll(StatsUtils.buildStatsContext(stats));
    policy
        .content()
        .rules()
        .forEach(
            (k, v) -> {
              try {
                context.put(k, Long.parseLong(v.toString()));
              } catch (Exception e) {
              }
            });
    return context;
  }

  @VisibleForTesting
  static JobExecuteContext getJobConfigFromPolicy(
      NameIdentifier nameIdentifier, RecommenderPolicy policy, Table tableMetadata) {
    Map<String, Object> jobConfig = PolicyUtils.getJobConfigFromPolicy(policy);
    return new CompactionJobContext(nameIdentifier, jobConfig, policy, tableMetadata);
  }
}
