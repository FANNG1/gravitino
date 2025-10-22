package org.apache.gravitino.recommender.impl.actor;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.recommender.api.PolicyActor;
import org.apache.gravitino.recommender.impl.util.ExpressionEvaluator;
import org.apache.gravitino.recommender.impl.util.PolicyUtils;
import org.apache.gravitino.recommender.impl.util.QLExpressionEvaluator;
import org.apache.gravitino.recommender.impl.util.StatsUtils;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.stats.PartitionStatistics;
import org.apache.gravitino.stats.Statistic;

@SuppressWarnings("UnusedVariable")
public class CompactionPolicyActor
    implements PolicyActor,
        PolicyActor.requirePartitionStats,
        PolicyActor.requireTableStats,
        PolicyActor.requireTableMetadata {
  private ExpressionEvaluator expressionEvaluator;
  private Policy policy;
  private List<Statistic> tableStats;
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
  public void setTableStats(List<Statistic> tableStats) {
    this.tableStats = tableStats;
  }

  @VisibleForTesting
  static boolean shouldTriggerCompaction(
      Policy policy, List<Statistic> stats, ExpressionEvaluator evaluator) {
    String triggerExpression = PolicyUtils.getTriggerExpression(policy);
    Map<String, Object> context = buildExpressionContext(policy, stats);
    return evaluator.evaluateBool(triggerExpression, context);
  }

  private boolean isPartitioned() {
    return tableMetadata.partitioning().length > 0;
  }

  @SuppressWarnings("EmptyCatch")
  private static Map<String, Object> buildExpressionContext(Policy policy, List<Statistic> stats) {
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
