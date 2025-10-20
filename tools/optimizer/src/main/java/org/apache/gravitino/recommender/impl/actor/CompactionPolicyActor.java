package org.apache.gravitino.recommender.impl.actor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.recommender.api.PolicyActor;
import org.apache.gravitino.recommender.impl.util.ExpressionEvaluator;
import org.apache.gravitino.recommender.impl.util.PolicyUtiles;
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

  @Override
  public void setPolicy(Policy policy) {
    this.policy = policy;
  }

  @Override
  public String policyType() {
    return "compaction";
  }

  @Override
  // Todo using policy expression to compute the score
  public long score() {
    if (!isPartitioned()) {
      Map<String, Object> context = buildExpressionContext();
      String scoreExpression = PolicyUtiles.getScoreExpression(policy);
      return expressionEvaluator.evaluateLong(scoreExpression, context);
    }
    // todo choose the partitions with the largest datafile size
    return 0;
  }

  @Override
  public boolean shouldTrigger() {
    // Todo using policy trigger expression to compute the trigger.
    // check whether the data size mse > xx
    String triggerExpression = PolicyUtiles.getTriggerExpression(policy);
    Map<String, Object> context = buildExpressionContext();
    return expressionEvaluator.evaluateBool(triggerExpression, context);
  }

  @Override
  public JobConfig jobConfig() {
    // for non partition table, return the properties like table name, target-filesize
    if (!isPartitioned()) {
      return null;
    }
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

  private boolean isPartitioned() {
    return tableMetadata.partitioning().length > 0;
  }

  private Map<String, Object> buildExpressionContext() {
    Map<String, Object> context = new HashMap<>();
    context.putAll(StatsUtils.buildTableStatsContext(tableStats));
    context.putAll(policy.content().properties());
    return context;
  }
}
