package org.apache.gravitino.recommender.impl.actor;

import java.util.List;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.recommender.api.PolicyActor;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.stats.PartitionStatistics;
import org.apache.gravitino.stats.Statistic;

@SuppressWarnings("UnusedVariable")
public class CompactionPolicyActor
    implements PolicyActor,
        PolicyActor.requirePartitionStats,
        PolicyActor.requireTableStats,
        PolicyActor.requireTableMetadata {
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
  public int score() {
    if (!isPartitioned()) {
      // return datafile size mse from table stats
      return 0;
    }
    // choose the partitions with the largest datafile size
    return 0;
  }

  @Override
  public boolean shouldTrigger() {
    // Todo using policy trigger expression to compute the trigger.
    // check whether the data size mse > xx
    return false;
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
}
