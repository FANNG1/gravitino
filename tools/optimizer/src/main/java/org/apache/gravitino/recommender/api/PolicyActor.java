package org.apache.gravitino.recommender.api;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.stats.PartitionStatistics;
import org.apache.gravitino.stats.Statistic;

public interface PolicyActor {
  interface requireTableMetadata {
    void setTableMetadata(Table tableMetadata);
  }

  interface requireTableStats {
    void setTableStats(List<Statistic> tableStats);
  }

  interface requirePartitionStats {
    void setPartitionStats(List<PartitionStatistics> partitionStats);
  }

  interface JobExecuteContext {
    NameIdentifier name();

    Map<String, Object> config();

    Policy policy();
  }

  void initialize(NameIdentifier nameIdentifier, Policy policy);

  String policyType();

  long score();
  // Whether to trigger a job to run the policy
  boolean shouldTrigger();
  // The job configuration to run the policy
  JobExecuteContext jobConfig();
}
