package org.apache.gravitino.recommender;

import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.recommender.api.JobSubmitter;
import org.apache.gravitino.recommender.api.PolicyActor;
import org.apache.gravitino.recommender.api.PolicyActor.JobExecuteContext;
import org.apache.gravitino.recommender.api.PolicyProvider;
import org.apache.gravitino.recommender.api.TableMetadataProvider;
import org.apache.gravitino.recommender.api.TableStatsProvider;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.stats.PartitionStatistics;
import org.apache.gravitino.stats.Statistic;

@SuppressWarnings("UnusedVariable")
public class Recommender {

  private PolicyProvider policyProvider;
  private TableStatsProvider statsProvider;
  private TableMetadataProvider tableMetadataProvider;
  private JobSubmitter jobSubmitter;

  public void recommendForOnePolicy(List<NameIdentifier> tableIdentifiers, String policyName) {
    Policy policy = policyProvider.getPolicy(policyName);

    // Set table metadata and stats
    PriorityQueue<PolicyActor> scoreQueue =
        new PriorityQueue<>((a, b) -> Long.compare(b.score(), a.score()));
    for (NameIdentifier tableIdentifier : tableIdentifiers) {
      PolicyActor policyActor = loadPolicyActor(policy, tableIdentifier);

      if (policyActor.shouldTrigger() == false) {
        continue;
      }
      scoreQueue.add(policyActor);
    }

    for (int i = 0; i < 10 && !scoreQueue.isEmpty(); i++) {
      PolicyActor actor = scoreQueue.poll();
      JobExecuteContext jobConfig = actor.jobConfig();
      jobSubmitter.submitJob(actor.policyType(), jobConfig);
    }
  }

  public void recommendForPolicyType(List<NameIdentifier> tableIdentifiers, String policyType) {
    List<String> policyNames = getPolicyNames(tableIdentifiers, policyType);
    for (String policyName : policyNames) {
      recommendForOnePolicy(tableIdentifiers, policyName);
    }
  }

  private PolicyActor loadPolicyActor(Policy policy, NameIdentifier tableIdentifier) {
    PolicyActor policyActor = getPolicyActor(policy.policyType());

    policyActor.initialize(tableIdentifier, policy);

    if (policyActor instanceof PolicyActor.requireTableMetadata) {
      Table tableMetadata = tableMetadataProvider.getTableMetadata(tableIdentifier);
      ((PolicyActor.requireTableMetadata) policyActor).setTableMetadata(tableMetadata);
    }

    if (policyActor instanceof PolicyActor.requireTableStats) {
      List<Statistic> tableStats = statsProvider.getTableStats(tableIdentifier);
      ((PolicyActor.requireTableStats) policyActor).setTableStats(tableStats);
      List<PartitionStatistics> partitionStats = statsProvider.getPartitionStats(tableIdentifier);
      ((PolicyActor.requirePartitionStats) policyActor).setPartitionStats(partitionStats);
    }

    return policyActor;
  }

  private PolicyActor getPolicyActor(String policyType) {
    return null;
  }

  private List<String> getPolicyNames(List<NameIdentifier> tableIdentifiers, String policyType) {
    // get policy names from policy provider
    // return policyProvider.getTablePolicy(tableIdentifier);
    // rewrite the code to flat the policy list to map the policy name to policy type
    Set<String> policyNames = new HashSet<>();
    for (NameIdentifier tableIdentifier : tableIdentifiers) {
      policyNames.addAll(
          policyProvider.getTablePolicy(tableIdentifier).stream()
              .filter(policy -> policy.policyType().equals(policyType))
              .map(Policy::name)
              .collect(Collectors.toList()));
    }
    return policyNames.stream().toList();
  }
}
