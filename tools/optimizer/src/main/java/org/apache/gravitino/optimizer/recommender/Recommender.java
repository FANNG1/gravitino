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

package org.apache.gravitino.optimizer.recommender;

import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.optimizer.api.common.PartitionStatistic;
import org.apache.gravitino.optimizer.api.common.SingleStatistic;
import org.apache.gravitino.optimizer.api.common.policy.RecommenderPolicy;
import org.apache.gravitino.optimizer.api.recommender.JobSubmitter;
import org.apache.gravitino.optimizer.api.recommender.PolicyActor;
import org.apache.gravitino.optimizer.api.recommender.PolicyActor.JobExecuteContext;
import org.apache.gravitino.optimizer.api.recommender.PolicyProvider;
import org.apache.gravitino.optimizer.api.recommender.StatsProvider;
import org.apache.gravitino.optimizer.api.recommender.SupportTableStats;
import org.apache.gravitino.optimizer.api.recommender.TableMetadataProvider;
import org.apache.gravitino.optimizer.common.OptimizerEnv;
import org.apache.gravitino.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.optimizer.common.util.ProviderUtils;
import org.apache.gravitino.optimizer.recommender.actor.CompactionPolicyActor;
import org.apache.gravitino.optimizer.recommender.impl.GravitinoTableMetadataProvider;
import org.apache.gravitino.optimizer.recommender.job.NoopJobSubmitter;
import org.apache.gravitino.rel.Table;

@SuppressWarnings("UnusedVariable")
public class Recommender {

  private OptimizerEnv optimizerEnv;
  private PolicyProvider policyProvider;
  private StatsProvider statsProvider;
  private TableMetadataProvider tableMetadataProvider;
  private JobSubmitter jobSubmitter;

  public Recommender(OptimizerConfig config) {
    this.optimizerEnv = OptimizerEnv.getInstance();
    optimizerEnv.initialize(config);

    this.policyProvider = loadPolicyProvider(config);
    policyProvider.initialize(optimizerEnv);
    this.statsProvider = loadStatsProvider(config);
    statsProvider.initialize(optimizerEnv);
    this.tableMetadataProvider = loadTableMetadataProvider();
    tableMetadataProvider.initialize(optimizerEnv);
    this.jobSubmitter = loadJobSubmitter();
  }

  public List<JobExecuteContext> recommendForOnePolicy(
      List<NameIdentifier> tableIdentifiers, String policyName) {
    RecommenderPolicy policy = policyProvider.getPolicy(policyName);

    PriorityQueue<PolicyActor> scoreQueue =
        new PriorityQueue<>((a, b) -> Long.compare(b.score(), a.score()));
    for (NameIdentifier tableIdentifier : tableIdentifiers) {
      PolicyActor policyActor = loadPolicyActor(policy, tableIdentifier);

      if (policyActor.shouldTrigger() == false) {
        continue;
      }
      scoreQueue.add(policyActor);
    }

    List<JobExecuteContext> jobConfigs =
        scoreQueue.stream().map(PolicyActor::jobConfig).collect(Collectors.toList());

    return jobConfigs;
  }

  public void recommendForPolicyType(List<NameIdentifier> tableIdentifiers, String policyType) {
    List<String> policyNames = getPolicyNames(tableIdentifiers, policyType);
    for (String policyName : policyNames) {
      List<JobExecuteContext> jobConfigs = recommendForOnePolicy(tableIdentifiers, policyName);
      for (JobExecuteContext jobConfig : jobConfigs) {
        jobSubmitter.submitJob(policyType, jobConfig);
      }
    }
  }

  private PolicyActor loadPolicyActor(RecommenderPolicy policy, NameIdentifier tableIdentifier) {
    PolicyActor policyActor = getPolicyActor(policy.policyType());

    policyActor.initialize(tableIdentifier, policy);

    if (policyActor instanceof PolicyActor.requireTableMetadata) {
      Table tableMetadata = tableMetadataProvider.getTableMetadata(tableIdentifier);
      ((PolicyActor.requireTableMetadata) policyActor).setTableMetadata(tableMetadata);
    }

    if (policyActor instanceof PolicyActor.requireTableStats) {
      List<SingleStatistic> tableStats =
          ((SupportTableStats) statsProvider).getTableStats(tableIdentifier);
      ((PolicyActor.requireTableStats) policyActor).setTableStats(tableStats);
      List<PartitionStatistic> partitionStats =
          ((SupportTableStats) statsProvider).getPartitionStats(tableIdentifier);
      ((PolicyActor.requirePartitionStats) policyActor).setPartitionStats(partitionStats);
    }

    return policyActor;
  }

  private PolicyActor getPolicyActor(String policyType) {
    return new CompactionPolicyActor();
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
              .map(RecommenderPolicy::name)
              .collect(Collectors.toList()));
    }
    return policyNames.stream().toList();
  }

  private PolicyProvider loadPolicyProvider(OptimizerConfig config) {
    String policyProviderName = config.get(OptimizerConfig.POLICY_PROVIDER_CONFIG);
    return ProviderUtils.createPolicyProviderInstance(policyProviderName);
  }

  private StatsProvider loadStatsProvider(OptimizerConfig config) {
    String statsProviderName = config.get(OptimizerConfig.STATS_PROVIDER_CONFIG);
    return ProviderUtils.createStatsProviderInstance(statsProviderName);
  }

  private TableMetadataProvider loadTableMetadataProvider() {
    return new GravitinoTableMetadataProvider();
  }

  private JobSubmitter loadJobSubmitter() {
    return new NoopJobSubmitter();
  }
}
