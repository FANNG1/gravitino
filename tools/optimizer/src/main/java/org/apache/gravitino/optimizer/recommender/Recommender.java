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

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.optimizer.api.common.PartitionStatistic;
import org.apache.gravitino.optimizer.api.common.RecommenderPolicy;
import org.apache.gravitino.optimizer.api.common.SingleStatistic;
import org.apache.gravitino.optimizer.api.recommender.JobSubmitter;
import org.apache.gravitino.optimizer.api.recommender.PolicyActor;
import org.apache.gravitino.optimizer.api.recommender.PolicyActor.DataRequirement;
import org.apache.gravitino.optimizer.api.recommender.PolicyActor.JobExecuteContext;
import org.apache.gravitino.optimizer.api.recommender.PolicyActorContext;
import org.apache.gravitino.optimizer.api.recommender.PolicyProvider;
import org.apache.gravitino.optimizer.api.recommender.StatsProvider;
import org.apache.gravitino.optimizer.api.recommender.SupportTableStats;
import org.apache.gravitino.optimizer.api.recommender.TableMetadataProvider;
import org.apache.gravitino.optimizer.common.OptimizerEnv;
import org.apache.gravitino.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.optimizer.common.util.InstanceLoaderUtils;
import org.apache.gravitino.optimizer.common.util.ProviderUtils;
import org.apache.gravitino.rel.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("UnusedVariable")
public class Recommender {
  private static final Logger LOG = LoggerFactory.getLogger(Recommender.class);
  private PolicyProvider policyProvider;
  private StatsProvider statsProvider;
  private TableMetadataProvider tableMetadataProvider;
  private JobSubmitter jobSubmitter;

  public Recommender(OptimizerEnv optimizerEnv) {
    OptimizerConfig config = optimizerEnv.config();

    this.policyProvider = loadPolicyProvider(config);
    policyProvider.initialize(optimizerEnv);
    this.statsProvider = loadStatsProvider(config);
    statsProvider.initialize(optimizerEnv);
    this.tableMetadataProvider = loadTableMetadataProvider(config);
    tableMetadataProvider.initialize(optimizerEnv);
    this.jobSubmitter = loadJobSubmitter(config);
    jobSubmitter.initialize(optimizerEnv);
  }

  public List<JobExecuteContext> recommendForOnePolicy(
      List<NameIdentifier> tableIdentifiers, String policyName) {
    LOG.info("Recommend policy {} for identifiers {}", policyName, tableIdentifiers);
    RecommenderPolicy policy = policyProvider.getPolicy(policyName);

    PriorityQueue<PolicyActor> scoreQueue =
        new PriorityQueue<>((a, b) -> Long.compare(b.score(), a.score()));
    for (NameIdentifier tableIdentifier : tableIdentifiers) {
      PolicyActor policyActor = loadPolicyActor(policy, tableIdentifier);
      if (policyActor.shouldTrigger() == false) {
        continue;
      }
      LOG.info(
          "Recommend policy {} for identifier {} score: {}",
          policyName,
          tableIdentifier,
          policyActor.score());
      scoreQueue.add(policyActor);
    }

    List<JobExecuteContext> jobConfigs =
        scoreQueue.stream().map(PolicyActor::jobConfig).collect(Collectors.toList());
    return jobConfigs;
  }

  public void recommendForPolicyType(List<NameIdentifier> nameIdentifiers, String policyType) {
    Map<String, List<NameIdentifier>> policiesByTable =
        getPolicyTables(nameIdentifiers, policyType);

    for (Map.Entry<String, List<NameIdentifier>> entry : policiesByTable.entrySet()) {
      String policyName = entry.getKey();
      List<JobExecuteContext> jobConfigs = recommendForOnePolicy(entry.getValue(), policyName);
      for (JobExecuteContext jobConfig : jobConfigs) {
        jobSubmitter.submitJob(policyType, jobConfig);
      }
    }
  }

  private PolicyActor loadPolicyActor(RecommenderPolicy policy, NameIdentifier tableIdentifier) {
    PolicyActor policyActor = getPolicyActor(policy.policyType());

    Set<DataRequirement> declaredRequirements = policyActor.requiredData();
    EnumSet<DataRequirement> requirements =
        declaredRequirements.isEmpty()
            ? EnumSet.noneOf(DataRequirement.class)
            : EnumSet.copyOf(declaredRequirements);
    Table tableMetadata = null;
    List<SingleStatistic<?>> tableStats = Collections.emptyList();
    List<PartitionStatistic> partitionStats = Collections.emptyList();

    if (requirements.contains(DataRequirement.TABLE_METADATA)) {
      tableMetadata = tableMetadataProvider.getTableMetadata(tableIdentifier);
    }

    if (requirements.contains(DataRequirement.TABLE_STATISTICS)
        || requirements.contains(DataRequirement.PARTITION_STATISTICS)) {
      SupportTableStats supportTableStats = requireTableStatsProvider();
      if (requirements.contains(DataRequirement.TABLE_STATISTICS)) {
        tableStats = supportTableStats.getTableStats(tableIdentifier);
      }
      if (requirements.contains(DataRequirement.PARTITION_STATISTICS)) {
        partitionStats = supportTableStats.getPartitionStats(tableIdentifier);
      }
    }

    PolicyActorContext context =
        PolicyActorContext.builder(tableIdentifier, policy)
            .withTableMetadata(tableMetadata)
            .withTableStatistics(tableStats)
            .withPartitionStatistics(partitionStats)
            .build();
    policyActor.initialize(context);

    return policyActor;
  }

  private PolicyActor getPolicyActor(String policyType) {
    return InstanceLoaderUtils.createActorInstance(policyType);
  }

  private Map<String, List<NameIdentifier>> getPolicyTables(
      List<NameIdentifier> tableIdentifiers, String policyType) {
    // LinkedHashMap preserves deterministic order for predictable job submission sequencing.
    Map<String, List<NameIdentifier>> tablesByPolicy = new LinkedHashMap<>();
    for (NameIdentifier tableIdentifier : tableIdentifiers) {
      policyProvider.getTablePolicy(tableIdentifier).stream()
          .filter(policy -> policy.policyType().equals(policyType))
          .forEach(
              policy ->
                  tablesByPolicy
                      .computeIfAbsent(policy.name(), key -> new ArrayList<>())
                      .add(tableIdentifier));
    }
    return tablesByPolicy;
  }

  private PolicyProvider loadPolicyProvider(OptimizerConfig config) {
    String policyProviderName = config.get(OptimizerConfig.POLICY_PROVIDER_CONFIG);
    return ProviderUtils.createPolicyProviderInstance(policyProviderName);
  }

  private StatsProvider loadStatsProvider(OptimizerConfig config) {
    String statsProviderName = config.get(OptimizerConfig.STATS_PROVIDER_CONFIG);
    return ProviderUtils.createStatsProviderInstance(statsProviderName);
  }

  private TableMetadataProvider loadTableMetadataProvider(OptimizerConfig config) {
    String tableMetadataProviderName = config.get(OptimizerConfig.TABLE_META_PROVIDER_CONFIG);
    return ProviderUtils.createTableMetadataProviderInstance(tableMetadataProviderName);
  }

  private JobSubmitter loadJobSubmitter(OptimizerConfig config) {
    String jobSubmitterName = config.get(OptimizerConfig.JOB_SUBMITTER_CONFIG);
    return ProviderUtils.createJobSubmitterInstance(jobSubmitterName);
  }

  private SupportTableStats requireTableStatsProvider() {
    if (statsProvider instanceof SupportTableStats) {
      return (SupportTableStats) statsProvider;
    }
    throw new IllegalStateException(
        String.format(
            "Stats provider %s does not support table or partition statistics",
            statsProvider.name()));
  }
}
