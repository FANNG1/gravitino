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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.optimizer.api.common.RecommenderPolicy;
import org.apache.gravitino.optimizer.api.recommender.JobSubmitter;
import org.apache.gravitino.optimizer.api.recommender.PolicyActor;
import org.apache.gravitino.optimizer.api.recommender.PolicyActor.JobExecuteContext;
import org.apache.gravitino.optimizer.api.recommender.PolicyActorContext;
import org.apache.gravitino.optimizer.api.recommender.PolicyProvider;
import org.apache.gravitino.optimizer.api.recommender.StatsProvider;
import org.apache.gravitino.optimizer.api.recommender.TableMetadataProvider;
import org.apache.gravitino.rel.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestRecommenderPolicyFiltering {

  @Test
  void recommendOnlyTablesWithPolicy() {
    NameIdentifier tableWithPolicy = NameIdentifier.of("db", "t_with_policy");
    NameIdentifier tableWithoutPolicy = NameIdentifier.of("db", "t_without_policy");

    StubRecommenderPolicy compactionPolicy =
        new StubRecommenderPolicy("policy-1", "COMPACTION", new HashMap<>());
    PolicyProvider policyProvider =
        new StubPolicyProvider(
            Map.of(
                tableWithPolicy, List.of(compactionPolicy),
                tableWithoutPolicy, List.of()),
            Map.of(compactionPolicy.name(), compactionPolicy));

    RecordingJobSubmitter jobSubmitter = new RecordingJobSubmitter();
    Recommender recommender =
        new StubRecommender(
            policyProvider, new NoopStatsProvider(), new NoopTableMetadataProvider(), jobSubmitter);

    recommender.recommendForPolicyType(
        List.of(tableWithPolicy, tableWithoutPolicy), compactionPolicy.policyType());

    Assertions.assertEquals(
        1, jobSubmitter.submitted.size(), "Only table with policy should be submitted");
    Assertions.assertEquals(
        tableWithPolicy, jobSubmitter.submitted.get(0).identifier(), "Wrong table submitted");
  }

  private static final class StubRecommender extends Recommender {
    StubRecommender(
        PolicyProvider policyProvider,
        StatsProvider statsProvider,
        TableMetadataProvider tableMetadataProvider,
        JobSubmitter jobSubmitter) {
      super(policyProvider, statsProvider, tableMetadataProvider, jobSubmitter);
    }

    @Override
    protected PolicyActor createPolicyActor(String policyType) {
      return new StubPolicyActor();
    }
  }

  private static final class StubPolicyActor implements PolicyActor {
    private PolicyActorContext context;

    @Override
    public void initialize(PolicyActorContext context) {
      this.context = context;
    }

    @Override
    public String policyType() {
      return "COMPACTION";
    }

    @Override
    public long score() {
      return 1;
    }

    @Override
    public boolean shouldTrigger() {
      return true;
    }

    @Override
    public JobExecuteContext jobConfig() {
      return new JobExecuteContext() {
        @Override
        public NameIdentifier identifier() {
          return context.identifier();
        }

        @Override
        public Map<String, Object> config() {
          return Map.of();
        }

        @Override
        public RecommenderPolicy policy() {
          return context.policy();
        }
      };
    }
  }

  private static final class StubRecommenderPolicy implements RecommenderPolicy {
    private final String name;
    private final String policyType;
    private final Map<String, Object> content;

    StubRecommenderPolicy(String name, String policyType, Map<String, Object> content) {
      this.name = name;
      this.policyType = policyType;
      this.content = content;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String policyType() {
      return policyType;
    }

    @Override
    public org.apache.gravitino.policy.PolicyContent content() {
      return org.apache.gravitino.policy.PolicyContents.custom(
          content, java.util.Set.of(), Map.of());
    }

    @Override
    public Optional<String> jobTemplateName() {
      return Optional.empty();
    }
  }

  private static final class RecordingJobSubmitter implements JobSubmitter {
    private final List<JobExecuteContext> submitted = new ArrayList<>();

    @Override
    public String name() {
      return "job-submit-recorder";
    }

    @Override
    public void initialize(org.apache.gravitino.optimizer.common.OptimizerEnv optimizerEnv) {}

    @Override
    public String submitJob(String policyType, JobExecuteContext job) {
      submitted.add(job);
      return "job-" + submitted.size();
    }
  }

  private static final class NoopStatsProvider implements StatsProvider {
    @Override
    public String name() {
      return "noop-stats";
    }

    @Override
    public void initialize(org.apache.gravitino.optimizer.common.OptimizerEnv optimizerEnv) {}
  }

  private static final class NoopTableMetadataProvider implements TableMetadataProvider {
    @Override
    public String name() {
      return "noop-metadata";
    }

    @Override
    public void initialize(org.apache.gravitino.optimizer.common.OptimizerEnv optimizerEnv) {}

    @Override
    public Table getTableMetadata(NameIdentifier tableIdentifier) {
      return null;
    }
  }

  private static final class StubPolicyProvider implements PolicyProvider {
    private final Map<NameIdentifier, List<RecommenderPolicy>> tablePolicies;
    private final Map<String, RecommenderPolicy> policiesByName;

    StubPolicyProvider(
        Map<NameIdentifier, List<RecommenderPolicy>> tablePolicies,
        Map<String, RecommenderPolicy> policiesByName) {
      this.tablePolicies = tablePolicies;
      this.policiesByName = policiesByName;
    }

    @Override
    public String name() {
      return "stub-policy-provider";
    }

    @Override
    public void initialize(org.apache.gravitino.optimizer.common.OptimizerEnv optimizerEnv) {}

    @Override
    public List<RecommenderPolicy> getTablePolicy(NameIdentifier tableIdentifier) {
      return tablePolicies.getOrDefault(tableIdentifier, List.of());
    }

    @Override
    public RecommenderPolicy getPolicy(String policyName) {
      return policiesByName.get(policyName);
    }
  }
}
