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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.Audit;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.optimizer.api.common.SingleStatistic;
import org.apache.gravitino.optimizer.api.recommender.PolicyActor.JobExecuteContext;
import org.apache.gravitino.optimizer.recommender.util.ExpressionEvaluator;
import org.apache.gravitino.optimizer.recommender.util.QLExpressionEvaluator;
import org.apache.gravitino.optimizer.updater.impl.SingleStatisticImpl;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.stats.Statistic;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

class TestCompactionPolicyActor {

  static class Stats implements Statistic {
    private final String name;
    private final long value;

    public Stats(String name, long value) {
      this.name = name;
      this.value = value;
    }

    @Override
    public Audit auditInfo() {
      return null;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Optional<StatisticValue<?>> value() {
      return Optional.of(StatisticValues.longValue(value));
    }

    @Override
    public boolean reserved() {
      return false;
    }

    @Override
    public boolean modifiable() {
      return false;
    }
  }

  private Policy policy = new CompactionPolicyForTest();

  private ExpressionEvaluator evaluator = new QLExpressionEvaluator();

  @Test
  void testShouldTriggerCompaction() {
    List<SingleStatistic> stats =
        Arrays.asList(new SingleStatisticImpl("datafile_mse", StatisticValues.longValue(2000L)));
    Assertions.assertEquals(
        true, CompactionPolicyActor.shouldTriggerCompaction(policy, stats, evaluator));

    stats = Arrays.asList(new SingleStatisticImpl("datafile_mse", StatisticValues.longValue(10L)));
    Assertions.assertEquals(
        false, CompactionPolicyActor.shouldTriggerCompaction(policy, stats, evaluator));

    // todo: handle not exists statistic
    // CompactionPolicyActor.shouldTriggerCompaction(policy, Arrays.asList(), evaluator);
  }

  @Test
  void testJobConfig() {
    NameIdentifier tableId = NameIdentifier.of("db", "table");
    Table tableMetadata = Mockito.mock(Table.class);
    JobExecuteContext config =
        CompactionPolicyActor.getJobConfigFromPolicy(tableId, policy, tableMetadata);
    Assertions.assertTrue(config instanceof CompactionJobContext);
    CompactionJobContext compactionConfig = (CompactionJobContext) config;
    Assertions.assertEquals(Optional.of(1024L), compactionConfig.targetFileSize());
    Assertions.assertEquals(tableId, compactionConfig.name());
    Assertions.assertEquals(
        ImmutableMap.of(RewriteDataFiles.TARGET_FILE_SIZE_BYTES, 1024L), compactionConfig.config());
    Assertions.assertEquals(Optional.empty(), compactionConfig.partitionNames());
  }
}
