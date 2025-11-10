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

package org.apache.gravitino.optimizer.common.util;

import org.apache.gravitino.optimizer.api.recommender.JobSubmitter;
import org.apache.gravitino.optimizer.api.recommender.PolicyProvider;
import org.apache.gravitino.optimizer.api.recommender.StatsProvider;
import org.apache.gravitino.optimizer.api.recommender.TableMetadataProvider;
import org.apache.gravitino.optimizer.recommender.job.GravitinoJobSubmitter;
import org.apache.gravitino.optimizer.recommender.job.NoopJobSubmitter;
import org.apache.gravitino.optimizer.recommender.policy.GravitinoPolicyProvider;
import org.apache.gravitino.optimizer.recommender.stats.GravitinoStatsProvider;
import org.apache.gravitino.optimizer.recommender.table.GravitinoTableMetadataProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestProviderUtils {

  @Test
  public void testCreatePolicyProviderInstance() {
    PolicyProvider policyProvider =
        ProviderUtils.createPolicyProviderInstance(
            GravitinoPolicyProvider.GRAVITINO_POLICY_PROVIDER_NAME);
    Assertions.assertNotNull(policyProvider);
    Assertions.assertTrue(policyProvider instanceof GravitinoPolicyProvider);
  }

  @Test
  public void testCreateJobSubmitterInstance() {
    JobSubmitter jobSubmitter =
        ProviderUtils.createJobSubmitterInstance(
            GravitinoJobSubmitter.GRAVITINO_JOB_SUBMITTER_NAME);
    Assertions.assertNotNull(jobSubmitter);
    Assertions.assertTrue(jobSubmitter instanceof GravitinoJobSubmitter);

    jobSubmitter =
        ProviderUtils.createJobSubmitterInstance(NoopJobSubmitter.NOOP_JOB_SUBMITTER_NAME);
    Assertions.assertNotNull(jobSubmitter);
    Assertions.assertTrue(jobSubmitter instanceof NoopJobSubmitter);
  }

  @Test
  public void testCreateStatsProviderInstance() {
    StatsProvider statsProvider =
        ProviderUtils.createStatsProviderInstance(
            GravitinoStatsProvider.GRAVITINO_STATS_PROVIDER_NAME);
    Assertions.assertNotNull(statsProvider);
    Assertions.assertTrue(statsProvider instanceof GravitinoStatsProvider);
  }

  @Test
  public void testCreateTableMetadataProviderInstance() {
    TableMetadataProvider tableMetadataProvider =
        ProviderUtils.createTableMetadataProviderInstance(
            GravitinoTableMetadataProvider.GRAVITINO_TABLE_METADATA_PROVIDER_NAME);
    Assertions.assertNotNull(tableMetadataProvider);
    Assertions.assertTrue(tableMetadataProvider instanceof GravitinoTableMetadataProvider);
  }
}
