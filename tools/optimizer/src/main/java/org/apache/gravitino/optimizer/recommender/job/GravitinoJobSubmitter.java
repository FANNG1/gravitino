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

package org.apache.gravitino.optimizer.recommender.job;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.optimizer.api.recommender.JobSubmitter;
import org.apache.gravitino.optimizer.api.recommender.PolicyActor.JobExecuteContext;
import org.apache.gravitino.optimizer.common.OptimizerEnv;
import org.apache.gravitino.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.optimizer.recommender.util.PolicyUtils;

@SuppressWarnings("unused")
public class GravitinoJobSubmitter implements JobSubmitter {

  public static final String GRAVITINO_JOB_SUBMITTER_NAME = "gravitino-job-submitter";

  private GravitinoClient gravitinoClient;

  private Map<String, Class<? extends GravitinoJobAdapter>> jobAdapters =
      ImmutableMap.of(PolicyUtils.COMPACTION_POLICY_TYPE, GravitinoCompactionJobAdapter.class);

  @Override
  public String name() {
    return GRAVITINO_JOB_SUBMITTER_NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    String uri = optimizerEnv.config().get(OptimizerConfig.GRAVITINO_URI_CONFIG);
    String metalake = optimizerEnv.config().get(OptimizerConfig.GRAVITINO_METALAKE_CONFIG);
    this.gravitinoClient = GravitinoClient.builder(uri).withMetalake(metalake).build();
    // this.defaultCatalogName =
    // optimizerEnv.config().get(OptimizerConfig.GRAVITINO_DEFAULT_CATALOG_CONFIG);
  }

  @Override
  public String submitJob(String policyType, JobExecuteContext job) {
    GravitinoJobAdapter jobAdapter = loadJobAdapter(policyType, job);
    if (jobAdapter == null) {
      throw new IllegalArgumentException("No job adapter found for policy type: " + policyType);
    }
    return gravitinoClient.runJob(jobAdapter.jobTemplateName(), jobAdapter.jobConfig()).jobId();
  }

  @VisibleForTesting
  GravitinoJobAdapter loadJobAdapter(String policyType, JobExecuteContext jobExecuteContext) {
    Class<? extends GravitinoJobAdapter> jobAdapterClz = jobAdapters.get(policyType);
    if (jobAdapterClz == null) {
      return null;
    }
    try {
      GravitinoJobAdapter jobAdapter = jobAdapterClz.getDeclaredConstructor().newInstance();
      jobAdapter.initialize(jobExecuteContext);
      return jobAdapter;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to create job adapter for policy type: " + policyType, e);
    }
  }
}
