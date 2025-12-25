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

package org.apache.gravitino.maintenance.optimizer.recommender.job;

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobSubmitter;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;

public class GravitinoJobSubmitter implements JobSubmitter {

  public static final String NAME = "gravitino-job-submitter";

  private GravitinoClient gravitinoClient;

  private final Map<String, Class<? extends GravitinoJobAdapter>> jobAdapters = Map.of();

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    String uri = optimizerEnv.config().get(OptimizerConfig.GRAVITINO_URI_CONFIG);
    String metalake = optimizerEnv.config().get(OptimizerConfig.GRAVITINO_METALAKE_CONFIG);
    this.gravitinoClient = GravitinoClient.builder(uri).withMetalake(metalake).build();
  }

  @Override
  public String submitJob(String jobTemplateName, JobExecutionContext jobExecutionContext) {
    GravitinoJobAdapter jobAdapter = loadJobAdapter(jobTemplateName, jobExecutionContext);
    if (jobAdapter == null) {
      throw new IllegalArgumentException("No job adapter found for template: " + jobTemplateName);
    }
    return gravitinoClient.runJob(jobAdapter.jobTemplateName(), jobAdapter.jobConfig()).jobId();
  }

  @Override
  public void close() {
    if (gravitinoClient != null) {
      gravitinoClient.close();
    }
  }

  @VisibleForTesting
  GravitinoJobAdapter loadJobAdapter(
      String jobTemplateName, JobExecutionContext jobExecutionContext) {
    Class<? extends GravitinoJobAdapter> jobAdapterClz = jobAdapters.get(jobTemplateName);
    if (jobAdapterClz == null) {
      return null;
    }
    try {
      GravitinoJobAdapter jobAdapter = jobAdapterClz.getDeclaredConstructor().newInstance();
      jobAdapter.initialize(jobExecutionContext);
      return jobAdapter;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to create job adapter for template: " + jobTemplateName, e);
    }
  }
}
