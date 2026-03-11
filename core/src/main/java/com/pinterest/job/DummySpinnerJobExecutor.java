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

package com.pinterest.job;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.gravitino.connector.job.JobExecutor;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.JobTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummySpinnerJobExecutor implements JobExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(DummySpinnerJobExecutor.class);

  private final Map<String, JobHandle.Status> jobStatusMap = new ConcurrentHashMap<>();

  @Override
  public void initialize(Map<String, String> configs) {
    LOG.info("Initializing DummySpinnerJobExecutor with configs: {}", configs);
  }

  @Override
  public String submitJob(JobTemplate jobTemplate) {
    SpinnerJobBuilder jobBuilder = SpinnerJobBuilderFactory.getJobBuilder(jobTemplate.name());
    String dagId = jobBuilder.getDagId();
    Map<String, Object> configs = jobBuilder.getJobConfig(jobTemplate);

    String runId = "dummy_run_" + UUID.randomUUID();
    String jobExecutionId = dagId + "::" + runId;

    LOG.info(
        "DummySpinnerJobExecutor: Simulating job submission - dagId: {}, runId: {}, configs: {}",
        dagId,
        runId,
        configs);

    jobStatusMap.put(jobExecutionId, JobHandle.Status.SUCCEEDED);

    return jobExecutionId;
  }

  @Override
  public JobHandle.Status getJobStatus(String jobId) throws NoSuchJobException {
    LOG.info("DummySpinnerJobExecutor: Getting job status for jobId: {}", jobId);

    JobHandle.Status status = jobStatusMap.get(jobId);
    if (status == null) {
      throw new NoSuchJobException("Job not found: %s", jobId);
    }

    return status;
  }

  @Override
  public void cancelJob(String jobId) throws NoSuchJobException {
    LOG.info("DummySpinnerJobExecutor: Cancelling job with jobId: {}", jobId);

    if (!jobStatusMap.containsKey(jobId)) {
      throw new NoSuchJobException("Job not found: %s", jobId);
    }

    jobStatusMap.put(jobId, JobHandle.Status.FAILED);
  }

  @Override
  public void close() throws IOException {
    LOG.info("DummySpinnerJobExecutor: Closing executor");
    jobStatusMap.clear();
  }
}
