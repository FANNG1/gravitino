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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.connector.job.JobExecutor;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.JobTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpinnerJobExecutor implements JobExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(SpinnerJobExecutor.class);

  @VisibleForTesting SpinnerJobClient spinnerClient;

  @Override
  public void initialize(Map<String, String> configs) {
    this.spinnerClient = new SpinnerJobClient(configs);
  }

  @Override
  public String submitJob(JobTemplate jobTemplate) {
    SpinnerJobBuilder jobBuilder = SpinnerJobBuilderFactory.getJobBuilder(jobTemplate.name());
    String dagId = jobBuilder.getDagId();
    Map<String, Object> configs = jobBuilder.getJobConfig(jobTemplate);
    try {
      DagRun dagRun = spinnerClient.submitDagRun(dagId, configs);
      String jobExecutionId = serializeJobId(dagRun);
      LOG.info("Submitted Spinner job: {}", jobExecutionId);
      return jobExecutionId;
    } catch (Exception e) {
      throw new RuntimeException("Failed to submit job", e);
    }
  }

  /** Serializes a DagRun into a job ID string in the format "dagId::executionDateTime". */
  private String serializeJobId(DagRun dagRun) {
    return dagRun.getDagId() + "::" + dagRun.getExecutionDateTime();
  }

  private String[] deserializeJobId(String jobId) throws NoSuchJobException {
    String[] parts = jobId.split("::", 2);
    if (parts.length != 2) {
      throw new NoSuchJobException("Invalid job ID format: %s", jobId);
    }
    return parts;
  }

  @Override
  public JobHandle.Status getJobStatus(String jobId) throws NoSuchJobException {
    String[] parts = deserializeJobId(jobId);
    String dagId = parts[0];
    String executionDateTime = parts[1];
    try {
      DagRunState state = spinnerClient.getDagRunState(dagId, executionDateTime);
      return SpinnerUtils.convertDagRunState(state);
    } catch (IOException e) {
      throw new RuntimeException("Failed to get job status for job ID: " + jobId, e);
    }
  }

  @Override
  public void cancelJob(String jobId) throws NoSuchJobException {
    String[] parts = deserializeJobId(jobId);
    String dagId = parts[0];
    String executionDateTime = parts[1];
    try {
      spinnerClient.cancelDagRun(dagId, executionDateTime);
      LOG.info("Cancelling Spinner job: {}", jobId);
    } catch (IOException e) {
      throw new RuntimeException("Failed to cancel job for job ID: " + jobId, e);
    }
  }

  @Override
  public void close() throws IOException {
    if (spinnerClient != null) {
      spinnerClient.close();
    }
  }
}
