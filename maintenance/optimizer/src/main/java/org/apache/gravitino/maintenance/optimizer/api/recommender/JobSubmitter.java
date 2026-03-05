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

package org.apache.gravitino.maintenance.optimizer.api.recommender;

import java.util.List;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.exceptions.NoSuchJobTemplateException;
import org.apache.gravitino.maintenance.optimizer.api.common.Provider;

/** Submits recommended jobs to an execution backend. */
@DeveloperApi
public interface JobSubmitter extends Provider {

  /**
   * Submit a job for execution.
   *
   * @param jobTemplateName the job template name (routes to the appropriate job template)
   * @param jobExecutionContext execution context built by the strategy handler
   * @return provider-specific job id
   * @throws NoSuchJobTemplateException if the job template name does not exist
   */
  String submitJob(String jobTemplateName, JobExecutionContext jobExecutionContext)
      throws NoSuchJobTemplateException;

  /**
   * Checks whether this job submitter supports batch job submission.
   *
   * <p>Batch job submission allows multiple job execution contexts to be submitted together as a
   * single batch operation, which can improve efficiency and reduce overhead for certain execution
   * backends.
   *
   * @param jobTemplateName the job template name (routes to the appropriate job template)
   * @return true if batch job submission is supported, false otherwise
   */
  boolean supportsBatchJob(String jobTemplateName);

  /**
   * Submit multiple jobs as a batch for execution.
   *
   * <p>This method allows submitting multiple job execution contexts together, which can be more
   * efficient than submitting them individually. Implementations should only provide this method if
   * {@link #supportsBatchJob(String)} returns true.
   *
   * @param jobTemplateName the job template name (routes to the appropriate job template)
   * @param jobExecutionContexts list of execution contexts to be submitted as a batch
   * @return provider-specific batch job id
   * @throws NoSuchJobTemplateException if the job template name does not exist
   * @throws UnsupportedOperationException if batch job submission is not supported
   */
  String submitBatchJob(String jobTemplateName, List<JobExecutionContext> jobExecutionContexts)
      throws NoSuchJobTemplateException, UnsupportedOperationException;
}
