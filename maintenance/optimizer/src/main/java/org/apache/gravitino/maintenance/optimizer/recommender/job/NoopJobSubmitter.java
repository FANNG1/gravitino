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

import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobSubmitter;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoopJobSubmitter implements JobSubmitter {
  private final Logger LOG = LoggerFactory.getLogger(NoopJobSubmitter.class);

  public static final String NAME = "noop";

  @Override
  public String submitJob(String jobTemplateName, JobExecutionContext jobExecutionContext) {
    LOG.info(
        "NoopJobSubmitter submitJob: template={}, identifier={}, jobExecuteContext={}",
        jobTemplateName,
        jobExecutionContext.nameIdentifier(),
        jobExecutionContext);
    return "";
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {}

  @Override
  public void close() throws Exception {}
}
