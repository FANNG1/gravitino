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

import java.util.Map;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.optimizer.api.recommender.JobSubmitter;
import org.apache.gravitino.optimizer.api.recommender.PolicyActor.JobExecuteContext;

@SuppressWarnings("unused")
public class GravitinoJobSubmitter implements JobSubmitter {

  GravitinoClient gravitinoClient;

  private Map<String, GravitinoJobAdapter> jobAdapters;

  public GravitinoJobSubmitter() {
    // get all job adapters from the classpath
  }

  @Override
  public String submitJob(String policyType, JobExecuteContext job) {
    GravitinoJobAdapter jobAdapter = loadJobAdapter(policyType, job);
    if (jobAdapter == null) {
      throw new IllegalArgumentException("No job adapter found for policy type: " + policyType);
    }
    return gravitinoClient.runJob(jobAdapter.jobTemplateName(), jobAdapter.jobConfig()).jobId();
  }

  private GravitinoJobAdapter loadJobAdapter(
      String policyType, JobExecuteContext jobExecuteContext) {
    GravitinoCompactionJobAdapter gravitinoCompactionJobAdapter =
        new GravitinoCompactionJobAdapter();
    gravitinoCompactionJobAdapter.initialize(jobExecuteContext);
    return gravitinoCompactionJobAdapter;
  }
}
