/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.tms;

import java.util.Optional;
import org.apache.gravitino.NameIdentifier;

public class JobManager {
  Map<NameIdentifier, Policy> jobs;
  JobProvider jobProvider;

  MetaCache metaCache;
  Stats statsCache;

  JobScheduler scheduler;

  JobSubmitter jobSubmitter;

  public void run() {
    for (NameIdentifier nameIdentifier : jobs.keySet()) {
      Policy policy = jobs.get(nameIdentifier);
      JobProvider provider = getJobProvider(policy);
      Optional<Meta> meta = Optional.empty();
      if (provider.requireMetadata()) {
        meta = metaCache.getMetadata(nameIdentifier);
      }
      Optional<Stats> stats = Optional.empty();
      if (provider.requireStats()) {
        stats = statsCache.getStats(nameIdentifier);
      }
      provider.getJob(nameIdentifier, meta, stats).ifPresent(job -> {
        scheduler.addJob(job);
      });
    }
  }

  void submit() {
    while (true) {
      Job job = scheduler.schedule();
      jobSubmitter.submit(job);
    }
  }

}
