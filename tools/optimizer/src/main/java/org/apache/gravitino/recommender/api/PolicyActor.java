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

package org.apache.gravitino.recommender.api;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.stats.PartitionStatistics;
import org.apache.gravitino.stats.Statistic;

public interface PolicyActor {
  interface requireTableMetadata {
    void setTableMetadata(Table tableMetadata);
  }

  interface requireTableStats {
    void setTableStats(List<Statistic> tableStats);
  }

  interface requirePartitionStats {
    void setPartitionStats(List<PartitionStatistics> partitionStats);
  }

  interface JobExecuteContext {
    NameIdentifier name();

    Map<String, Object> config();

    Policy policy();
  }

  void initialize(NameIdentifier nameIdentifier, Policy policy);

  String policyType();

  long score();
  // Whether to trigger a job to run the policy
  boolean shouldTrigger();
  // The job configuration to run the policy
  JobExecuteContext jobConfig();
}
