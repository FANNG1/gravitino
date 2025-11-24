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

package org.apache.gravitino.optimizer.api.recommender;

import java.util.Map;
import java.util.Set;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.optimizer.api.common.RecommenderPolicy;

@DeveloperApi
public interface PolicyActor {
  /**
   * Declares the optional data the actor needs before it can be initialized. The {@link
   * PolicyActorContext} handed to {@link #initialize(PolicyActorContext)} will include only the
   * requested items.
   */
  enum DataRequirement {
    TABLE_METADATA,
    TABLE_STATISTICS,
    PARTITION_STATISTICS
  }

  interface JobExecuteContext {
    NameIdentifier identifier();

    // The config options for the job, e.g. target_file_size_bytes
    Map<String, Object> config();

    RecommenderPolicy policy();
  }

  /** Declares which pieces of data this actor needs before it can be initialized. */
  default Set<DataRequirement> requiredData() {
    return Set.of();
  }

  /**
   * Initialize the actor with the supplied context.
   *
   * @param context immutable view of table identifier, policy, and any requested metadata or
   *     statistics
   */
  void initialize(PolicyActorContext context);

  /** Stable identifier for the policy type this actor handles (e.g. {@code COMPACTION}). */
  String policyType();

  /**
   * Score that allows ranking multiple recommendations for the same policy; higher scores take
   * precedence.
   */
  long score();
  // Whether to trigger a job to run the policy
  boolean shouldTrigger();
  // The job configuration to run the policy
  JobExecuteContext jobConfig();
}
