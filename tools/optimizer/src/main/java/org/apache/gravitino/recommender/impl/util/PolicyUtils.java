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

package org.apache.gravitino.recommender.impl.util;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.policy.Policy;

public class PolicyUtils {

  public static final String JOB_ROLE_PREFIX = "job.";

  public static String getTriggerExpression(Policy policy) {
    return policy.content().properties().get("compaction.trigger-expr");
  }

  public static String getJobTemplateName(Policy policy) {
    return policy.content().properties().get("job.template-name");
  }

  public static String getScoreExpression(Policy policy) {
    return policy.content().properties().get("compaction.score-expr");
  }

  @SuppressWarnings("EmptyCatch")
  public static Map<String, Object> getJobConfigFromPolicy(Policy policy) {
    Map<String, Object> jobConfig = new HashMap<>();
    // Todo: get job config from rule not properties
    policy
        .content()
        .properties()
        .forEach(
            (k, v) -> {
              if (k.startsWith(JOB_ROLE_PREFIX)) {
                try {
                  long longValue = Long.parseLong(v);
                  jobConfig.put(k.substring(JOB_ROLE_PREFIX.length()), longValue);
                } catch (NumberFormatException e) {
                }
              }
            });
    return jobConfig;
  }
}
