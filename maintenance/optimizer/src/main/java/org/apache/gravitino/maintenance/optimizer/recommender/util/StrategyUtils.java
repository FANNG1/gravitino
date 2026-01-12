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

package org.apache.gravitino.maintenance.optimizer.recommender.util;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.maintenance.optimizer.api.common.Strategy;

public class StrategyUtils {

  public static final String COMPACTION_STRATEGY_TYPE = "compaction";

  public static final String JOB_ROLE_PREFIX = "job.";
  public static final String TRIGGER_EXPR = "trigger-expr";
  public static final String SCORE_EXPR = "score-expr";

  public static String getTriggerExpression(Strategy strategy) {
    return strategy.rules().get(TRIGGER_EXPR).toString();
  }

  public static String getJobTemplateName(Strategy strategy) {
    return strategy.jobTemplateName();
  }

  public static String getScoreExpression(Strategy strategy) {
    return strategy.rules().get(SCORE_EXPR).toString();
  }

  public static Map<String, Object> getJobConfigFromStrategy(Strategy strategy) {
    Map<String, Object> jobConfig = new HashMap<>();
    strategy
        .rules()
        .forEach(
            (k, v) -> {
              if (k.startsWith(JOB_ROLE_PREFIX)) {
                jobConfig.put(k.substring(JOB_ROLE_PREFIX.length()), v);
              }
            });
    strategy.jobOptions().forEach(jobConfig::putIfAbsent);
    return jobConfig;
  }
}
