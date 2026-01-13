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

import java.util.Map;
import org.apache.gravitino.maintenance.optimizer.api.common.Strategy;

/** Utility methods and rule keys for interpreting optimizer strategies. */
public class StrategyUtils {

  public static final String COMPACTION_STRATEGY_TYPE = "compaction";

  /** Prefix for job option keys exposed as rules. */
  public static final String JOB_ROLE_PREFIX = "job.";
  /** Rule key for the trigger expression. */
  public static final String TRIGGER_EXPR = "trigger-expr";
  /** Rule key for the score expression. */
  public static final String SCORE_EXPR = "score-expr";
  /** Rule key for the partition table score aggregation mode. */
  public static final String PARTITION_TABLE_SCORE_MODE = "partition_table_score_mode";

  public static final String SCORE_MODE_SUM = "sum";
  public static final String SCORE_MODE_AVG = "avg";
  public static final String SCORE_MODE_MAX = "max";
  private static final String DEFAULT_TRIGGER_EXPR = "false";
  private static final String DEFAULT_SCORE_EXPR = "-1";
  private static final String DEFAULT_PARTITION_TABLE_SCORE_MODE = SCORE_MODE_AVG;

  /**
   * Resolve the trigger expression for a strategy.
   *
   * @param strategy strategy definition
   * @return trigger expression or {@code false} by default
   */
  public static String getTriggerExpression(Strategy strategy) {
    Object value = strategy.rules().get(TRIGGER_EXPR);
    if (value == null) {
      return DEFAULT_TRIGGER_EXPR;
    }
    String expression = value.toString();
    return expression.trim().isEmpty() ? DEFAULT_TRIGGER_EXPR : expression;
  }

  /**
   * Resolve the job template name for a strategy.
   *
   * @param strategy strategy definition
   * @return job template name
   */
  public static String getJobTemplateName(Strategy strategy) {
    return strategy.jobTemplateName();
  }

  /**
   * Resolve the score expression for a strategy.
   *
   * @param strategy strategy definition
   * @return score expression or {@code -1} by default
   */
  public static String getScoreExpression(Strategy strategy) {
    Object value = strategy.rules().get(SCORE_EXPR);
    if (value == null) {
      return DEFAULT_SCORE_EXPR;
    }
    String expression = value.toString();
    return expression.trim().isEmpty() ? DEFAULT_SCORE_EXPR : expression;
  }

  /**
   * Resolve job options for a strategy.
   *
   * @param strategy strategy definition
   * @return immutable job options map
   */
  public static Map<String, String> getJobOptionsFromStrategy(Strategy strategy) {
    return strategy.jobOptions();
  }

  /**
   * Resolve the partition table score mode for a strategy.
   *
   * <p>Supported values are {@code sum}, {@code avg}, and {@code max}. Defaults to {@code avg}.
   *
   * @param strategy strategy definition
   * @return normalized score mode string
   */
  public static String getPartitionTableScoreMode(Strategy strategy) {
    Object value = strategy.rules().get(PARTITION_TABLE_SCORE_MODE);
    if (value == null) {
      return DEFAULT_PARTITION_TABLE_SCORE_MODE;
    }
    String mode = value.toString().trim().toLowerCase();
    return mode.isEmpty() ? DEFAULT_PARTITION_TABLE_SCORE_MODE : mode;
  }
}
