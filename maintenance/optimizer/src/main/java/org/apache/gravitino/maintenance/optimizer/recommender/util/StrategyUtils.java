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
import org.apache.gravitino.maintenance.optimizer.recommender.actor.ScoreMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility methods and rule keys for interpreting optimizer strategies. */
public class StrategyUtils {

  private static final Logger LOG = LoggerFactory.getLogger(StrategyUtils.class);

  public static final String COMPACTION_STRATEGY_TYPE = "compaction";

  /** Prefix for job option keys exposed as rules. */
  public static final String JOB_ROLE_PREFIX = "job.";
  /** Rule key for the trigger expression. */
  public static final String TRIGGER_EXPR = "trigger-expr";
  /** Rule key for the score expression. */
  public static final String SCORE_EXPR = "score-expr";
  /** Rule key for the partition table score aggregation mode. */
  public static final String PARTITION_TABLE_SCORE_MODE = "partition_table_score_mode";
  /** Rule key for the maximum number of partitions selected for execution. */
  public static final String MAX_PARTITION_NUM = "max_partition_num";

  private static final String DEFAULT_TRIGGER_EXPR = "false";
  private static final String DEFAULT_SCORE_EXPR = "-1";
  private static final ScoreMode DEFAULT_PARTITION_TABLE_SCORE_MODE = ScoreMode.AVG;
  private static final int DEFAULT_MAX_PARTITION_NUM = 100;

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
   * <p>Supported values are {@code sum}, {@code avg}, and {@code max}. Defaults to {@code AVG}.
   *
   * @param strategy strategy definition
   * @return score mode enum
   */
  public static ScoreMode getPartitionTableScoreMode(Strategy strategy) {
    Object value = strategy.rules().get(PARTITION_TABLE_SCORE_MODE);
    if (value == null) {
      return DEFAULT_PARTITION_TABLE_SCORE_MODE;
    }
    if (value instanceof ScoreMode) {
      return (ScoreMode) value;
    }
    String mode = value.toString().trim().toLowerCase();
    if (mode.isEmpty()) {
      return DEFAULT_PARTITION_TABLE_SCORE_MODE;
    }
    switch (mode) {
      case "sum":
        return ScoreMode.SUM;
      case "max":
        return ScoreMode.MAX;
      case "avg":
        return ScoreMode.AVG;
      default:
        LOG.warn(
            "Unsupported partition table score mode '{}' for strategy {}, defaulting to avg",
            mode,
            strategy.name());
        return DEFAULT_PARTITION_TABLE_SCORE_MODE;
    }
  }

  /**
   * Resolve the maximum number of partitions selected for execution.
   *
   * <p>Defaults to {@code 100} when the rule is missing or invalid.
   *
   * @param strategy strategy definition
   * @return maximum number of partitions to include
   */
  public static int getMaxPartitionNum(Strategy strategy) {
    Object value = strategy.rules().get(MAX_PARTITION_NUM);
    if (value == null) {
      return DEFAULT_MAX_PARTITION_NUM;
    }
    String limit = value.toString().trim();
    if (limit.isEmpty()) {
      return DEFAULT_MAX_PARTITION_NUM;
    }
    try {
      int parsed = Integer.parseInt(limit);
      return parsed > 0 ? parsed : DEFAULT_MAX_PARTITION_NUM;
    } catch (NumberFormatException e) {
      return DEFAULT_MAX_PARTITION_NUM;
    }
  }
}
