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

package org.apache.gravitino.optimizer.updater.computer;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.optimizer.api.common.PartitionStatistic;
import org.apache.gravitino.optimizer.api.common.SingleStatistic;
import org.apache.gravitino.optimizer.api.recommender.SupportTableStats;
import org.apache.gravitino.optimizer.api.updater.SupportJobStats;
import org.apache.gravitino.optimizer.common.OptimizerEnv;
import org.apache.gravitino.optimizer.updater.SingleStatisticImpl;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;

public class CliStatsComputer implements SupportTableStats, SupportJobStats {

  public static final String NAME = "cli";
  public static final String CUSTOM = "custom";

  private List<SingleStatistic<?>> tableStatistics = new ArrayList<>();
  private List<SingleStatistic<?>> jobStatistics = new ArrayList<>();
  private List<PartitionStatistic> partitionStatistics = new ArrayList<>();

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    String customContent = optimizerEnv.config().getAllConfig().get(CUSTOM);
    Preconditions.checkArgument(StringUtils.isNotBlank(customContent), "custom content is empty");
    if (customContent.startsWith("table:")) {
      this.tableStatistics = getStatistics(customContent.substring("table:".length()));
    } else if (customContent.startsWith("job:")) {
      this.jobStatistics = getStatistics(customContent.substring("job:".length()));
    } else {
      throw new IllegalArgumentException("custom content format is invalid: " + customContent);
    }
  }

  @Override
  public List<SingleStatistic<?>> getTableStats(NameIdentifier tableIdentifier) {
    return tableStatistics;
  }

  @Override
  public List<PartitionStatistic> getPartitionStats(NameIdentifier tableIdentifier) {
    return partitionStatistics;
  }

  @Override
  public List<SingleStatistic<?>> computeJobStats(NameIdentifier jobIdentifier) {
    return jobStatistics;
  }

  static List<SingleStatistic<?>> getStatistics(String customContent) {

    List<SingleStatistic<?>> statistics = new ArrayList<>();
    // The custom content format is like "table:name=value,name=value", parse it to a list of
    // SingleStatistic
    // 1. Split the content by comma
    String[] stats = customContent.split(",");
    // 2. For each stat, split it by colon
    for (String stat : stats) {
      String[] statParts = stat.split(":");
      Preconditions.checkArgument(statParts.length == 2, "custom stat format is invalid");
      // 3. For each stat part, split it by equal sign
      String[] statPartParts = statParts[1].split("=");
      Preconditions.checkArgument(statPartParts.length == 2, "custom stat format is invalid");

      String name = statPartParts[0];
      StatisticValue<?> value = getStatisticValue(statPartParts[1]);
      SingleStatisticImpl<?> singleStatistic = new SingleStatisticImpl<>(name, value);
      statistics.add(singleStatistic);
    }
    return statistics;
  }

  @SuppressWarnings("EmptyCatch")
  private static StatisticValue getStatisticValue(String value) {
    try {
      Long longValue = Long.parseLong(value);
      return StatisticValues.longValue(longValue);
    } catch (NumberFormatException e) {
    }

    try {
      Double doubleValue = Double.parseDouble(value);
      return StatisticValues.doubleValue(doubleValue);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("value is not a number: " + value);
    }
  }
}
