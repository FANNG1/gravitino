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
import org.apache.gravitino.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.optimizer.api.updater.SupportComputeTableStats;
import org.apache.gravitino.optimizer.api.updater.SupportJobStats;
import org.apache.gravitino.optimizer.common.OptimizerContent;
import org.apache.gravitino.optimizer.common.OptimizerEnv;
import org.apache.gravitino.optimizer.common.StatsComputerContent;
import org.apache.gravitino.optimizer.updater.StatisticEntryImpl;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;

public class CliStatsComputer implements SupportComputeTableStats, SupportJobStats {

  public static final String NAME = "gravitino-cli";
  private List<StatisticEntry<?>> tableStatistics = new ArrayList<>();
  private List<StatisticEntry<?>> jobStatistics = new ArrayList<>();

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    OptimizerContent content = optimizerEnv.content();
    Preconditions.checkArgument(
        content instanceof StatsComputerContent,
        "StatsComputerContent is required for CliStatsComputer");
    String statsPayload = ((StatsComputerContent) content).statsPayload();
    Preconditions.checkArgument(StringUtils.isNotBlank(statsPayload), "stats payload is empty");
    if (statsPayload.startsWith("table:")) {
      this.tableStatistics = getStatistics(statsPayload.substring("table:".length()));
    } else if (statsPayload.startsWith("job:")) {
      this.jobStatistics = getStatistics(statsPayload.substring("job:".length()));
    } else {
      throw new IllegalArgumentException("stats payload format is invalid: " + statsPayload);
    }
  }

  @Override
  public List<StatisticEntry<?>> computeTableStats(NameIdentifier tableIdentifier) {
    return tableStatistics;
  }

  @Override
  public List<StatisticEntry<?>> computeJobStats(NameIdentifier jobIdentifier) {
    return jobStatistics;
  }

  static List<StatisticEntry<?>> getStatistics(String statsPayload) {

    List<StatisticEntry<?>> statistics = new ArrayList<>();
    String[] stats = statsPayload.split(",");
    for (String stat : stats) {
      // 3. For each stat part, split it by equal sign
      String[] statPartParts = stat.split("=");
      Preconditions.checkArgument(statPartParts.length == 2, "custom stat format is invalid");

      String name = statPartParts[0];
      StatisticValue<?> value = getStatisticValue(statPartParts[1]);
      StatisticEntryImpl<?> singleStatistic = new StatisticEntryImpl<>(name, value);
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
