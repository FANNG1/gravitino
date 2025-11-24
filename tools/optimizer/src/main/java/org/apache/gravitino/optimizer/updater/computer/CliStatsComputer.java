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
import org.apache.gravitino.optimizer.api.updater.SupportJobStats;
import org.apache.gravitino.optimizer.api.updater.SupportTableStats;
import org.apache.gravitino.optimizer.common.OptimizerEnv;
import org.apache.gravitino.optimizer.updater.StatisticEntryImpl;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;

public class CliStatsComputer implements SupportTableStats, SupportJobStats {

  public static final String NAME = "gravitino-cli";
  private List<StatisticEntry<?>> tableStatistics = new ArrayList<>();
  private List<StatisticEntry<?>> jobStatistics = new ArrayList<>();

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    String customContent = optimizerEnv.customContent();
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
  public List<StatisticEntry<?>> computeTableStats(NameIdentifier tableIdentifier) {
    return tableStatistics;
  }

  @Override
  public List<StatisticEntry<?>> computeJobStats(NameIdentifier jobIdentifier) {
    return jobStatistics;
  }

  static List<StatisticEntry<?>> getStatistics(String customContent) {

    List<StatisticEntry<?>> statistics = new ArrayList<>();
    String[] stats = customContent.split(",");
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
