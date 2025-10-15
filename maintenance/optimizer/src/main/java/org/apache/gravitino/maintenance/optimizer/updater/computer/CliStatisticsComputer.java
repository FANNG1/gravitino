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

package org.apache.gravitino.maintenance.optimizer.updater.computer;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.TableStatisticsBundle;
import org.apache.gravitino.maintenance.optimizer.api.updater.SupportsCalculateJobStatistics;
import org.apache.gravitino.maintenance.optimizer.api.updater.SupportsCalculateTableStatistics;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerContent;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.StatisticsComputerContent;
import org.apache.gravitino.maintenance.optimizer.updater.StatisticEntryImpl;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;

public class CliStatisticsComputer
    implements SupportsCalculateTableStatistics, SupportsCalculateJobStatistics {

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
        content instanceof StatisticsComputerContent,
        "StatisticsComputerContent is required for CliStatisticsComputer");
    String statisticsPayload = ((StatisticsComputerContent) content).statisticsPayload();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(statisticsPayload), "statistics payload is empty");
    if (statisticsPayload.startsWith("table:")) {
      this.tableStatistics = getStatistics(statisticsPayload.substring("table:".length()));
    } else if (statisticsPayload.startsWith("job:")) {
      this.jobStatistics = getStatistics(statisticsPayload.substring("job:".length()));
    } else {
      throw new IllegalArgumentException(
          "statistics payload format is invalid: " + statisticsPayload);
    }
  }

  @Override
  public TableStatisticsBundle calculateTableStatistics(NameIdentifier tableIdentifier) {
    return new TableStatisticsBundle(tableStatistics, java.util.Map.of());
  }

  @Override
  public List<StatisticEntry<?>> calculateJobStatistics(NameIdentifier jobIdentifier) {
    return jobStatistics;
  }

  static List<StatisticEntry<?>> getStatistics(String statisticsPayload) {
    List<StatisticEntry<?>> statistics = new ArrayList<>();
    String[] stats = statisticsPayload.split(",");
    for (String stat : stats) {
      // 3. For each stat part, split it by equal sign
      String[] statPartParts = stat.split("=");
      Preconditions.checkArgument(statPartParts.length == 2, "custom statistic format is invalid");

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
