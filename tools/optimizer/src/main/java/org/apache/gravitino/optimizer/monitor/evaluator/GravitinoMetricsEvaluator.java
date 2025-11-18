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

package org.apache.gravitino.optimizer.monitor.evaluator;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.optimizer.api.common.SingleMetric;
import org.apache.gravitino.optimizer.api.common.SingleStatistic;
import org.apache.gravitino.optimizer.api.monitor.MetricsEvaluator;
import org.apache.gravitino.optimizer.common.util.StatisticValueUtils;
import org.apache.gravitino.stats.StatisticValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitinoMetricsEvaluator implements MetricsEvaluator {

  public static final String NAME = "gravitino-metrics-evaluator";

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoMetricsEvaluator.class);

  private List<SingleStatistic.Name> evaluateTableMetricsNames =
      List.of(
          SingleStatistic.Name.TABLE_STORAGE_COST,
          SingleStatistic.Name.DATAFILE_AVG_SIZE,
          SingleStatistic.Name.DATAFILE_NUMBER,
          SingleStatistic.Name.DATAFILE_SIZE_MSE,
          SingleStatistic.Name.POSITION_DELETE_FILE_NUMBER,
          SingleStatistic.Name.EQUAL_DELETE_FILE_NUMBER);

  private List<SingleStatistic.Name> evaluateJobMetricsNames =
      List.of(SingleStatistic.Name.JOB_COST, SingleStatistic.Name.JOB_DURATION);

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public boolean evaluateTableMetrics(
      NameIdentifier tableIdentifier,
      Map<String, List<SingleMetric>> beforeMetrics,
      Map<String, List<SingleMetric>> afterMetrics) {
    //  evaluate table storage cost, data file size, data file number, position file number, etc
    evaluateTableMetricsNames.stream()
        .forEach(
            metricName -> {
              String name = metricName.name();
              doEvaluation(
                  tableIdentifier, beforeMetrics.get(name), afterMetrics.get(name), metricName);
            });
    return false;
  }

  @Override
  public boolean evaluateJobMetrics(
      NameIdentifier jobIdentifier,
      Map<String, List<SingleMetric>> beforeMetrics,
      Map<String, List<SingleMetric>> afterMetrics) {
    // evaluate job cost, duration, etc
    evaluateJobMetricsNames.stream()
        .forEach(
            metricName -> {
              String name = metricName.name();
              doEvaluation(
                  jobIdentifier, beforeMetrics.get(name), afterMetrics.get(name), metricName);
            });
    return false;
  }

  private void doEvaluation(
      NameIdentifier identifier,
      List<SingleMetric> beforeMetrics,
      List<SingleMetric> afterMetrics,
      SingleStatistic.Name metricName) {
    // implement evaluation logic here, 1. print metrics before and after action time, 2 compare the
    // avg metrics before and after action time
    // 1. print metrics before and after action time
    LOG.info(
        String.format(
            "Metrics %s of %s before action time: %s",
            metricName, identifier, beforeMetrics.stream().map(SingleMetric::toString).toList()));
    LOG.info(
        String.format(
            "Metrics %s of %s after action time: %s",
            metricName, identifier, afterMetrics.stream().map(SingleMetric::toString).toList()));

    StatisticValue beforeAvg =
        StatisticValueUtils.avg(beforeMetrics.stream().map(this::toStatisticValue).toList());

    StatisticValue afterAvg =
        StatisticValueUtils.avg(afterMetrics.stream().map(this::toStatisticValue).toList());

    LOG.info(
        String.format(
            "Metrics %s of %s avg before action time: %s", metricName, identifier, beforeAvg));
    LOG.info(
        String.format(
            "Metrics %s of %s avg after action time: %s", metricName, identifier, afterAvg));
  }

  private StatisticValue toStatisticValue(SingleMetric metric) {
    SingleStatistic<?> statistic = metric.statistic();
    return statistic.value();
  }
}
