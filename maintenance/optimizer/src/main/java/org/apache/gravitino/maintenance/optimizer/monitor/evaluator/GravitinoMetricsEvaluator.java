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

package org.apache.gravitino.maintenance.optimizer.monitor.evaluator;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricSample;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricScope;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricsEvaluator;
import org.apache.gravitino.maintenance.optimizer.common.util.StatisticValueUtils;
import org.apache.gravitino.stats.StatisticValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitinoMetricsEvaluator implements MetricsEvaluator {

  public static final String NAME = "gravitino-metrics-evaluator";

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoMetricsEvaluator.class);

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public boolean evaluateMetrics(
      MetricScope scope,
      Map<String, List<MetricSample>> beforeMetrics,
      Map<String, List<MetricSample>> afterMetrics) {
    Set<String> metricNames = new java.util.HashSet<>(beforeMetrics.keySet());
    metricNames.addAll(afterMetrics.keySet());

    metricNames.forEach(
        name -> {
          List<MetricSample> metricsBeforeList =
              beforeMetrics.getOrDefault(name, Collections.emptyList());
          List<MetricSample> metricsAfterList =
              afterMetrics.getOrDefault(name, Collections.emptyList());
          if (metricsBeforeList.isEmpty() && metricsAfterList.isEmpty()) {
            LOG.debug(
                "Metrics {} of {} ({}) before and after action time are empty, skip evaluation",
                name,
                scope.identifier(),
                scope.type());
            return;
          }

          doEvaluation(scope, metricsBeforeList, metricsAfterList, name);
        });
    return true;
  }

  private void doEvaluation(
      MetricScope scope,
      List<MetricSample> beforeMetrics,
      List<MetricSample> afterMetrics,
      String metricName) {
    // implement evaluation logic here, 1. print metrics before and after action time, 2 compare the
    // avg metrics before and after action time
    // 1. print metrics before and after action time
    LOG.info(
        String.format(
            "Metrics %s of %s (%s) before action time: %s",
            metricName,
            scope.identifier(),
            scope.type(),
            beforeMetrics.stream().map(MetricSample::toString).toList()));
    LOG.info(
        String.format(
            "Metrics %s of %s (%s) after action time: %s",
            metricName,
            scope.identifier(),
            scope.type(),
            afterMetrics.stream().map(MetricSample::toString).toList()));

    List<StatisticValue<?>> beforeValues =
        beforeMetrics.stream().map(this::toStatisticValue).collect(Collectors.toList());
    List<StatisticValue<?>> afterValues =
        afterMetrics.stream().map(this::toStatisticValue).collect(Collectors.toList());
    java.util.Optional<StatisticValue<?>> beforeAvg = StatisticValueUtils.avg(beforeValues);
    java.util.Optional<StatisticValue<?>> afterAvg = StatisticValueUtils.avg(afterValues);

    LOG.info(
        String.format(
            "Metrics %s of %s (%s) avg before action time: %s",
            metricName,
            scope.identifier(),
            scope.type(),
            beforeAvg.map(value -> String.valueOf(value.value())).orElse("N/A")));
    LOG.info(
        String.format(
            "Metrics %s of %s (%s) avg after action time: %s",
            metricName,
            scope.identifier(),
            scope.type(),
            afterAvg.map(value -> String.valueOf(value.value())).orElse("N/A")));
  }

  private StatisticValue<?> toStatisticValue(MetricSample metric) {
    StatisticEntry<?> statistic = metric.statistic();
    return statistic.value();
  }
}
