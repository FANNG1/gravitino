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

package org.apache.gravitino.optimizer.monitor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.optimizer.api.common.SingleMetric;
import org.apache.gravitino.optimizer.api.monitor.JobProvider;
import org.apache.gravitino.optimizer.api.monitor.MetricsEvaluator;
import org.apache.gravitino.optimizer.api.monitor.MetricsProvider;
import org.apache.gravitino.optimizer.common.OptimizerEnv;
import org.apache.gravitino.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.optimizer.common.util.InstanceLoaderUtils;
import org.apache.gravitino.optimizer.common.util.ProviderUtils;

public class Monitor {

  private MetricsProvider metricsProvider;
  private JobProvider jobProvider;
  private MetricsEvaluator defaultMetricsEvaluator;

  public Monitor(OptimizerEnv optimizerEnv) {
    this.metricsProvider = loadMetricsProvider(optimizerEnv.config());
    metricsProvider.initialize(optimizerEnv);
    this.jobProvider = loadJobProvider(optimizerEnv.config());
    jobProvider.initialize(optimizerEnv);
    this.defaultMetricsEvaluator = loadMetricsEvaluator(optimizerEnv.config());
  }

  public void run(
      List<NameIdentifier> tableIdentifiers,
      long actionTime,
      long rangeSeconds,
      Optional<String> policyType) {
    for (NameIdentifier tableIdentifier : tableIdentifiers) {
      run(tableIdentifier, actionTime, rangeSeconds, policyType);
    }
  }

  public void run(
      NameIdentifier tableIdentifier,
      long ActionTime,
      long rangeSeconds,
      Optional<String> policyType) {
    MetricsEvaluator evaluator = getMetricsEvaluator(policyType);
    evaluateTableMetrics(evaluator, tableIdentifier, ActionTime, rangeSeconds);
    List<NameIdentifier> jobs = jobProvider.getJobNames(tableIdentifier);
    for (NameIdentifier job : jobs) {
      evaluateJobMetrics(evaluator, job, ActionTime, rangeSeconds);
    }
  }

  public MetricsEvaluator metricsEvaluator() {
    return defaultMetricsEvaluator;
  }

  void evaluateTableMetrics(
      MetricsEvaluator evaluator, NameIdentifier tableIdentifier, long time, long rangeSeconds) {
    Pair<Long, Long> timeRange = getTimeRange(time, rangeSeconds);
    Map<String, List<SingleMetric>> metrics =
        metricsProvider.listTableMetrics(
            tableIdentifier, Optional.empty(), timeRange.getLeft(), timeRange.getRight());

    Pair<Map<String, List<SingleMetric>>, Map<String, List<SingleMetric>>> splitMetrics =
        splitMetrics(metrics, time);

    evaluator.evaluateTableMetrics(
        tableIdentifier, splitMetrics.getLeft(), splitMetrics.getRight());
  }

  private Pair<Map<String, List<SingleMetric>>, Map<String, List<SingleMetric>>> splitMetrics(
      Map<String, List<SingleMetric>> metrics, long actionTimeInSeconds) {
    // split metrics into metrics before and after action time
    Map<String, List<SingleMetric>> beforeMetrics = new HashMap<>();
    Map<String, List<SingleMetric>> afterMetrics = new HashMap<>();
    for (Map.Entry<String, List<SingleMetric>> entry : metrics.entrySet()) {
      String metricName = entry.getKey();
      List<SingleMetric> metricList = entry.getValue();
      beforeMetrics.put(
          metricName,
          metricList.stream().filter(m -> m.timestamp() < actionTimeInSeconds).toList());
      afterMetrics.put(
          metricName,
          metricList.stream().filter(m -> m.timestamp() >= actionTimeInSeconds).toList());
    }
    return Pair.of(beforeMetrics, afterMetrics);
  }

  private void evaluateJobMetrics(
      MetricsEvaluator evaluator,
      NameIdentifier jobIdentifier,
      long actionTimeInSeconds,
      long rangeSeconds) {
    Pair<Long, Long> timeRange = getTimeRange(actionTimeInSeconds, rangeSeconds);
    Map<String, List<SingleMetric>> metrics =
        metricsProvider.listJobMetrics(jobIdentifier, timeRange.getLeft(), timeRange.getRight());
    Pair<Map<String, List<SingleMetric>>, Map<String, List<SingleMetric>>> splitMetrics =
        splitMetrics(metrics, actionTimeInSeconds);
    evaluator.evaluateJobMetrics(jobIdentifier, splitMetrics.getLeft(), splitMetrics.getRight());
  }

  @SuppressWarnings("UnusedVariable")
  private MetricsEvaluator getMetricsEvaluator(Optional<String> policyType) {
    // TODO: use different metrics evaluator for different policy type
    return defaultMetricsEvaluator;
  }

  private Pair<Long, Long> getTimeRange(long actionTime, long rangeSeconds) {
    long startTime = actionTime - rangeSeconds;
    long endTime = actionTime + rangeSeconds;
    return Pair.of(startTime, endTime);
  }

  private MetricsProvider loadMetricsProvider(OptimizerConfig optimizerConfig) {
    return ProviderUtils.createMetricsProviderInstance(
        optimizerConfig.get(OptimizerConfig.METRICS_PROVIDER_CONFIG));
  }

  private JobProvider loadJobProvider(OptimizerConfig optimizerConfig) {
    return ProviderUtils.createJobProviderInstance(
        optimizerConfig.get(OptimizerConfig.JOB_PROVIDER_CONFIG));
  }

  private MetricsEvaluator loadMetricsEvaluator(OptimizerConfig optimizerConfig) {
    return InstanceLoaderUtils.createMetricsEvaluatorInstance(
        optimizerConfig.get(OptimizerConfig.METRICS_EVALUATOR_CONFIG));
  }
}
