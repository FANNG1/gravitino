/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.maintenance.optimizer.tool;

import com.google.common.base.Preconditions;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricSample;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricsProvider;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.common.util.ProviderUtils;
import org.apache.gravitino.maintenance.optimizer.recommender.util.PartitionUtils;

/** Simple CLI helper to print table (and optional partition) metrics. */
public class MetricsLister {
  private final MetricsProvider metricsProvider;
  private final PrintStream out;

  public MetricsLister(OptimizerEnv optimizerEnv) {
    this(optimizerEnv, System.out);
  }

  MetricsLister(OptimizerEnv optimizerEnv, PrintStream out) {
    Preconditions.checkArgument(optimizerEnv != null, "optimizerEnv must not be null");
    Preconditions.checkArgument(out != null, "output stream must not be null");
    this.out = out;

    String providerName = optimizerEnv.config().get(OptimizerConfig.METRICS_PROVIDER_CONFIG);
    this.metricsProvider = ProviderUtils.createMetricsProviderInstance(providerName);
    this.metricsProvider.initialize(optimizerEnv);
  }

  public void listTableMetrics(
      List<NameIdentifier> identifiers,
      Optional<PartitionPath> partitionPath,
      long startTimeSeconds,
      long endTimeSeconds) {
    Preconditions.checkArgument(
        identifiers != null && !identifiers.isEmpty(), "identifiers must not be empty");
    Preconditions.checkArgument(endTimeSeconds >= startTimeSeconds, "Invalid time range");

    for (NameIdentifier identifier : identifiers) {
      printTableMetrics(identifier, startTimeSeconds, endTimeSeconds);
      partitionPath.ifPresent(
          partition ->
              printPartitionMetrics(identifier, partition, startTimeSeconds, endTimeSeconds));
    }
  }

  private void printTableMetrics(
      NameIdentifier identifier, long startTimeSeconds, long endTimeSeconds) {
    Map<String, List<MetricSample>> metrics =
        metricsProvider.getTableMetrics(identifier, startTimeSeconds, endTimeSeconds);
    printMetrics(identifier, "table", metrics);
  }

  private void printPartitionMetrics(
      NameIdentifier identifier,
      PartitionPath partitionPath,
      long startTimeSeconds,
      long endTimeSeconds) {
    Map<String, List<MetricSample>> metrics =
        metricsProvider.getPartitionMetrics(
            identifier, partitionPath, startTimeSeconds, endTimeSeconds);
    printMetrics(
        identifier, "partition:" + PartitionUtils.encodePartitionPath(partitionPath), metrics);
  }

  public void listJobMetrics(
      List<NameIdentifier> identifiers, long startTimeSeconds, long endTimeSeconds) {
    Preconditions.checkArgument(
        identifiers != null && !identifiers.isEmpty(), "identifiers must not be empty");
    Preconditions.checkArgument(endTimeSeconds >= startTimeSeconds, "Invalid time range");

    for (NameIdentifier identifier : identifiers) {
      Map<String, List<MetricSample>> metrics =
          metricsProvider.getJobMetrics(identifier, startTimeSeconds, endTimeSeconds);
      printMetrics(identifier, "job", metrics);
    }
  }

  private void printMetrics(
      NameIdentifier identifier, String scope, Map<String, List<MetricSample>> metrics) {
    if (metrics == null || metrics.isEmpty()) {
      out.printf("OK %s [%s]: no metrics%n", identifier, scope);
      return;
    }
    metrics.forEach(
        (metricName, samples) -> {
          if (samples == null || samples.isEmpty()) {
            out.printf("OK %s [%s] %s: no samples%n", identifier, scope, metricName);
            return;
          }
          for (MetricSample sample : samples) {
            out.printf(
                "OK %s [%s] %s ts=%d value=%s%n",
                identifier,
                scope,
                metricName,
                sample.timestamp(),
                sample.statistic().value().value());
          }
        });
  }
}
