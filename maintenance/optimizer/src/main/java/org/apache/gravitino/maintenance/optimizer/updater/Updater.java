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

package org.apache.gravitino.maintenance.optimizer.updater;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricSample;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.updater.MetricsUpdater;
import org.apache.gravitino.maintenance.optimizer.api.updater.StatisticsCalculator;
import org.apache.gravitino.maintenance.optimizer.api.updater.StatisticsUpdater;
import org.apache.gravitino.maintenance.optimizer.api.updater.SupportsCalculateJobStatistics;
import org.apache.gravitino.maintenance.optimizer.api.updater.SupportsCalculateTableStatistics;
import org.apache.gravitino.maintenance.optimizer.common.CloseableGroup;
import org.apache.gravitino.maintenance.optimizer.common.MetricSampleImpl;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.common.util.InstanceLoaderUtils;
import org.apache.gravitino.maintenance.optimizer.common.util.ProviderUtils;
import org.apache.gravitino.maintenance.optimizer.recommender.util.PartitionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point that wires together the statistics calculator and updater providers to persist
 * optimizer statistics or metrics.
 *
 * <p>Usage:
 *
 * <ol>
 *   <li>Configure {@link OptimizerConfig#STATISTICS_UPDATER_CONFIG} and {@link
 *       OptimizerConfig#METRICS_UPDATER_CONFIG} with provider names.
 *   <li>Instantiate {@link Updater} with an {@link OptimizerEnv} to initialize the providers.
 *   <li>Call {@link #update(String, List, UpdateType)} for specific identifiers or {@link
 *       #updateAll(String, UpdateType)} for bulk refresh.
 *   <li>Call {@link #close()} to release provider resources.
 * </ol>
 */
public class Updater implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(Updater.class);

  private StatisticsUpdater statisticsUpdater;
  private MetricsUpdater metricsUpdater;
  private OptimizerEnv optimizerEnv;
  private final CloseableGroup closeableGroup = new CloseableGroup();

  public Updater(OptimizerEnv optimizerEnv) {
    this.optimizerEnv = optimizerEnv;
    this.statisticsUpdater = loadStatisticsUpdater(optimizerEnv.config());
    statisticsUpdater.initialize(optimizerEnv);
    this.metricsUpdater = loadMetricsUpdater(optimizerEnv.config());
    metricsUpdater.initialize(optimizerEnv);
    addToCloseableGroup();
  }

  /**
   * Updates statistics or metrics for the provided identifiers.
   *
   * <p>This is the main entry point for updating a bounded set of targets. The updater resolves the
   * {@link StatisticsCalculator} by name, calculates table and partition statistics, and persists
   * either raw statistics or derived metrics based on {@code updateType}. If the calculator
   * implements {@link SupportsCalculateJobStatistics} and {@code updateType} is {@link
   * UpdateType#METRICS}, job metrics are also emitted.
   *
   * @param statisticsComputerName The provider name of the statistics calculator.
   * @param nameIdentifiers The identifiers to update (table and/or job).
   * @param updateType The target update type: statistics or metrics.
   */
  public void update(
      String statisticsComputerName, List<NameIdentifier> nameIdentifiers, UpdateType updateType) {
    StatisticsCalculator calculator = getStatisticsCalculator(statisticsComputerName);
    long tableRecords = 0;
    long partitionRecords = 0;
    long jobRecords = 0;
    for (NameIdentifier nameIdentifier : nameIdentifiers) {
      if (calculator instanceof SupportsCalculateTableStatistics) {
        SupportsCalculateTableStatistics supportTableStatistics =
            ((SupportsCalculateTableStatistics) calculator);
        List<StatisticEntry<?>> statistics =
            supportTableStatistics.calculateTableStatistics(nameIdentifier);
        Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics =
            supportTableStatistics.calculatePartitionStatistics(nameIdentifier);
        tableRecords += countStatistics(statistics);
        partitionRecords += countPartitionStatistics(partitionStatistics);
        LOG.info(
            "Updating table statistics/metrics: calculator={}, updateType={}, identifier={}",
            statisticsComputerName,
            updateType,
            nameIdentifier);
        updateTable(statistics, nameIdentifier, updateType);
        updatePartition(partitionStatistics, nameIdentifier, updateType);
      }
      if (calculator instanceof SupportsCalculateJobStatistics
          && updateType.equals(UpdateType.METRICS)) {
        SupportsCalculateJobStatistics supportJobStatistics =
            ((SupportsCalculateJobStatistics) calculator);
        List<StatisticEntry<?>> statistics =
            supportJobStatistics.calculateJobStatistics(nameIdentifier);
        jobRecords += countStatistics(statistics);
        LOG.info(
            "Updating job metrics: calculator={}, identifier={}",
            statisticsComputerName,
            nameIdentifier);
        updateJob(statistics, nameIdentifier);
      }
    }
    System.out.println(
        String.format(
            "SUMMARY: %s totalRecords=%d tableRecords=%d partitionRecords=%d jobRecords=%d",
            updateType.name().toLowerCase(Locale.ROOT),
            tableRecords + partitionRecords + jobRecords,
            tableRecords,
            partitionRecords,
            jobRecords));
  }

  /**
   * Updates statistics or metrics for all identifiers returned by the calculator.
   *
   * <p>This is the main entry point for batch refreshes. The updater asks the {@link
   * StatisticsCalculator} for all table statistics (and optionally job statistics) and persists
   * them according to {@code updateType}.
   *
   * @param statisticsComputerName The provider name of the statistics calculator.
   * @param updateType The target update type: statistics or metrics.
   */
  public void updateAll(String statisticsComputerName, UpdateType updateType) {
    StatisticsCalculator calculator = getStatisticsCalculator(statisticsComputerName);
    long tableRecords = 0;
    long partitionRecords = 0;
    long jobRecords = 0;

    if (calculator instanceof SupportsCalculateTableStatistics supportTableStatistics) {
      Map<NameIdentifier, List<StatisticEntry<?>>> allTableStatistics =
          supportTableStatistics.calculateAllTableStatistics();

      tableRecords += countAllStatistics(allTableStatistics);
      allTableStatistics.forEach(
          (identifier, statistics) -> updateTable(statistics, identifier, updateType));

      Map<NameIdentifier, Map<PartitionPath, List<StatisticEntry<?>>>> allPartitionStatistics =
          calculateAllPartitionStatisticsSafe(supportTableStatistics);
      partitionRecords += countAllPartitionStatistics(allPartitionStatistics);
      allPartitionStatistics.forEach(
          (identifier, partitionStatistics) ->
              updatePartition(partitionStatistics, identifier, updateType));
    }

    if (calculator instanceof SupportsCalculateJobStatistics supportJobStatistics
        && UpdateType.METRICS.equals(updateType)) {
      Map<NameIdentifier, List<StatisticEntry<?>>> allJobStatistics =
          supportJobStatistics.calculateAllJobStatistics();
      jobRecords += countAllStatistics(allJobStatistics);
      allJobStatistics.forEach((identifier, statistics) -> updateJob(statistics, identifier));
    }
    System.out.println(
        String.format(
            "SUMMARY: %s totalRecords=%d tableRecords=%d partitionRecords=%d jobRecords=%d",
            updateType.name().toLowerCase(Locale.ROOT),
            tableRecords + partitionRecords + jobRecords,
            tableRecords,
            partitionRecords,
            jobRecords));
  }

  @VisibleForTesting
  public MetricsUpdater getMetricsUpdater() {
    return metricsUpdater;
  }

  @Override
  public void close() throws Exception {
    closeableGroup.close();
  }

  private void addToCloseableGroup() {
    closeableGroup.register(statisticsUpdater, StatisticsUpdater.class.getSimpleName());
    closeableGroup.register(metricsUpdater, MetricsUpdater.class.getSimpleName());
  }

  private void updateTable(
      List<StatisticEntry<?>> statistics, NameIdentifier tableIdentifier, UpdateType updateType) {
    switch (updateType) {
      case STATISTICS:
        updateTableStatistics(statistics, tableIdentifier);
        break;
      case METRICS:
        updateTableMetrics(statistics, tableIdentifier);
        break;
    }
  }

  private void updatePartition(
      Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics,
      NameIdentifier tableIdentifier,
      UpdateType updateType) {
    switch (updateType) {
      case STATISTICS:
        updatePartitionStatistics(partitionStatistics, tableIdentifier);
        break;
      case METRICS:
        updatePartitionMetrics(partitionStatistics, tableIdentifier);
        break;
    }
  }

  private void updateTableStatistics(
      List<StatisticEntry<?>> statistics, NameIdentifier tableIdentifier) {
    LOG.info(
        "Persisting table statistics: identifier={}, count={}, details={}",
        tableIdentifier,
        statistics != null ? statistics.size() : 0,
        summarize(statistics));
    statisticsUpdater.updateTableStatistics(tableIdentifier, statistics);
  }

  private void updatePartitionStatistics(
      Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics,
      NameIdentifier tableIdentifier) {
    if (partitionStatistics == null || partitionStatistics.isEmpty()) {
      LOG.info(
          "Persist partition statistics skipped: identifier={}, reason=empty partitions",
          tableIdentifier);
      return;
    }
    LOG.info(
        "Persisting partition statistics: identifier={}, partitions={}, names={}, sample={}",
        tableIdentifier,
        partitionStatistics.size(),
        partitionNames(partitionStatistics),
        summarize(partitionStatistics.values().stream().flatMap(Collection::stream).toList()));
    statisticsUpdater.updatePartitionStatistics(tableIdentifier, partitionStatistics);
  }

  private void updateTableMetrics(
      List<StatisticEntry<?>> statistics, NameIdentifier tableIdentifier) {
    long timestampSeconds = System.currentTimeMillis() / 1000;
    LOG.info(
        "Persisting table metrics: identifier={}, count={}, details={}",
        tableIdentifier,
        statistics != null ? statistics.size() : 0,
        summarize(statistics));
    metricsUpdater.updateTableMetrics(tableIdentifier, toMetrics(statistics, timestampSeconds));
  }

  private void updatePartitionMetrics(
      Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics,
      NameIdentifier tableIdentifier) {
    if (partitionStatistics == null || partitionStatistics.isEmpty()) {
      LOG.info(
          "Persist partition metrics skipped: identifier={}, reason=empty partitions",
          tableIdentifier);
      return;
    }
    long timestampSeconds = System.currentTimeMillis() / 1000;
    List<MetricSample> partitionMetrics = toPartitionMetrics(partitionStatistics, timestampSeconds);
    LOG.info(
        "Persisting partition metrics: identifier={}, partitions={}, names={}, details={}",
        tableIdentifier,
        partitionStatistics.size(),
        partitionNames(partitionStatistics),
        summarize(partitionStatistics.values().stream().flatMap(Collection::stream).toList()));
    metricsUpdater.updateTableMetrics(tableIdentifier, partitionMetrics);
  }

  private void updateJob(List<StatisticEntry<?>> statistics, NameIdentifier jobIdentifier) {
    long timestampSeconds = System.currentTimeMillis() / 1000;

    LOG.info(
        "Persisting job metrics: identifier={}, count={}, details={}",
        jobIdentifier,
        statistics != null ? statistics.size() : 0,
        summarize(statistics));
    metricsUpdater.updateJobMetrics(jobIdentifier, toMetrics(statistics, timestampSeconds));
  }

  private String summarize(List<StatisticEntry<?>> statistics) {
    if (statistics == null || statistics.isEmpty()) {
      return "[]";
    }
    int limit = Math.min(statistics.size(), 20);
    String summary =
        statistics.stream()
            .limit(limit)
            .map(stat -> stat.name() + "=" + stat.value().value())
            .collect(Collectors.joining(", ", "[", "]"));
    if (statistics.size() > limit) {
      summary = summary + " ... (" + statistics.size() + " total)";
    }
    return summary;
  }

  private List<MetricSample> toMetrics(List<StatisticEntry<?>> statistics, long timestamp) {
    List<MetricSample> metrics = new ArrayList<>();
    if (statistics != null) {
      statistics.forEach(stat -> metrics.add(new MetricSampleImpl(timestamp, stat)));
    }
    return metrics;
  }

  private List<MetricSample> toPartitionMetrics(
      Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics, long timestamp) {
    List<MetricSample> metrics = new ArrayList<>();
    if (partitionStatistics != null) {
      partitionStatistics.forEach(
          (partitionPath, statisticEntries) ->
              statisticEntries.forEach(
                  stat ->
                      metrics.add(
                          new org.apache.gravitino.maintenance.optimizer.common
                              .PartitionMetricSampleImpl(timestamp, stat, partitionPath))));
    }
    return metrics;
  }

  private StatisticsCalculator getStatisticsCalculator(String statisticsComputerName) {
    StatisticsCalculator calculator =
        InstanceLoaderUtils.createStatisticsCalculatorInstance(statisticsComputerName);
    calculator.initialize(optimizerEnv);
    return calculator;
  }

  private Map<NameIdentifier, Map<PartitionPath, List<StatisticEntry<?>>>>
      calculateAllPartitionStatisticsSafe(SupportsCalculateTableStatistics supportTableStatistics) {
    try {
      return supportTableStatistics.calculateAllPartitionStatistics();
    } catch (UnsupportedOperationException e) {
      LOG.debug("Bulk partition statistics computation not supported: {}", e.getMessage());
      return Map.of();
    }
  }

  private StatisticsUpdater loadStatisticsUpdater(OptimizerConfig config) {
    String statisticsUpdaterName = config.get(OptimizerConfig.STATISTICS_UPDATER_CONFIG);
    return ProviderUtils.createStatisticsUpdaterInstance(statisticsUpdaterName);
  }

  private String partitionNames(Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics) {
    return partitionStatistics.keySet().stream()
        .map(PartitionUtils::encodePartitionPath)
        .collect(Collectors.joining(", ", "[", "]"));
  }

  private MetricsUpdater loadMetricsUpdater(OptimizerConfig config) {
    String metricsUpdaterName = config.get(OptimizerConfig.METRICS_UPDATER_CONFIG);
    return ProviderUtils.createMetricsUpdaterInstance(metricsUpdaterName);
  }

  private long countStatistics(List<StatisticEntry<?>> statistics) {
    return statistics == null ? 0 : statistics.size();
  }

  private long countPartitionStatistics(
      Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics) {
    if (partitionStatistics == null) {
      return 0;
    }
    return partitionStatistics.values().stream().mapToLong(this::countStatistics).sum();
  }

  private long countAllStatistics(Map<NameIdentifier, List<StatisticEntry<?>>> statisticsByTable) {
    if (statisticsByTable == null) {
      return 0;
    }
    return statisticsByTable.values().stream().mapToLong(this::countStatistics).sum();
  }

  private long countAllPartitionStatistics(
      Map<NameIdentifier, Map<PartitionPath, List<StatisticEntry<?>>>> partitionStatisticsByTable) {
    if (partitionStatisticsByTable == null) {
      return 0;
    }
    return partitionStatisticsByTable.values().stream()
        .mapToLong(this::countPartitionStatistics)
        .sum();
  }
}
