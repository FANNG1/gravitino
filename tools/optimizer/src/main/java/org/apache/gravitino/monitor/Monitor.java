package org.apache.gravitino.monitor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.monitor.api.JobProvider;
import org.apache.gravitino.monitor.api.MetricsProvider;
import org.apache.gravitino.monitor.api.SingleMetric;
import org.apache.gravitino.monitor.api.TableMetricsEvaluator;
import org.apache.gravitino.monitor.impl.GravitinoEvaluator;

public class Monitor {

  private MetricsProvider metricsProvider;
  private JobProvider jobProvider;
  private Map<String, TableMetricsEvaluator> evaluators = new HashMap<>();
  private TableMetricsEvaluator defaultEvaluator = new GravitinoEvaluator();

  public Monitor() {
    this.metricsProvider = loadMetricsProvider();
    this.jobProvider = loadJobProvider();
  }

  public void run(
      NameIdentifier tableIdentifier,
      long ActionTime,
      long rangeSeconds,
      Optional<String> policyType) {
    TableMetricsEvaluator evaluator = getMetricsEvaluator(policyType);
    evaluateTableMetrics(evaluator, tableIdentifier, ActionTime, rangeSeconds);
    List<NameIdentifier> jobs = jobProvider.getJobNames(tableIdentifier);
    for (NameIdentifier job : jobs) {
      evaluateJobMetrics(evaluator, job, ActionTime, rangeSeconds);
    }
  }

  void evaluateTableMetrics(
      TableMetricsEvaluator evaluator,
      NameIdentifier tableIdentifier,
      long time,
      long rangeSeconds) {
    Pair<Long, Long> timeRange = getTimeRange(time, rangeSeconds);
    Map<String, List<SingleMetric>> metrics =
        metricsProvider.tableMetricDetails(
            tableIdentifier, Optional.empty(), timeRange.getLeft(), timeRange.getRight());

    Pair<Map<String, List<SingleMetric>>, Map<String, List<SingleMetric>>> splitMetrics =
        splitMetrics(metrics, time);

    evaluator.evaluateTableMetrics(splitMetrics.getLeft(), splitMetrics.getRight());
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
      TableMetricsEvaluator evaluator, NameIdentifier jobIdentifier, long time, long rangeSeconds) {
    Map<String, List<SingleMetric>> metrics =
        metricsProvider.jobMetricDetails(jobIdentifier, time, rangeSeconds);
    Pair<Map<String, List<SingleMetric>>, Map<String, List<SingleMetric>>> splitMetrics =
        splitMetrics(metrics, time);
    evaluator.evaluateJobMetrics(splitMetrics.getLeft(), splitMetrics.getRight());
  }

  private TableMetricsEvaluator getMetricsEvaluator(Optional<String> policyType) {
    if (policyType.isPresent()) {
      return evaluators.getOrDefault(policyType.get(), defaultEvaluator);
    }
    return defaultEvaluator;
  }

  private Pair<Long, Long> getTimeRange(long actionTime, long rangeHours) {
    long startTime = actionTime - rangeHours * 60 * 60;
    long endTime = actionTime + rangeHours * 60 * 60;
    return Pair.of(startTime, endTime);
  }

  private MetricsProvider loadMetricsProvider() {
    return null;
  }

  private JobProvider loadJobProvider() {
    return null;
  }
}
