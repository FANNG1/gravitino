package org.apache.gravitino.monitor;

import java.util.List;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.monitor.api.JobMetricsProvider;
import org.apache.gravitino.monitor.api.JobProvider;
import org.apache.gravitino.monitor.api.Metrics;
import org.apache.gravitino.monitor.api.MetricsEvaluator;
import org.apache.gravitino.monitor.api.TableMetricsProvider;

public class Monitor {
  JobMetricsProvider jobMetricsProvider;
  TableMetricsProvider tableMetricsProvider;
  JobProvider jobProvider;

  void run(
      NameIdentifier tableIdentifier, long time, long rangeSeconds, Optional<String> policyType) {
    MetricsEvaluator evaluator = getMetricsEvaluator(policyType);
    evaluateTableMetrics(evaluator, tableIdentifier, time, rangeSeconds);
    String[] jobNames = jobProvider.getJobNames(tableIdentifier);
    for (String jobName : jobNames) {
      evaluateJobMetrics(evaluator, jobName, time, rangeSeconds);
    }
  }

  void evaluateTableMetrics(
      MetricsEvaluator evaluator, NameIdentifier tableIdentifier, long time, long rangeSeconds) {
    List<Metrics> metrics = tableMetricsProvider.metrics(tableIdentifier, time, rangeSeconds);
    evaluator.evaluateTableMetrics(metrics);
  }

  void evaluateJobMetrics(
      MetricsEvaluator evaluator, String jobName, long time, long rangeSeconds) {
    List<Metrics> metrics = jobMetricsProvider.jobMetrics(jobName, time, rangeSeconds);
    evaluator.evaluateJobMetrics(metrics);
  }

  MetricsEvaluator getMetricsEvaluator(Optional<String> policyType) {
    // Could define specific evaluator for different policy type, or else use default
    return null;
  }
}
