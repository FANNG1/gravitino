package org.apache.gravitino.monitor.api;

import java.util.List;
import java.util.Map;

// Evaluator interface for the table and related job metrics before and after optimization actions.
public interface TableMetricsEvaluator {
  boolean evaluateTableMetrics(
      Map<String, List<SingleMetric>> beforeMetrics, Map<String, List<SingleMetric>> afterMetrics);

  boolean evaluateJobMetrics(
      Map<String, List<SingleMetric>> beforeMetrics, Map<String, List<SingleMetric>> afterMetrics);
}
