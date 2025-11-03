package org.apache.gravitino.monitor.api;

import java.util.List;
import java.util.Map;

public interface MetricsEvaluator {
  /**
   * Initializes the evaluator with the given action time and range hours.
   *
   * @param actionTime The timestamp (in seconds) associated with the action (e.g., optimize job
   *     finish time)
   * @param rangeHours The time range (in hours) to consider when evaluate metrics (e.g., metrics
   *     from the past N hours relative to actionTime)
   */
  void initialize(long actionTime, long rangeHours);

  boolean evaluateTableMetrics(
      Map<String, List<SingleMetric>> beforeMetrics, Map<String, List<SingleMetric>> afterMetrics);

  boolean evaluateJobMetrics(List<SingleMetric> metrics);
}
