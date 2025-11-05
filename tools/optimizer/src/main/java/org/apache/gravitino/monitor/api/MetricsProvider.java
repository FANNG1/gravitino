package org.apache.gravitino.monitor.api;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.common.SinglePartition;

/**
 * Provider interface for retrieving job-related metrics. Implementations of this interface should
 * provide specific logic to fetch metrics for a given job.
 */
public interface MetricsProvider {
  /**
   * Retrieves metrics for a specific job within a specified time range.
   *
   * @param jobIdentifier The identifier of the job to retrieve metrics for
   * @param startTime The start timestamp (in seconds) of the time range to consider when fetching
   *     metrics
   * @param endTime The end timestamp (in seconds) of the time range to consider when fetching
   *     metrics
   * @return A list of {@link SingleMetric} objects containing the job metrics for the specified
   *     criteria
   */
  Map<String, List<SingleMetric>> jobMetricDetails(
      NameIdentifier jobIdentifier, long startTime, long endTime);

  Map<String, List<SingleMetric>> tableMetricDetails(
      NameIdentifier tableIdentifier,
      Optional<List<SinglePartition>> partitions,
      long startTime,
      long endTime);
}
