package org.apache.gravitino.monitor.api;

import java.util.List;
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
   * @return A list of {@link Metrics} objects containing the job metrics for the specified criteria
   */
  List<Metrics> jobMetricDetails(NameIdentifier jobIdentifier, long startTime, long endTime);

  List<Metrics> tableMetricDetails(
      NameIdentifier tableIdentifier,
      Optional<SinglePartition> partition,
      long startTime,
      long endTime);
}
