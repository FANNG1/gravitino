package org.apache.gravitino.monitor.api;

import java.util.List;

/**
 * Provider interface for retrieving job-related metrics. Implementations of this interface should
 * provide specific logic to fetch metrics for a given job.
 */
public interface JobMetricsProvider {
  /**
   * Retrieves metrics for a specific job within a specified time range.
   *
   * @param jobName The name of the job to retrieve metrics for
   * @param startTime The start timestamp (in seconds) of the time range to consider when fetching
   *     metrics
   * @param endTime The end timestamp (in seconds) of the time range to consider when fetching
   *     metrics
   * @return A list of {@link Metrics} objects containing the job metrics for the specified criteria
   */
  List<Metrics> jobMetrics(String jobName, long startTime, long endTime);
}
