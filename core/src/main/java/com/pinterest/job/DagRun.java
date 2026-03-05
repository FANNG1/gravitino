package com.pinterest.job;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class DagRun {
  private final String dagId;
  private final String executionDate;
  private final String runId;

  public DagRun(String dagId, String runId) {
    this(dagId, extractExecutionDate(runId), runId);
  }

  /**
   * Extract execution date from run id. E.g. if runId equals to
   * `manual__2025-12-24T21:58:52+00:00`, then return `2025-12-24T21:58:52+00:00`
   *
   * @param runId run id
   * @return execution date
   */
  private static String extractExecutionDate(String runId) {
    if (runId == null || runId.isEmpty()) {
      throw new IllegalArgumentException("runId cannot be null or empty");
    }
    String[] parts = runId.split("__");
    if (parts.length != 2) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid runId format: expected exactly 2 parts delimited by '__', but got %d parts in '%s'",
              parts.length, runId));
    }
    return parts[1];
  }
}
