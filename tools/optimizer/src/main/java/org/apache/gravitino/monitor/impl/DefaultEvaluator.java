package org.apache.gravitino.monitor.impl;

import java.util.List;
import org.apache.gravitino.monitor.api.Metrics;
import org.apache.gravitino.monitor.api.MetricsEvaluator;

public class DefaultEvaluator implements MetricsEvaluator {
  @SuppressWarnings("UnusedVariable")
  private long actionTime;

  @SuppressWarnings("UnusedVariable")
  private long rangeHours;

  @Override
  public void initialize(long actionTime, long rangeHours) {
    this.actionTime = actionTime;
    this.rangeHours = rangeHours;
  }

  @Override
  public boolean evaluateTableMetrics(List<Metrics> metrics) {
    //  evaluate table storage cost, data file size, data file number, position file number, etc
    return false;
  }

  @Override
  public boolean evaluateJobMetrics(List<Metrics> metrics) {
    // evaluate job cost, duration, etc
    return false;
  }
}
