package org.apache.gravitino.monitor.impl;

import java.util.List;
import org.apache.gravitino.monitor.api.Metrics;
import org.apache.gravitino.monitor.api.SingleMetric;

public class MetricsImpl implements Metrics {
  private final String name;
  private final List<SingleMetric> metrics;

  public MetricsImpl(String name, List<SingleMetric> metrics) {
    this.name = name;
    this.metrics = metrics;
  }

  @Override
  public String metricName() {
    return name;
  }

  @Override
  public List<SingleMetric> metricsList() {
    return metrics;
  }
}
