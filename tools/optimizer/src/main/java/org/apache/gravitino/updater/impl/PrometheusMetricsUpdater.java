package org.apache.gravitino.updater.impl;

import org.apache.gravitino.monitor.api.Metrics;
import org.apache.gravitino.updater.api.MetricsUpdater;

// Update metrics to Prometheus
public class PrometheusMetricsUpdater implements MetricsUpdater {

  @Override
  public void updateMetrics(Metrics metrics) {}
}
