package org.apache.gravitino.updater.impl;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.monitor.api.Metrics;
import org.apache.gravitino.updater.api.MetricsUpdater;

// Update metrics to h2
public class GravitinoMetricsUpdater implements MetricsUpdater {

  @Override
  public void updateTableMetrics(NameIdentifier nameIdentifier, Metrics metrics) {}

  @Override
  public void updateJobMetrics(NameIdentifier nameIdentifier, Metrics metrics) {}
}
