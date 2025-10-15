package org.apache.gravitino.updater.api;

import org.apache.gravitino.monitor.api.Metrics;

public interface MetricsUpdater {
  void updateMetrics(Metrics metrics);
}
