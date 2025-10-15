package org.apache.gravitino.updater.api;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.monitor.api.Metrics;

public interface MetricsComputer extends Computer {
  Metrics computeMetrics(NameIdentifier tableIdentifier);
}
