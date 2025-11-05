package org.apache.gravitino.monitor.api;

import java.util.List;

public interface Metrics {

  String metricName();

  List<SingleMetric> metricsList();
}
