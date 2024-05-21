/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.metrics.source;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.datastrato.gravitino.Config;
import java.lang.management.ManagementFactory;

public class JVMMetricsSource extends MetricsSource {
  public JVMMetricsSource(Config config) {
    super(MetricsSource.JVM_METRIC_NAME, config);
    MetricRegistry metricRegistry = getMetricRegistry();
    metricRegistry.registerAll(new GarbageCollectorMetricSet());
    metricRegistry.registerAll(new MemoryUsageGaugeSet());
    metricRegistry.registerAll(new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
  }
}
