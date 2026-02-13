/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.maintenance.optimizer.integration.test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricSample;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionMetricSample;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.common.MetricSampleImpl;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.PartitionMetricSampleImpl;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.monitor.metrics.GravitinoMetricsProvider;
import org.apache.gravitino.maintenance.optimizer.updater.StatisticEntryImpl;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.GravitinoMetricsUpdater;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.H2MetricsStorage.H2MetricsStorageConfig;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GravitinoMetricsIT {

  private GravitinoMetricsUpdater updater;
  private GravitinoMetricsProvider provider;

  @BeforeAll
  public void setUp() {
    OptimizerEnv optimizerEnv = getOptimizeEnv();
    updater = new GravitinoMetricsUpdater();
    updater.initialize(optimizerEnv);
    provider = new GravitinoMetricsProvider();
    provider.initialize(optimizerEnv);

    updater.cleanupTableMetricsBefore(Long.MAX_VALUE);
    updater.cleanupJobMetricsBefore(Long.MAX_VALUE);
  }

  @Test
  void testTableMetrics() {
    NameIdentifier tableIdentifier = NameIdentifier.of("catalog", "schema", "table");

    PartitionPath partition1 =
        PartitionPath.of(
            Arrays.asList(new PartitionEntryImpl("p1", "v1"), new PartitionEntryImpl("p2", "v2")));
    PartitionPath partition2 =
        PartitionPath.of(
            Arrays.asList(new PartitionEntryImpl("p1", "v11"), new PartitionEntryImpl("p2", "v2")));

    updater.updateTableMetrics(
        tableIdentifier,
        Arrays.asList(
            new MetricSampleImpl(1000, new StatisticEntryImpl("a", StatisticValues.longValue(10))),
            new MetricSampleImpl(
                1000, new StatisticEntryImpl("b", StatisticValues.longValue(1000))),
            new PartitionMetricSampleImpl(
                1000, new StatisticEntryImpl("b", StatisticValues.longValue(1003)), partition1),
            new PartitionMetricSampleImpl(
                1000, new StatisticEntryImpl("b", StatisticValues.longValue(1004)), partition2),
            new MetricSampleImpl(
                1001, new StatisticEntryImpl("a", StatisticValues.longValue(100L)))));

    Map<String, List<MetricSample>> metrics = provider.tableMetrics(tableIdentifier, 1000, 1002);

    Assertions.assertEquals(2, metrics.size());

    Assertions.assertTrue(metrics.containsKey("a"));
    List<MetricSample> aMetrics = metrics.get("a");
    Assertions.assertEquals(2, aMetrics.size());
    Assertions.assertEquals(10L, aMetrics.get(0).statistic().value().value());
    Assertions.assertEquals(1000, aMetrics.get(0).timestamp());
    Assertions.assertEquals(100L, aMetrics.get(1).statistic().value().value());
    Assertions.assertEquals(1001, aMetrics.get(1).timestamp());

    Assertions.assertTrue(metrics.containsKey("b"));
    List<MetricSample> bMetrics = metrics.get("b");
    Assertions.assertEquals(1, bMetrics.size());
    Assertions.assertEquals(1000L, bMetrics.get(0).statistic().value().value());
    Assertions.assertEquals(1000, bMetrics.get(0).timestamp());

    Map<String, List<MetricSample>> partitionMetrics =
        provider.partitionMetrics(tableIdentifier, partition1, 1000, 1002);
    Assertions.assertEquals(1, partitionMetrics.size());

    Assertions.assertTrue(partitionMetrics.containsKey("b"));
    List<MetricSample> partitionMetrics1 = partitionMetrics.get("b");
    Assertions.assertEquals(1, partitionMetrics1.size());
    Assertions.assertTrue(partitionMetrics1.get(0) instanceof PartitionMetricSample);
    PartitionMetricSample metricSample1 = (PartitionMetricSample) partitionMetrics1.get(0);
    Assertions.assertEquals(partition1, metricSample1.partition());
    Assertions.assertEquals(1003L, metricSample1.statistic().value().value());
    Assertions.assertEquals(1000, metricSample1.timestamp());

    partitionMetrics = provider.partitionMetrics(tableIdentifier, partition2, 1000, 1002);
    Assertions.assertEquals(1, partitionMetrics.size());

    Assertions.assertTrue(partitionMetrics.containsKey("b"));
    List<MetricSample> partitionMetrics2 = partitionMetrics.get("b");
    Assertions.assertEquals(1, partitionMetrics2.size());
    Assertions.assertTrue(partitionMetrics2.get(0) instanceof PartitionMetricSample);
    PartitionMetricSample metricSample2 = (PartitionMetricSample) partitionMetrics2.get(0);
    Assertions.assertEquals(partition2, metricSample2.partition());
    Assertions.assertEquals(1004L, metricSample2.statistic().value().value());
    Assertions.assertEquals(1000, metricSample2.timestamp());
  }

  @Test
  void testJobMetrics() {
    NameIdentifier jobIdentifier = NameIdentifier.of("job1");

    updater.updateJobMetrics(
        jobIdentifier,
        Arrays.asList(
            new MetricSampleImpl(2000, new StatisticEntryImpl("x", StatisticValues.longValue(20))),
            new MetricSampleImpl(
                2000, new StatisticEntryImpl("y", StatisticValues.longValue(2000))),
            new MetricSampleImpl(
                2001, new StatisticEntryImpl("x", StatisticValues.longValue(200L)))));

    Map<String, List<MetricSample>> metrics = provider.jobMetrics(jobIdentifier, 2000, 2002);

    Assertions.assertEquals(2, metrics.size());

    Assertions.assertTrue(metrics.containsKey("x"));
    List<MetricSample> xMetrics = metrics.get("x");
    Assertions.assertEquals(2, xMetrics.size());
    Assertions.assertEquals(20L, xMetrics.get(0).statistic().value().value());
    Assertions.assertEquals(2000, xMetrics.get(0).timestamp());
    Assertions.assertEquals(200L, xMetrics.get(1).statistic().value().value());
    Assertions.assertEquals(2001, xMetrics.get(1).timestamp());

    Assertions.assertTrue(metrics.containsKey("y"));
    List<MetricSample> yMetrics = metrics.get("y");
    Assertions.assertEquals(1, yMetrics.size());
    Assertions.assertEquals(2000L, yMetrics.get(0).statistic().value().value());
    Assertions.assertEquals(2000, yMetrics.get(0).timestamp());
  }

  private OptimizerEnv getOptimizeEnv() {
    OptimizerEnv optimizerEnv = OptimizerEnv.getInstance();
    Map config = ImmutableMap.of(H2MetricsStorageConfig.H2_METRICS_STORAGE_PATH, "/tmp/metrics.db");
    optimizerEnv.initialize(new OptimizerConfig(config));
    return optimizerEnv;
  }
}
