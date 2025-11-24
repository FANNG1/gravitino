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

package org.apache.gravitino.optimizer.integration.test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.optimizer.api.common.MetricsPoint;
import org.apache.gravitino.optimizer.api.common.PartitionEntry;
import org.apache.gravitino.optimizer.common.MetricPointImpl;
import org.apache.gravitino.optimizer.common.OptimizerEnv;
import org.apache.gravitino.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.optimizer.monitor.metrics.GravitinoMetricsProvider;
import org.apache.gravitino.optimizer.updater.PartitionStatisticEntryImpl;
import org.apache.gravitino.optimizer.updater.StatisticEntryImpl;
import org.apache.gravitino.optimizer.updater.metrics.GravitinoMetricsUpdater;
import org.apache.gravitino.optimizer.updater.metrics.storage.H2MetricsStorage.H2MetricsStorageConfig;
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

    List<PartitionEntry> partition1 =
        Arrays.asList(new PartitionEntryImpl("p1", "v1"), new PartitionEntryImpl("p2", "v2"));
    List<PartitionEntry> partition2 =
        Arrays.asList(new PartitionEntryImpl("p1", "v11"), new PartitionEntryImpl("p2", "v2"));

    updater.updateTableMetrics(
        tableIdentifier,
        Arrays.asList(
            new MetricPointImpl(1000, new StatisticEntryImpl("a", StatisticValues.longValue(10))),
            new MetricPointImpl(1000, new StatisticEntryImpl("b", StatisticValues.longValue(1000))),
            new MetricPointImpl(
                1000,
                new PartitionStatisticEntryImpl("b", StatisticValues.longValue(1003), partition1)),
            new MetricPointImpl(
                1000,
                new PartitionStatisticEntryImpl("b", StatisticValues.longValue(1004), partition2)),
            new MetricPointImpl(
                1001, new StatisticEntryImpl("a", StatisticValues.longValue(100L)))));

    Map<String, List<MetricsPoint>> metrics =
        provider.listTableMetrics(tableIdentifier, Optional.empty(), 1000, 1002);

    Assertions.assertEquals(2, metrics.size());

    Assertions.assertTrue(metrics.containsKey("a"));
    List<MetricsPoint> aMetrics = metrics.get("a");
    Assertions.assertEquals(2, aMetrics.size());
    Assertions.assertEquals(10L, aMetrics.get(0).statistic().value().value());
    Assertions.assertEquals(1000, aMetrics.get(0).timestamp());
    Assertions.assertEquals(100L, aMetrics.get(1).statistic().value().value());
    Assertions.assertEquals(1001, aMetrics.get(1).timestamp());

    Assertions.assertTrue(metrics.containsKey("b"));
    List<MetricsPoint> bMetrics = metrics.get("b");
    Assertions.assertEquals(1, bMetrics.size());
    Assertions.assertEquals(1000L, bMetrics.get(0).statistic().value().value());
    Assertions.assertEquals(1000, bMetrics.get(0).timestamp());

    Map<String, List<MetricsPoint>> partitionMetrics =
        provider.listTableMetrics(tableIdentifier, Optional.of(partition1), 1000, 1002);
    Assertions.assertEquals(1, partitionMetrics.size());

    Assertions.assertTrue(partitionMetrics.containsKey("b"));
    List<MetricsPoint> partitionMetrics1 = partitionMetrics.get("b");
    Assertions.assertEquals(1, partitionMetrics1.size());
    Assertions.assertTrue(
        partitionMetrics1.get(0).statistic() instanceof PartitionStatisticEntryImpl);
    Assertions.assertEquals(
        partition1,
        ((PartitionStatisticEntryImpl) partitionMetrics1.get(0).statistic()).partitionName());
    Assertions.assertEquals(1003L, partitionMetrics1.get(0).statistic().value().value());
    Assertions.assertEquals(1000, partitionMetrics1.get(0).timestamp());

    partitionMetrics =
        provider.listTableMetrics(tableIdentifier, Optional.of(partition2), 1000, 1002);
    Assertions.assertEquals(1, partitionMetrics.size());

    Assertions.assertTrue(partitionMetrics.containsKey("b"));
    List<MetricsPoint> partitionMetrics2 = partitionMetrics.get("b");
    Assertions.assertEquals(1, partitionMetrics2.size());
    Assertions.assertTrue(
        partitionMetrics2.get(0).statistic() instanceof PartitionStatisticEntryImpl);
    Assertions.assertEquals(
        partition2,
        ((PartitionStatisticEntryImpl) partitionMetrics2.get(0).statistic()).partitionName());
    Assertions.assertEquals(1004L, partitionMetrics2.get(0).statistic().value().value());
    Assertions.assertEquals(1000, partitionMetrics2.get(0).timestamp());
  }

  @Test
  void testJobMetrics() {
    NameIdentifier jobIdentifier = NameIdentifier.of("job1");

    updater.updateJobMetrics(
        jobIdentifier,
        Arrays.asList(
            new MetricPointImpl(2000, new StatisticEntryImpl("x", StatisticValues.longValue(20))),
            new MetricPointImpl(2000, new StatisticEntryImpl("y", StatisticValues.longValue(2000))),
            new MetricPointImpl(
                2001, new StatisticEntryImpl("x", StatisticValues.longValue(200L)))));

    Map<String, List<MetricsPoint>> metrics = provider.listJobMetrics(jobIdentifier, 2000, 2002);

    Assertions.assertEquals(2, metrics.size());

    Assertions.assertTrue(metrics.containsKey("x"));
    List<MetricsPoint> xMetrics = metrics.get("x");
    Assertions.assertEquals(2, xMetrics.size());
    Assertions.assertEquals(20L, xMetrics.get(0).statistic().value().value());
    Assertions.assertEquals(2000, xMetrics.get(0).timestamp());
    Assertions.assertEquals(200L, xMetrics.get(1).statistic().value().value());
    Assertions.assertEquals(2001, xMetrics.get(1).timestamp());

    Assertions.assertTrue(metrics.containsKey("y"));
    List<MetricsPoint> yMetrics = metrics.get("y");
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
