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

package org.apache.gravitino.maintenance.optimizer.updater.metrics.storage;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.time.Instant;
import org.apache.gravitino.NameIdentifier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestH2MetricsRepository {
  private H2MetricsRepository storage;

  @BeforeAll
  void setUp() {
    storage = new H2MetricsRepository();
    storage.initialize(ImmutableMap.of());
    storage.cleanupAllMetricsBefore(Long.MAX_VALUE);
  }

  @AfterAll
  void tearDown() {
    storage.cleanupAllMetricsBefore(Long.MAX_VALUE);
    storage.close();
  }

  @Test
  void testStoreAndRetrieveTableMetricsWithNullPartition() {
    NameIdentifier nameIdentifier = NameIdentifier.of("catalog", "db", "test_null_partition");
    long now = currentEpochSeconds();
    MetricRecord metric = new MetricRecordImpl(now, "value1");
    MetricRecord metric2 = new MetricRecordImpl(now, "value2");

    storage.storeTableMetric(nameIdentifier, "metric1", Optional.empty(), metric);
    storage.storeTableMetric(nameIdentifier, "metric2", Optional.empty(), metric);
    storage.storeTableMetric(nameIdentifier, "metric2", Optional.empty(), metric2);

    storage.storeTableMetric(
        NameIdentifier.of("catalog2", "db", "test_null_partition"),
        "metric1",
        Optional.empty(),
        metric);
    storage.storeTableMetric(
        NameIdentifier.of("catalog", "db2", "test_null_partition"),
        "metric1",
        Optional.empty(),
        metric);
    storage.storeTableMetric(
        NameIdentifier.of("catalog", "db", "test_null_partition2"),
        "metric1",
        Optional.empty(),
        metric);

    Map<String, List<MetricRecord>> metrics =
        storage.getTableMetrics(nameIdentifier, 0, Long.MAX_VALUE);

    Assertions.assertEquals(2, metrics.size());

    Assertions.assertTrue(metrics.containsKey("metric1"));
    Assertions.assertEquals(Arrays.asList("value1"), getMetricValues(metrics.get("metric1")));

    Assertions.assertTrue(metrics.containsKey("metric2"));
    Assertions.assertEquals(2, getMetricValues(metrics.get("metric2")).size());
    Assertions.assertEquals(
        Arrays.asList("value1", "value2"), getMetricValues(metrics.get("metric2")));
  }

  @Test
  void testStoreAndRetrieveMetricsWithNonNullPartition() {
    NameIdentifier nameIdentifier = NameIdentifier.of("catalog", "db", "test_non_null_partition");
    long now = currentEpochSeconds();
    MetricRecord metric = new MetricRecordImpl(now, "value1");
    MetricRecord metric2 = new MetricRecordImpl(now, "value2");
    MetricRecord metric3 = new MetricRecordImpl(now, "value3");

    String partition1 = "a=1/b=2";
    String partition2 = "a=1/b=3";

    storage.storeTableMetric(nameIdentifier, "metric", Optional.of(partition1), metric);
    storage.storeTableMetric(nameIdentifier, "metric", Optional.of(partition2), metric);
    storage.storeTableMetric(nameIdentifier, "metric2", Optional.of(partition2), metric2);
    storage.storeTableMetric(nameIdentifier, "metric2", Optional.of(partition2), metric3);

    Map<String, List<MetricRecord>> metrics =
        storage.getPartitionMetrics(nameIdentifier, partition1, 0, Long.MAX_VALUE);

    Assertions.assertEquals(1, metrics.size());
    Assertions.assertTrue(metrics.containsKey("metric"));
    Assertions.assertEquals(Arrays.asList("value1"), getMetricValues(metrics.get("metric")));

    metrics =
        storage.getPartitionMetrics(nameIdentifier, partition2, 0, Long.MAX_VALUE);
    Assertions.assertEquals(2, metrics.size());
    Assertions.assertTrue(metrics.containsKey("metric"));
    Assertions.assertEquals(Arrays.asList("value1"), getMetricValues(metrics.get("metric")));
    Assertions.assertTrue(metrics.containsKey("metric2"));
    Assertions.assertEquals(
        Arrays.asList("value2", "value3"), getMetricValues(metrics.get("metric2")));
  }

  @Test
  void testRetrieveMetricsWithNullAndNonNullPartition() {
    NameIdentifier nameIdentifier = NameIdentifier.of("test_mismatch_partition");
    MetricRecord metric = new MetricRecordImpl(0, "value1");
    MetricRecord metric2 = new MetricRecordImpl(1, "value2");
    MetricRecord metric3 = new MetricRecordImpl(2, "value3");

    String partition1 = "a=1/b=2";
    String partition2 = "a=1/b=3";

    storage.storeTableMetric(nameIdentifier, "metric1", Optional.empty(), metric);
    storage.storeTableMetric(nameIdentifier, "metric1", Optional.empty(), metric2);
    storage.storeTableMetric(nameIdentifier, "metric1", Optional.empty(), metric3);
    storage.storeTableMetric(nameIdentifier, "metric1", Optional.of(partition1), metric);
    storage.storeTableMetric(nameIdentifier, "metric1", Optional.of(partition2), metric2);
    storage.storeTableMetric(nameIdentifier, "metric1", Optional.of(partition2), metric3);

    Map<String, List<MetricRecord>> metrics =
        storage.getTableMetrics(nameIdentifier, 0, Long.MAX_VALUE);
    Assertions.assertEquals(1, metrics.size());
    Assertions.assertTrue(metrics.containsKey("metric1"));
    Assertions.assertEquals(
        Arrays.asList("value1", "value2", "value3"), getMetricValues(metrics.get("metric1")));

    metrics =
        storage.getPartitionMetrics(nameIdentifier, partition1, 0, Long.MAX_VALUE);
    Assertions.assertEquals(1, metrics.size());
    Assertions.assertTrue(metrics.containsKey("metric1"));
    Assertions.assertEquals(Arrays.asList("value1"), getMetricValues(metrics.get("metric1")));

    metrics =
        storage.getPartitionMetrics(nameIdentifier, partition2, 0, Long.MAX_VALUE);
    Assertions.assertEquals(1, metrics.size());
    Assertions.assertTrue(metrics.containsKey("metric1"));
    Assertions.assertEquals(
        Arrays.asList("value2", "value3"), getMetricValues(metrics.get("metric1")));
  }

  @Test
  void testCaseInsensitiveIdentifierPartitionAndMetricName() {
    NameIdentifier storedId = NameIdentifier.of("CATALOGX", "DBX", "TABLEX");
    NameIdentifier queryId = NameIdentifier.of("catalogx", "dbx", "tablex");
    MetricRecord metric = new MetricRecordImpl(currentEpochSeconds(), "v1");
    String storedPartition = "Region=US/Day=2025-01-01";
    String queryPartition = "region=us/day=2025-01-01";

    storage.storeTableMetric(storedId, "METRIC_UPPER", Optional.of(storedPartition), metric);
    storage.storeJobMetric(storedId, "JOB_METRIC", metric);

    Map<String, List<MetricRecord>> partitionMetrics =
        storage.getPartitionMetrics(queryId, queryPartition, 0, Long.MAX_VALUE);
    Assertions.assertTrue(partitionMetrics.containsKey("metric_upper"));
    Assertions.assertEquals(List.of("v1"), getMetricValues(partitionMetrics.get("metric_upper")));

    Map<String, List<MetricRecord>> jobMetrics =
        storage.getJobMetrics(queryId, 0, Long.MAX_VALUE);
    Assertions.assertTrue(jobMetrics.containsKey("job_metric"));
    Assertions.assertEquals(List.of("v1"), getMetricValues(jobMetrics.get("job_metric")));
  }

  @Test
  void testInitializeTwiceFails() {
    H2MetricsRepository repository = new H2MetricsRepository();
    repository.initialize(ImmutableMap.of());

    IllegalStateException e =
        Assertions.assertThrows(IllegalStateException.class, () -> repository.initialize(ImmutableMap.of()));
    Assertions.assertTrue(e.getMessage().contains("already been initialized"));
  }

  @Test
  void testInvalidTimeWindowFailsFast() {
    NameIdentifier id = NameIdentifier.of("catalog", "db", "table");

    IllegalArgumentException tableException =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> storage.getTableMetrics(id, 10, 10));
    Assertions.assertTrue(tableException.getMessage().contains("Invalid time window"));

    IllegalArgumentException partitionException =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> storage.getPartitionMetrics(id, "p=1", 20, 10));
    Assertions.assertTrue(partitionException.getMessage().contains("Invalid time window"));

    IllegalArgumentException jobException =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> storage.getJobMetrics(id, 30, 30));
    Assertions.assertTrue(jobException.getMessage().contains("Invalid time window"));
  }

  private List<String> getMetricValues(List<MetricRecord> metrics) {
    return metrics.stream().map(MetricRecord::getValue).toList();
  }

  private long currentEpochSeconds() {
    return Instant.now().getEpochSecond();
  }
}
