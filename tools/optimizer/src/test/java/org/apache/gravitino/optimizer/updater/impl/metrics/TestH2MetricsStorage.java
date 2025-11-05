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

package org.apache.gravitino.optimizer.updater.impl.metrics;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestH2MetricsStorage {
  private H2MetricsStorage storage;

  @BeforeAll
  void setUp() {
    storage = new H2MetricsStorage();
    storage.cleanupAllMetricsBefore(System.currentTimeMillis());
  }

  @AfterAll
  void tearDown() {
    storage.cleanupAllMetricsBefore(System.currentTimeMillis());
    storage.close();
  }

  @Test
  void testStoreAndRetrieveTableMetricsWithNullPartition() {
    NameIdentifier nameIdentifier = NameIdentifier.of("catalog", "db", "test_null_partition");
    StorageMetric metric = new StorageMetricImpl(System.currentTimeMillis(), "value1");
    StorageMetric metric2 = new StorageMetricImpl(System.currentTimeMillis(), "value2");

    storage.storeTableMetrics(nameIdentifier, "metric1", Optional.empty(), metric);
    storage.storeTableMetrics(nameIdentifier, "metric2", Optional.empty(), metric);
    storage.storeTableMetrics(nameIdentifier, "metric2", Optional.empty(), metric2);

    storage.storeTableMetrics(
        NameIdentifier.of("catalog2", "db", "test_null_partition"),
        "metric1",
        Optional.empty(),
        metric);
    storage.storeTableMetrics(
        NameIdentifier.of("catalog", "db2", "test_null_partition"),
        "metric1",
        Optional.empty(),
        metric);
    storage.storeTableMetrics(
        NameIdentifier.of("catalog", "db", "test_null_partition2"),
        "metric1",
        Optional.empty(),
        metric);

    Map<String, List<StorageMetric>> metrics =
        storage.getAllTableMetrics(nameIdentifier, Optional.empty(), 0, System.currentTimeMillis());

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
    StorageMetric metric = new StorageMetricImpl(System.currentTimeMillis(), "value1");
    StorageMetric metric2 = new StorageMetricImpl(System.currentTimeMillis(), "value2");
    StorageMetric metric3 = new StorageMetricImpl(System.currentTimeMillis(), "value3");

    String partition1 = "a=1/b=2";
    String partition2 = "a=1/b=3";

    storage.storeTableMetrics(nameIdentifier, "metric", Optional.of(partition1), metric);
    storage.storeTableMetrics(nameIdentifier, "metric", Optional.of(partition2), metric);
    storage.storeTableMetrics(nameIdentifier, "metric2", Optional.of(partition2), metric2);
    storage.storeTableMetrics(nameIdentifier, "metric2", Optional.of(partition2), metric3);

    Map<String, List<StorageMetric>> metrics =
        storage.getAllTableMetrics(
            nameIdentifier, Optional.of(partition1), 0, System.currentTimeMillis());

    Assertions.assertEquals(1, metrics.size());
    Assertions.assertTrue(metrics.containsKey("metric"));
    Assertions.assertEquals(Arrays.asList("value1"), getMetricValues(metrics.get("metric")));

    metrics =
        storage.getAllTableMetrics(
            nameIdentifier, Optional.of(partition2), 0, System.currentTimeMillis());
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
    StorageMetric metric = new StorageMetricImpl(0, "value1");
    StorageMetric metric2 = new StorageMetricImpl(1, "value2");
    StorageMetric metric3 = new StorageMetricImpl(2, "value3");

    String partition1 = "a=1/b=2";
    String partition2 = "a=1/b=3";

    storage.storeTableMetrics(nameIdentifier, "metric1", Optional.empty(), metric);
    storage.storeTableMetrics(nameIdentifier, "metric1", Optional.empty(), metric2);
    storage.storeTableMetrics(nameIdentifier, "metric1", Optional.empty(), metric3);
    storage.storeTableMetrics(nameIdentifier, "metric1", Optional.of(partition1), metric);
    storage.storeTableMetrics(nameIdentifier, "metric1", Optional.of(partition2), metric2);
    storage.storeTableMetrics(nameIdentifier, "metric1", Optional.of(partition2), metric3);

    Map<String, List<StorageMetric>> metrics =
        storage.getAllTableMetrics(nameIdentifier, Optional.empty(), 0, System.currentTimeMillis());
    Assertions.assertEquals(1, metrics.size());
    Assertions.assertTrue(metrics.containsKey("metric1"));
    Assertions.assertEquals(
        Arrays.asList("value1", "value2", "value3"), getMetricValues(metrics.get("metric1")));

    metrics =
        storage.getAllTableMetrics(
            nameIdentifier, Optional.of(partition1), 0, System.currentTimeMillis());
    Assertions.assertEquals(1, metrics.size());
    Assertions.assertTrue(metrics.containsKey("metric1"));
    Assertions.assertEquals(Arrays.asList("value1"), getMetricValues(metrics.get("metric1")));

    metrics =
        storage.getAllTableMetrics(
            nameIdentifier, Optional.of(partition2), 0, System.currentTimeMillis());
    Assertions.assertEquals(1, metrics.size());
    Assertions.assertTrue(metrics.containsKey("metric1"));
    Assertions.assertEquals(
        Arrays.asList("value2", "value3"), getMetricValues(metrics.get("metric1")));
  }

  private List<String> getMetricValues(List<StorageMetric> metrics) {
    return metrics.stream().map(StorageMetric::getValue).toList();
  }
}
