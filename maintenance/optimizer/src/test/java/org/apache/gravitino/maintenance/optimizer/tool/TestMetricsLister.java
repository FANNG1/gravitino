/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.maintenance.optimizer.tool;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.recommender.util.PartitionUtils;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.H2MetricsStorage;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.H2MetricsStorage.H2MetricsStorageConfig;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.MetricRecord;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.MetricRecordImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestMetricsLister {

  private OptimizerEnv optimizerEnv;
  private H2MetricsStorage storage;
  private Path metricsPath;

  @BeforeEach
  void setUp() throws Exception {
    metricsPath = Files.createTempFile("metrics-lister", ".db");
    Map<String, String> props = new HashMap<>();
    props.put(OptimizerConfig.GRAVITINO_URI, "http://localhost:8090");
    props.put(OptimizerConfig.GRAVITINO_METALAKE, "test");
    props.put(OptimizerConfig.GRAVITINO_DEFAULT_CATALOG, "generic");
    props.put(
        OptimizerConfig.OPTIMIZER_PREFIX
            + "h2-metrics."
            + H2MetricsStorageConfig.H2_METRICS_STORAGE_PATH,
        metricsPath.toString());

    optimizerEnv = OptimizerEnv.getInstance();
    optimizerEnv.initialize(new OptimizerConfig(props));

    storage = new H2MetricsStorage();
    storage.initialize(props);
    storage.cleanupAllMetricsBefore(System.currentTimeMillis());
  }

  @Test
  void listTableAndPartitionMetrics() {
    NameIdentifier tableId = NameIdentifier.of("generic", "db", "table1");
    MetricRecord m1 = new MetricRecordImpl(1L, "10");
    storage.storeTableMetric(tableId, "metric1", Optional.empty(), m1);

    PartitionPath partitionPath =
        PartitionPath.of(List.of(new PartitionEntryImpl("country", "US")));
    MetricRecord m2 = new MetricRecordImpl(2L, "20");
    storage.storeTableMetric(
        tableId, "metric1", Optional.of(PartitionUtils.encodePartitionPath(partitionPath)), m2);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    MetricsLister lister = new MetricsLister(optimizerEnv, new PrintStream(baos));

    lister.listTableMetrics(List.of(tableId), Optional.of(partitionPath), 0, Long.MAX_VALUE);

    String output = baos.toString(StandardCharsets.UTF_8);
    assertTrue(output.contains("OK generic.db.table1 [table] metric1"));
    assertTrue(output.contains("value=10"));
    assertTrue(output.contains("partition:"));
    assertTrue(output.contains("value=20"));
  }

  @Test
  void listJobMetrics() {
    NameIdentifier jobId = NameIdentifier.of("job_catalog", "job_schema", "job1");
    MetricRecord m1 = new MetricRecordImpl(3L, "30");
    storage.storeJobMetric(jobId, "job_metric", m1);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    MetricsLister lister = new MetricsLister(optimizerEnv, new PrintStream(baos));
    lister.listJobMetrics(List.of(jobId), 0, Long.MAX_VALUE);

    String output = baos.toString(StandardCharsets.UTF_8);
    assertTrue(output.contains("OK job_catalog.job_schema.job1 [job] job_metric"));
    assertTrue(output.contains("value=30"));
  }
}
