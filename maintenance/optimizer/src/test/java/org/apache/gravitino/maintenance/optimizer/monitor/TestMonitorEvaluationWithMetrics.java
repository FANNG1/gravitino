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

package org.apache.gravitino.maintenance.optimizer.monitor;

import com.google.common.collect.ImmutableMap;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.StatisticsComputerContent;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.monitor.job.FileJobProvider;
import org.apache.gravitino.maintenance.optimizer.updater.UpdateType;
import org.apache.gravitino.maintenance.optimizer.updater.Updater;
import org.apache.gravitino.maintenance.optimizer.updater.computer.CliStatisticsComputer;
import org.apache.gravitino.maintenance.optimizer.updater.computer.LocalStatisticsComputer;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.H2MetricsStorage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestMonitorEvaluationWithMetrics {

  @Test
  void testEvaluateWithTableAndJobMetrics() throws Exception {
    Path metricsPath = Files.createTempDirectory("metrics-db").resolve("metrics-test");
    Path jobMappingFile = Files.createTempFile("job-mapping", ".jsonl");
    Files.writeString(
        jobMappingFile,
        "{\"identifier\":\"test.db.table\",\"job-identifiers\":[\"test.db.job1\"]}\n",
        StandardCharsets.UTF_8);

    Map<String, String> config =
        ImmutableMap.<String, String>builder()
            .put(OptimizerConfig.GRAVITINO_DEFAULT_CATALOG, "test")
            .put(OptimizerConfig.GRAVITINO_METALAKE, "test-metalake")
            .put(OptimizerConfig.JOB_PROVIDER_CONFIG.getKey(), FileJobProvider.NAME)
            .put(FileJobProvider.JOB_FILE_PATH_CONFIG, jobMappingFile.toString())
            .put(OptimizerConfig.MONITOR_CALLBACKS_CONFIG.getKey(), TestMonitorCallback.NAME)
            .put(
                OptimizerConfig.STATISTICS_UPDATER_CONFIG.getKey(),
                org.apache.gravitino.maintenance.optimizer.updater.TestStatisticsUpdater.NAME)
            .put(
                OptimizerConfig.OPTIMIZER_PREFIX
                    + "h2-metrics."
                    + H2MetricsStorage.H2MetricsStorageConfig.H2_METRICS_STORAGE_PATH,
                metricsPath.toString())
            .build();

    OptimizerEnv env = OptimizerEnv.getInstance();
    env.initialize(new OptimizerConfig(config));

    NameIdentifier tableIdentifier = NameIdentifier.parse("test.db.table");
    NameIdentifier jobIdentifier = NameIdentifier.parse("test.db.job1");

    Updater updater = new Updater(env);
    String tablePayloadBefore =
        "{\"identifier\":\"test.db.table\",\"stats-type\":\"table\",\"row_count\":100}";
    env.setContent(new StatisticsComputerContent(null, tablePayloadBefore));
    updater.update(
        LocalStatisticsComputer.LOCAL_STATISTICS_COMPUTER_NAME,
        List.of(tableIdentifier),
        UpdateType.METRICS);

    env.setContent(new StatisticsComputerContent(null, "job:duration=10"));
    updater.update(CliStatisticsComputer.NAME, List.of(jobIdentifier), UpdateType.METRICS);

    long actionTimeSeconds = (System.currentTimeMillis() / 1000) + 1;
    Thread.sleep(1100);

    String tablePayloadAfter =
        "{\"identifier\":\"test.db.table\",\"stats-type\":\"table\",\"row_count\":200}";
    env.setContent(new StatisticsComputerContent(null, tablePayloadAfter));
    updater.update(
        LocalStatisticsComputer.LOCAL_STATISTICS_COMPUTER_NAME,
        List.of(tableIdentifier),
        UpdateType.METRICS);

    env.setContent(new StatisticsComputerContent(null, "job:duration=20"));
    updater.update(CliStatisticsComputer.NAME, List.of(jobIdentifier), UpdateType.METRICS);

    TestMonitorCallback.reset(2);
    Monitor monitor = new Monitor(env);
    monitor.evaluateMetrics(tableIdentifier, actionTimeSeconds, 10, java.util.Optional.empty());

    Assertions.assertTrue(TestMonitorCallback.await(10, TimeUnit.SECONDS));
    List<org.apache.gravitino.maintenance.optimizer.api.monitor.EvaluationResult> results =
        TestMonitorCallback.results();

    org.apache.gravitino.maintenance.optimizer.api.monitor.EvaluationResult tableResult =
        results.stream()
            .filter(
                result ->
                    result.scope().type()
                        == org.apache.gravitino.maintenance.optimizer.api.monitor.MetricScope.Type
                            .TABLE)
            .findFirst()
            .orElseThrow();
    Assertions.assertTrue(tableResult.beforeMetrics().containsKey("row_count"));
    Assertions.assertTrue(tableResult.afterMetrics().containsKey("row_count"));
    long beforeRowCount =
        (long) tableResult.beforeMetrics().get("row_count").get(0).statistic().value().value();
    long afterRowCount =
        (long) tableResult.afterMetrics().get("row_count").get(0).statistic().value().value();
    Assertions.assertEquals(100L, beforeRowCount);
    Assertions.assertEquals(200L, afterRowCount);

    org.apache.gravitino.maintenance.optimizer.api.monitor.EvaluationResult jobResult =
        results.stream()
            .filter(
                result ->
                    result.scope().type()
                        == org.apache.gravitino.maintenance.optimizer.api.monitor.MetricScope.Type
                            .JOB)
            .findFirst()
            .orElseThrow();
    Assertions.assertTrue(jobResult.beforeMetrics().containsKey("duration"));
    Assertions.assertTrue(jobResult.afterMetrics().containsKey("duration"));
    long beforeDuration =
        (long) jobResult.beforeMetrics().get("duration").get(0).statistic().value().value();
    long afterDuration =
        (long) jobResult.afterMetrics().get("duration").get(0).statistic().value().value();
    Assertions.assertEquals(10L, beforeDuration);
    Assertions.assertEquals(20L, afterDuration);
  }
}
