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
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricSample;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionMetricSample;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricScope;
import org.apache.gravitino.maintenance.optimizer.common.MetricSampleImpl;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.PartitionMetricSampleImpl;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.monitor.Monitor;
import org.apache.gravitino.maintenance.optimizer.monitor.callback.MonitorCallbackForTest;
import org.apache.gravitino.maintenance.optimizer.monitor.evaluator.MetricsEvaluatorForTest;
import org.apache.gravitino.maintenance.optimizer.monitor.job.JobProviderForTest;
import org.apache.gravitino.maintenance.optimizer.recommender.util.PartitionUtils;
import org.apache.gravitino.maintenance.optimizer.updater.StatisticEntryImpl;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.GravitinoMetricsUpdater;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

@SuppressWarnings("UnusedVariable")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MonitorIT {

  private GravitinoMetricsUpdater updater;
  private Monitor monitor;

  @BeforeAll
  public void setUp() {
    OptimizerEnv env = getOptimizerEnv();
    this.updater = new GravitinoMetricsUpdater();
    updater.initialize(env);
    this.monitor = new Monitor(env);
    MonitorCallbackForTest.reset();

    updater.cleanupJobMetricsBefore(Long.MAX_VALUE);
    updater.cleanupTableMetricsBefore(Long.MAX_VALUE);
  }

  @Test
  void testTableMetrics() {
    long actionTime = 10;
    long rangeSeconds = 2;
    NameIdentifier table1 = NameIdentifier.of("db", "table");
    NameIdentifier job1 = JobProviderForTest.job1;
    NameIdentifier job2 = JobProviderForTest.job2;

    // update table1, job1, job2 metrics by updater

    updater.updateTableMetrics(
        table1,
        Arrays.asList(
            new MetricSampleImpl(
                8, new StatisticEntryImpl("storage", StatisticValues.longValue(10))),
            new MetricSampleImpl(
                9, new StatisticEntryImpl("s3_cost", StatisticValues.longValue(1000))),
            new MetricSampleImpl(
                10, new StatisticEntryImpl("s3_cost", StatisticValues.longValue(1003))),
            new MetricSampleImpl(
                11, new StatisticEntryImpl("s3_cost", StatisticValues.longValue(1004))),
            new MetricSampleImpl(
                12, new StatisticEntryImpl("storage", StatisticValues.longValue(100L)))));

    monitor.run(table1, actionTime, rangeSeconds, Optional.empty());
    MetricsEvaluatorForTest evaluator = (MetricsEvaluatorForTest) monitor.metricsEvaluator();
    Map<String, List<MetricSample>> tableBeforeMetrics = evaluator.tableBeforeMetrics;

    Assertions.assertTrue(tableBeforeMetrics.containsKey("storage"));
    List<MetricSample> storageMetrics = tableBeforeMetrics.get("storage");
    Assertions.assertEquals(1, storageMetrics.size());
    Assertions.assertEquals(8, storageMetrics.get(0).timestamp());
    Assertions.assertEquals(10L, storageMetrics.get(0).statistic().value().value());

    Assertions.assertTrue(tableBeforeMetrics.containsKey("s3_cost"));
    List<MetricSample> s3CostMetrics = tableBeforeMetrics.get("s3_cost");
    Assertions.assertEquals(1, s3CostMetrics.size());
    Assertions.assertEquals(1000L, s3CostMetrics.get(0).statistic().value().value());
    Assertions.assertEquals(9, s3CostMetrics.get(0).timestamp());

    Map<String, List<MetricSample>> tableAfterMetrics = evaluator.tableAfterMetrics;
    Assertions.assertTrue(tableAfterMetrics.containsKey("storage"));
    storageMetrics = tableAfterMetrics.get("storage");
    Assertions.assertEquals(1, storageMetrics.size());
    Assertions.assertEquals(12, storageMetrics.get(0).timestamp());
    Assertions.assertEquals(100L, storageMetrics.get(0).statistic().value().value());

    Assertions.assertTrue(tableAfterMetrics.containsKey("s3_cost"));
    s3CostMetrics = tableAfterMetrics.get("s3_cost");
    Assertions.assertEquals(2, s3CostMetrics.size());
    Assertions.assertEquals(10, s3CostMetrics.get(0).timestamp());
    Assertions.assertEquals(1003L, s3CostMetrics.get(0).statistic().value().value());
    Assertions.assertEquals(11, s3CostMetrics.get(1).timestamp());
    Assertions.assertEquals(1004L, s3CostMetrics.get(1).statistic().value().value());
  }

  @Test
  void testPartitionMetrics() {
    long actionTime = 10;
    long rangeSeconds = 2;
    NameIdentifier table1 = NameIdentifier.of("db", "partitionTable");
    PartitionPath partitionPath =
        PartitionUtils.decodePartitionPath("[{\"country\":\"US\"},{\"region\":\"CA\"}]");

    updater.updateTableMetrics(
        table1,
        Arrays.asList(
            new PartitionMetricSampleImpl(
                8, new StatisticEntryImpl("storage", StatisticValues.longValue(10)), partitionPath),
            new PartitionMetricSampleImpl(
                12,
                new StatisticEntryImpl("storage", StatisticValues.longValue(20)),
                partitionPath),
            new PartitionMetricSampleImpl(
                11,
                new StatisticEntryImpl("s3_cost", StatisticValues.longValue(5)),
                partitionPath)));

    monitor.run(table1, actionTime, rangeSeconds, Optional.empty(), Optional.of(partitionPath));
    MetricsEvaluatorForTest evaluator = (MetricsEvaluatorForTest) monitor.metricsEvaluator();

    Map<String, List<MetricSample>> tableBeforeMetrics = evaluator.tableBeforeMetrics;
    Assertions.assertTrue(tableBeforeMetrics.containsKey("storage"));
    List<MetricSample> storageBefore = tableBeforeMetrics.get("storage");
    Assertions.assertEquals(1, storageBefore.size());
    assertPartitionMetric(storageBefore.get(0), partitionPath, 8, 10L);

    Map<String, List<MetricSample>> tableAfterMetrics = evaluator.tableAfterMetrics;
    Assertions.assertTrue(tableAfterMetrics.containsKey("storage"));
    List<MetricSample> storageAfter = tableAfterMetrics.get("storage");
    Assertions.assertEquals(1, storageAfter.size());
    assertPartitionMetric(storageAfter.get(0), partitionPath, 12, 20L);

    Assertions.assertTrue(tableAfterMetrics.containsKey("s3_cost"));
    List<MetricSample> s3After = tableAfterMetrics.get("s3_cost");
    Assertions.assertEquals(1, s3After.size());
    assertPartitionMetric(s3After.get(0), partitionPath, 11, 5L);
  }

  @Test
  void testJobMetrics() {
    long actionTime = 10;
    long rangeSeconds = 2;
    NameIdentifier job1 = JobProviderForTest.job1;
    NameIdentifier job2 = JobProviderForTest.job2;

    List<MetricSample> jobMetrics =
        Arrays.asList(
            new MetricSampleImpl(
                8, new StatisticEntryImpl("job_runtime", StatisticValues.longValue(8))),
            new MetricSampleImpl(
                9, new StatisticEntryImpl("job_cost", StatisticValues.longValue(9))),
            new MetricSampleImpl(
                10, new StatisticEntryImpl("job_cost", StatisticValues.longValue(10))),
            new MetricSampleImpl(
                100, new StatisticEntryImpl("job_cost", StatisticValues.longValue(11))),
            new MetricSampleImpl(
                12, new StatisticEntryImpl("job_runtime", StatisticValues.longValue(12L))));

    // update job1, job2 metrics by updater
    updater.updateJobMetrics(job1, jobMetrics);
    updater.updateJobMetrics(job2, jobMetrics);

    monitor.run(NameIdentifier.of("db", "table"), actionTime, rangeSeconds, Optional.empty());
    MetricsEvaluatorForTest evaluator = (MetricsEvaluatorForTest) monitor.metricsEvaluator();
    checkJobMetrics(evaluator.jobBeforeMetrics1, evaluator.jobAfterMetrics1);
    checkJobMetrics(evaluator.jobBeforeMetrics2, evaluator.jobAfterMetrics2);
  }

  @Test
  void testMonitorCallbacks() {
    MonitorCallbackForTest.reset();
    NameIdentifier table = NameIdentifier.of("db", "table");

    monitor.run(table, 10, 1, Optional.empty());

    Assertions.assertEquals(3, MonitorCallbackForTest.INVOCATIONS.get());
    Assertions.assertTrue(
        MonitorCallbackForTest.RESULTS.stream()
            .anyMatch(result -> result.scope().type() == MetricScope.Type.TABLE));
  }

  private void checkJobMetrics(
      Map<String, List<MetricSample>> jobBeforeMetrics,
      Map<String, List<MetricSample>> jobAfterMetrics) {
    Assertions.assertTrue(jobBeforeMetrics.containsKey("job_runtime"));
    List<MetricSample> job_runtimeMetrics = jobBeforeMetrics.get("job_runtime");
    Assertions.assertEquals(1, job_runtimeMetrics.size());
    Assertions.assertEquals(8, job_runtimeMetrics.get(0).timestamp());
    Assertions.assertEquals(8L, job_runtimeMetrics.get(0).statistic().value().value());

    Assertions.assertTrue(jobBeforeMetrics.containsKey("job_cost"));
    List<MetricSample> jobCostMetrics = jobBeforeMetrics.get("job_cost");
    Assertions.assertEquals(1, jobCostMetrics.size());
    Assertions.assertEquals(9L, jobCostMetrics.get(0).statistic().value().value());
    Assertions.assertEquals(9, jobCostMetrics.get(0).timestamp());

    Assertions.assertTrue(jobAfterMetrics.containsKey("job_runtime"));
    job_runtimeMetrics = jobAfterMetrics.get("job_runtime");
    Assertions.assertEquals(1, job_runtimeMetrics.size());
    Assertions.assertEquals(12, job_runtimeMetrics.get(0).timestamp());
    Assertions.assertEquals(12L, job_runtimeMetrics.get(0).statistic().value().value());

    Assertions.assertTrue(jobAfterMetrics.containsKey("job_cost"));
    jobCostMetrics = jobAfterMetrics.get("job_cost");
    Assertions.assertEquals(1, jobCostMetrics.size());
    Assertions.assertEquals(10, jobCostMetrics.get(0).timestamp());
    Assertions.assertEquals(10L, jobCostMetrics.get(0).statistic().value().value());
  }

  private void assertPartitionMetric(
      MetricSample metricSample,
      PartitionPath expectedPartition,
      long expectedTimestamp,
      long expectedValue) {
    Assertions.assertTrue(metricSample instanceof PartitionMetricSample);
    PartitionMetricSample partitionMetric = (PartitionMetricSample) metricSample;
    Assertions.assertEquals(expectedPartition, partitionMetric.partition());
    Assertions.assertEquals(expectedTimestamp, partitionMetric.timestamp());
    Assertions.assertEquals(expectedValue, partitionMetric.statistic().value().value());
  }

  private OptimizerEnv getOptimizerEnv() {
    Map<String, String> configs =
        ImmutableMap.<String, String>builder()
            .put(OptimizerConfig.METRICS_EVALUATOR, MetricsEvaluatorForTest.NAME)
            .put(OptimizerConfig.JOB_PROVIDER, JobProviderForTest.NAME)
            .put(OptimizerConfig.MONITOR_CALLBACKS, MonitorCallbackForTest.NAME)
            .build();

    OptimizerConfig config = new OptimizerConfig(configs);
    OptimizerEnv env = OptimizerEnv.getInstance();
    env.initialize(config);
    return env;
  }
}
