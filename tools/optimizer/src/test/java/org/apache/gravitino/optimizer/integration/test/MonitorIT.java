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
import org.apache.gravitino.optimizer.api.common.SingleMetric;
import org.apache.gravitino.optimizer.common.OptimizerEnv;
import org.apache.gravitino.optimizer.common.SingleMetricImpl;
import org.apache.gravitino.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.optimizer.monitor.Monitor;
import org.apache.gravitino.optimizer.monitor.evaluator.MetricsEvaluatorForTest;
import org.apache.gravitino.optimizer.monitor.job.JobProviderForTest;
import org.apache.gravitino.optimizer.updater.impl.GravitinoMetricsUpdater;
import org.apache.gravitino.optimizer.updater.impl.SingleStatisticImpl;
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
            new SingleMetricImpl(
                8, new SingleStatisticImpl("storage", StatisticValues.longValue(10))),
            new SingleMetricImpl(
                9, new SingleStatisticImpl("s3_cost", StatisticValues.longValue(1000))),
            new SingleMetricImpl(
                10, new SingleStatisticImpl("s3_cost", StatisticValues.longValue(1003))),
            new SingleMetricImpl(
                11, new SingleStatisticImpl("s3_cost", StatisticValues.longValue(1004))),
            new SingleMetricImpl(
                12, new SingleStatisticImpl("storage", StatisticValues.longValue(100L)))));

    monitor.run(table1, actionTime, rangeSeconds, Optional.empty());
    MetricsEvaluatorForTest evaluator = (MetricsEvaluatorForTest) monitor.metricsEvaluator();
    Map<String, List<SingleMetric>> tableBeforeMetrics = evaluator.tableBeforeMetrics;

    Assertions.assertTrue(tableBeforeMetrics.containsKey("storage"));
    List<SingleMetric> storageMetrics = tableBeforeMetrics.get("storage");
    Assertions.assertEquals(1, storageMetrics.size());
    Assertions.assertEquals(8, storageMetrics.get(0).timestamp());
    Assertions.assertEquals(10L, storageMetrics.get(0).statistic().value().value());

    Assertions.assertTrue(tableBeforeMetrics.containsKey("s3_cost"));
    List<SingleMetric> s3CostMetrics = tableBeforeMetrics.get("s3_cost");
    Assertions.assertEquals(1, s3CostMetrics.size());
    Assertions.assertEquals(1000L, s3CostMetrics.get(0).statistic().value().value());
    Assertions.assertEquals(9, s3CostMetrics.get(0).timestamp());

    Map<String, List<SingleMetric>> tableAfterMetrics = evaluator.tableAfterMetrics;
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
  void testJobMetrics() {
    long actionTime = 10;
    long rangeSeconds = 2;
    NameIdentifier job1 = JobProviderForTest.job1;
    NameIdentifier job2 = JobProviderForTest.job2;

    List<SingleMetric> jobMetrics =
        Arrays.asList(
            new SingleMetricImpl(
                8, new SingleStatisticImpl("job_runtime", StatisticValues.longValue(8))),
            new SingleMetricImpl(
                9, new SingleStatisticImpl("job_cost", StatisticValues.longValue(9))),
            new SingleMetricImpl(
                10, new SingleStatisticImpl("job_cost", StatisticValues.longValue(10))),
            new SingleMetricImpl(
                100, new SingleStatisticImpl("job_cost", StatisticValues.longValue(11))),
            new SingleMetricImpl(
                12, new SingleStatisticImpl("job_runtime", StatisticValues.longValue(12L))));

    // update job1, job2 metrics by updater
    updater.updateJobMetrics(job1, jobMetrics);
    updater.updateJobMetrics(job2, jobMetrics);

    monitor.run(NameIdentifier.of("db", "table"), actionTime, rangeSeconds, Optional.empty());
    MetricsEvaluatorForTest evaluator = (MetricsEvaluatorForTest) monitor.metricsEvaluator();
    checkJobMetrics(evaluator.jobBeforeMetrics1, evaluator.jobAfterMetrics1);
    checkJobMetrics(evaluator.jobBeforeMetrics2, evaluator.jobAfterMetrics2);
  }

  private void checkJobMetrics(
      Map<String, List<SingleMetric>> jobBeforeMetrics,
      Map<String, List<SingleMetric>> jobAfterMetrics) {
    Assertions.assertTrue(jobBeforeMetrics.containsKey("job_runtime"));
    List<SingleMetric> job_runtimeMetrics = jobBeforeMetrics.get("job_runtime");
    Assertions.assertEquals(1, job_runtimeMetrics.size());
    Assertions.assertEquals(8, job_runtimeMetrics.get(0).timestamp());
    Assertions.assertEquals(8L, job_runtimeMetrics.get(0).statistic().value().value());

    Assertions.assertTrue(jobBeforeMetrics.containsKey("job_cost"));
    List<SingleMetric> jobCostMetrics = jobBeforeMetrics.get("job_cost");
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

  private OptimizerEnv getOptimizerEnv() {
    Map<String, String> configs =
        ImmutableMap.of(
            OptimizerConfig.METRICS_EVALUATOR, MetricsEvaluatorForTest.NAME,
            OptimizerConfig.JOB_PROVIDER, JobProviderForTest.NAME);

    OptimizerConfig config = new OptimizerConfig(configs);
    OptimizerEnv env = OptimizerEnv.getInstance();
    env.initialize(config);
    return env;
  }
}
