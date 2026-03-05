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

package org.apache.gravitino.maintenance.optimizer.recommender.job;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction.CompactionStrategyHandler;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestGravitinoJobSubmitter {
  @Test
  void testLoadJobAdapterReturnsCompactionAdapter() {
    GravitinoJobSubmitter submitter = new GravitinoJobSubmitter();
    GravitinoJobAdapter adapter = submitter.loadJobAdapter(CompactionStrategyHandler.NAME);
    Assertions.assertTrue(adapter instanceof GravitinoCompactionJobAdapter);
  }

  @Test
  void testLoadJobAdapterFallsBackToConfiguredClassName() {
    String jobTemplateName = "custom";
    OptimizerConfig config =
        new OptimizerConfig(
            Map.of(
                OptimizerConfig.GRAVITINO_URI,
                "http://localhost:8090",
                OptimizerConfig.GRAVITINO_METALAKE,
                "test-metalake",
                OptimizerConfig.JOB_ADAPTER_PREFIX + jobTemplateName + ".className",
                GravitinoCompactionJobAdapter.class.getName()));
    GravitinoJobSubmitter submitter = new GravitinoJobSubmitter();
    OptimizerEnv optimizerEnv = OptimizerEnv.getInstance();
    optimizerEnv.initialize(config);
    submitter.initialize(optimizerEnv);

    GravitinoJobAdapter adapter = submitter.loadJobAdapter(jobTemplateName);
    Assertions.assertTrue(adapter instanceof GravitinoCompactionJobAdapter);
  }

  @Test
  void testBuildJobConfigMergesWithExpectedPrecedence() {
    OptimizerConfig config =
        new OptimizerConfig(
            Map.of(
                OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "custom", "optimizer",
                OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "override", "optimizer"));
    GravitinoJobSubmitter submitter = new GravitinoJobSubmitter();
    OptimizerEnv optimizerEnv = OptimizerEnv.getInstance();
    optimizerEnv.initialize(config);
    submitter.initialize(optimizerEnv);

    JobExecutionContext context =
        buildTestJobExecutionContext(
            "db.table", Map.of("context", "context", "override", "context"), "compaction");

    GravitinoJobAdapter adapter = Mockito.mock(GravitinoJobAdapter.class);
    Mockito.when(adapter.jobConfig(context))
        .thenReturn(Map.of("table", "db.table", "options", "map('k','v')", "override", "adapter"));

    Map<String, String> jobConfig = GravitinoJobSubmitter.buildJobConfig(config, context, adapter);

    int expectedJobConfigSize = 4;
    Assertions.assertEquals(expectedJobConfigSize, jobConfig.size());
    Assertions.assertEquals("optimizer", jobConfig.get("custom"));
    Assertions.assertEquals("adapter", jobConfig.get("override"));
    Assertions.assertEquals("db.table", jobConfig.get("table"));
    Assertions.assertEquals("map('k','v')", jobConfig.get("options"));
    Mockito.verify(adapter, Mockito.times(1)).jobConfig(context);
  }

  @Test
  void testSupportsBatchJob() {
    GravitinoJobAdapter mockGravitinoJobAdapter = Mockito.mock(GravitinoJobAdapter.class);
    Mockito.when(mockGravitinoJobAdapter.supportsBatchJob()).thenReturn(false);

    GravitinoJobSubmitter submitter = Mockito.mock(GravitinoJobSubmitter.class);
    String templateName = "testTemplateName";
    Mockito.when(submitter.loadJobAdapter(templateName)).thenReturn(mockGravitinoJobAdapter);
    Mockito.when(submitter.supportsBatchJob(templateName)).thenCallRealMethod();

    boolean supportsBatchJob = submitter.supportsBatchJob(templateName);

    Assertions.assertFalse(supportsBatchJob);
    Mockito.verify(submitter, Mockito.times(1)).loadJobAdapter(templateName);
    Mockito.verify(mockGravitinoJobAdapter, Mockito.times(1)).supportsBatchJob();
  }

  @Test
  void testBuildJobConfigWithMultipleContexts() throws Exception {
    OptimizerConfig config =
        new OptimizerConfig(
            Map.of(OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "shared", "sharedValue"));

    String identifier1 = "db1.table1";
    JobExecutionContext context1 =
        buildTestJobExecutionContext(identifier1, Map.of(), "compaction");
    String identifier2 = "db2.table2";
    JobExecutionContext context2 =
        buildTestJobExecutionContext(identifier2, Map.of(), "compaction");
    String identifier3 = "db1.table3";
    JobExecutionContext context3 =
        buildTestJobExecutionContext(identifier3, Map.of(), "compaction");

    GravitinoJobAdapter adapter = Mockito.mock(GravitinoJobAdapter.class);
    String partitions1 = "dt=2026-02-24";
    Mockito.when(adapter.jobConfig(context1))
        .thenReturn(Map.of("table", identifier1, "partitions", partitions1));
    String partitions2 = "(dt=2026-02-23 AND hr=10) OR (dt=2026-02-24 AND hr=11)";
    Mockito.when(adapter.jobConfig(context2))
        .thenReturn(Map.of("table", identifier2, "partitions", partitions2));
    Mockito.when(adapter.jobConfig(context3)).thenReturn(Map.of("table", identifier3));

    List<JobExecutionContext> contextsList = List.of(context1, context2, context3);
    Map<String, String> jobConfig =
        GravitinoJobSubmitter.buildJobConfig(config, contextsList, adapter);

    Assertions.assertEquals(2, jobConfig.size());
    Assertions.assertEquals("sharedValue", jobConfig.get("shared"));
    Assertions.assertTrue(jobConfig.containsKey("jobs"));

    String jobsJson = jobConfig.get("jobs");
    ObjectMapper objectMapper = new ObjectMapper();
    List<Map<String, String>> jobs = objectMapper.readValue(jobsJson, new TypeReference<>() {});

    Assertions.assertEquals(3, jobs.size());

    Map<String, String> job1 = jobs.get(0);
    Assertions.assertEquals(identifier1, job1.get("table"));
    Assertions.assertEquals(partitions1, job1.get("partitions"));

    Map<String, String> job2 = jobs.get(1);
    Assertions.assertEquals(identifier2, job2.get("table"));
    Assertions.assertEquals(partitions2, job2.get("partitions"));

    Map<String, String> job3 = jobs.get(2);
    Assertions.assertEquals(identifier3, job3.get("table"));
    Assertions.assertFalse(job3.containsKey("partitions"));

    Mockito.verify(adapter, Mockito.times(1)).jobConfig(context1);
    Mockito.verify(adapter, Mockito.times(1)).jobConfig(context2);
    Mockito.verify(adapter, Mockito.times(1)).jobConfig(context3);
  }

  @Test
  void testBuildJobConfigWithEmptyContextList() {
    OptimizerConfig config =
        new OptimizerConfig(Map.of(OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "key", "value"));

    GravitinoJobAdapter adapter = Mockito.mock(GravitinoJobAdapter.class);

    Map<String, String> jobConfig =
        GravitinoJobSubmitter.buildJobConfig(config, java.util.List.of(), adapter);

    Assertions.assertEquals(1, jobConfig.size());
    Assertions.assertEquals("value", jobConfig.get("key"));
    Assertions.assertFalse(jobConfig.containsKey("jobs"));
    Mockito.verify(adapter, Mockito.never()).jobConfig(Mockito.any());
  }

  private JobExecutionContext buildTestJobExecutionContext(
      String identifier, Map<String, String> options, String templateName) {
    return new JobExecutionContext() {
      @Override
      public NameIdentifier nameIdentifier() {
        return NameIdentifier.parse(identifier);
      }

      @Override
      public Map<String, String> jobOptions() {
        return options;
      }

      @Override
      public String jobTemplateName() {
        return templateName;
      }
    };
  }
}
