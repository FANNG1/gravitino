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

package com.pinterest.job;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.JobTemplate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestDummySpinnerJobExecutor {

  private DummySpinnerJobExecutor executor;
  private JobTemplate mockJobTemplate;

  @BeforeEach
  void setUp() {
    executor = new DummySpinnerJobExecutor();
    mockJobTemplate = Mockito.mock(JobTemplate.class);
    executor.initialize(new HashMap<>());
  }

  @Test
  void testSubmitJobSuccess() {
    Mockito.when(mockJobTemplate.name()).thenReturn("pinterest-compaction");
    String datasetName = "default.test_table";
    String partitionsWhereClause = "dt=2026-02-20";
    Map<String, String> customFields =
        Map.of("table", datasetName, "partitions", partitionsWhereClause);
    Mockito.when(mockJobTemplate.customFields()).thenReturn(customFields);

    String jobId = executor.submitJob(mockJobTemplate);

    Assertions.assertNotNull(jobId);
    Assertions.assertTrue(jobId.startsWith("iceberg_table_optimization_actor::dummy_run_"));
  }

  @Test
  void testSubmitJobWithUnknownTemplateNameThrowsException() {
    String templateName = "unknown-template";
    Mockito.when(mockJobTemplate.name()).thenReturn(templateName);
    Mockito.when(mockJobTemplate.customFields()).thenReturn(new HashMap<>());

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> executor.submitJob(mockJobTemplate));

    String expectedMessage = String.format("Unknown job template name: %s", templateName);
    Assertions.assertEquals(expectedMessage, exception.getMessage());
  }

  @Test
  void testGetJobStatusForSubmittedJob() throws NoSuchJobException {
    Mockito.when(mockJobTemplate.name()).thenReturn("pinterest-compaction");
    String datasetName = "default.test_table";
    Map<String, String> customFields = Map.of("table", datasetName);
    Mockito.when(mockJobTemplate.customFields()).thenReturn(customFields);

    String jobId = executor.submitJob(mockJobTemplate);
    JobHandle.Status status = executor.getJobStatus(jobId);

    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, status);
  }

  @Test
  void testGetJobStatusForNonExistentJobThrowsException() {
    String nonExistentJobId = "non_existent_dag::non_existent_run";

    NoSuchJobException exception =
        Assertions.assertThrows(
            NoSuchJobException.class, () -> executor.getJobStatus(nonExistentJobId));

    String expectedMessage = String.format("Job not found: %s", nonExistentJobId);
    Assertions.assertEquals(expectedMessage, exception.getMessage());
  }

  @Test
  void testCancelJobSuccess() throws NoSuchJobException {
    Mockito.when(mockJobTemplate.name()).thenReturn("pinterest-compaction");
    String datasetName = "default.test_table";
    Map<String, String> customFields = Map.of("table", datasetName);
    Mockito.when(mockJobTemplate.customFields()).thenReturn(customFields);

    String jobId = executor.submitJob(mockJobTemplate);
    executor.cancelJob(jobId);

    JobHandle.Status status = executor.getJobStatus(jobId);
    Assertions.assertEquals(JobHandle.Status.FAILED, status);
  }

  @Test
  void testCancelNonExistentJobThrowsException() {
    String nonExistentJobId = "non_existent_dag::non_existent_run";

    NoSuchJobException exception =
        Assertions.assertThrows(
            NoSuchJobException.class, () -> executor.cancelJob(nonExistentJobId));

    String expectedMessage = String.format("Job not found: %s", nonExistentJobId);
    Assertions.assertEquals(expectedMessage, exception.getMessage());
  }

  @Test
  void testClose() throws IOException, NoSuchJobException {
    Mockito.when(mockJobTemplate.name()).thenReturn("pinterest-compaction");
    String datasetName = "default.test_table";
    Map<String, String> customFields = Map.of("table", datasetName);
    Mockito.when(mockJobTemplate.customFields()).thenReturn(customFields);

    String jobId = executor.submitJob(mockJobTemplate);
    executor.close();

    NoSuchJobException exception =
        Assertions.assertThrows(NoSuchJobException.class, () -> executor.getJobStatus(jobId));

    String expectedMessage = String.format("Job not found: %s", jobId);
    Assertions.assertEquals(expectedMessage, exception.getMessage());
  }
}
