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
import java.util.List;
import java.util.Map;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.JobTemplate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestSpinnerJobExecutor {

  private SpinnerJobExecutor executor;
  private JobTemplate mockJobTemplate;

  @BeforeEach
  void setUp() {
    executor = new SpinnerJobExecutor();
    mockJobTemplate = Mockito.mock(JobTemplate.class);
    executor.initialize(new HashMap<>());
  }

  @Test
  void testSubmitJobSuccess() throws IOException {
    Mockito.when(mockJobTemplate.name()).thenReturn("pinterest-compaction");
    String datasetName = "default.test_table";
    String partitionsWhereClause = "dt=2026-02-20";
    Map<String, String> customFields =
        Map.of("table", datasetName, "partitions", partitionsWhereClause);
    Mockito.when(mockJobTemplate.customFields()).thenReturn(customFields);

    String dagId = "iceberg_table_optimization_actor";
    String runId = "manual__2025-12-24T21:58:52+00:00";
    String query =
        String.format(
            "CALL system.rewrite_data_files(table => '%s', where => \"%s\")",
            datasetName, partitionsWhereClause);
    Map<String, Object> spinnerConfigs =
        Map.of("jobs", List.of(Map.of("dataset_name", datasetName, "query", query)));
    SpinnerJobClient mockClient = Mockito.mock(SpinnerJobClient.class);
    DagRun dagRun = new DagRun(dagId, runId);
    Mockito.when(mockClient.submitDagRun(dagId, spinnerConfigs)).thenReturn(dagRun);

    String jobId;
    try (SpinnerJobExecutor executorWithMock =
        new SpinnerJobExecutor() {
          @Override
          public void initialize(Map<String, String> configs) {
            this.spinnerClient = mockClient;
          }
        }) {
      executorWithMock.initialize(new HashMap<>());
      jobId = executorWithMock.submitJob(mockJobTemplate);
    }

    String expectedJobId = String.format("%s::%s", dagId, runId);
    Assertions.assertEquals(expectedJobId, jobId);
    Mockito.verify(mockClient).submitDagRun(dagId, spinnerConfigs);
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
  void testSubmitJobWithIOExceptionThrowsRuntimeException() throws IOException {
    Mockito.when(mockJobTemplate.name())
        .thenReturn(new CompactionSpinnerJobBuilder().getJobTemplateName());
    String datasetName = "default.test_table";
    String partitionsWhereClause = "dt=2026-02-20";
    Map<String, String> customFields =
        Map.of("table", datasetName, "partitions", partitionsWhereClause);
    Mockito.when(mockJobTemplate.customFields()).thenReturn(customFields);

    String query =
        String.format(
            "CALL system.rewrite_data_files(table => '%s', where => \"%s\")",
            datasetName, partitionsWhereClause);
    Map<String, Object> spinnerConfigs =
        Map.of("jobs", List.of(Map.of("dataset_name", datasetName, "query", query)));
    SpinnerJobClient mockClient = Mockito.mock(SpinnerJobClient.class);
    String dagId = "iceberg_table_optimization_actor";
    Mockito.when(mockClient.submitDagRun(dagId, spinnerConfigs))
        .thenThrow(new IOException("Connection failed"));

    RuntimeException exception;
    try (SpinnerJobExecutor executorWithMock =
        new SpinnerJobExecutor() {
          @Override
          public void initialize(Map<String, String> configs) {
            this.spinnerClient = mockClient;
          }
        }) {
      executorWithMock.initialize(new HashMap<>());
      exception =
          Assertions.assertThrows(
              RuntimeException.class, () -> executorWithMock.submitJob(mockJobTemplate));
    }

    String expectedMessage = "Failed to submit job";
    Assertions.assertEquals(expectedMessage, exception.getMessage());
    Assertions.assertInstanceOf(IOException.class, exception.getCause());
    Assertions.assertEquals("Connection failed", exception.getCause().getMessage());
    Mockito.verify(mockClient).submitDagRun(dagId, spinnerConfigs);
  }

  @Test
  void testGetJobStatusQueued() throws IOException, NoSuchJobException {
    String jobId = "pyang_dev::manual__2025-12-24T21:58:52+00:00";

    SpinnerJobClient mockClient = Mockito.mock(SpinnerJobClient.class);
    Mockito.when(mockClient.getDagRunState("pyang_dev", "manual__2025-12-24T21:58:52+00:00"))
        .thenReturn("queued");

    JobHandle.Status status;
    try (SpinnerJobExecutor executorWithMock =
        new SpinnerJobExecutor() {
          @Override
          public void initialize(Map<String, String> configs) {
            this.spinnerClient = mockClient;
          }
        }) {
      executorWithMock.initialize(new HashMap<>());
      status = executorWithMock.getJobStatus(jobId);
    }

    Assertions.assertEquals(JobHandle.Status.QUEUED, status);
  }

  @Test
  void testGetJobStatusRunning() throws IOException, NoSuchJobException {
    String jobId = "pyang_dev::manual__2025-12-24T21:58:52+00:00";

    SpinnerJobClient mockClient = Mockito.mock(SpinnerJobClient.class);
    Mockito.when(mockClient.getDagRunState("pyang_dev", "manual__2025-12-24T21:58:52+00:00"))
        .thenReturn("running");

    JobHandle.Status status;
    try (SpinnerJobExecutor executorWithMock =
        new SpinnerJobExecutor() {
          @Override
          public void initialize(Map<String, String> configs) {
            this.spinnerClient = mockClient;
          }
        }) {
      executorWithMock.initialize(new HashMap<>());
      status = executorWithMock.getJobStatus(jobId);
    }

    Assertions.assertEquals(JobHandle.Status.STARTED, status);
  }

  @Test
  void testGetJobStatusSucceeded() throws IOException, NoSuchJobException {
    String jobId = "pyang_dev::manual__2025-12-24T21:58:52+00:00";

    SpinnerJobClient mockClient = Mockito.mock(SpinnerJobClient.class);
    Mockito.when(mockClient.getDagRunState("pyang_dev", "manual__2025-12-24T21:58:52+00:00"))
        .thenReturn("success");

    JobHandle.Status status;
    try (SpinnerJobExecutor executorWithMock =
        new SpinnerJobExecutor() {
          @Override
          public void initialize(Map<String, String> configs) {
            this.spinnerClient = mockClient;
          }
        }) {
      executorWithMock.initialize(new HashMap<>());
      status = executorWithMock.getJobStatus(jobId);
    }

    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, status);
  }

  @Test
  void testGetJobStatusFailed() throws IOException, NoSuchJobException {
    String jobId = "pyang_dev::manual__2025-12-24T21:58:52+00:00";

    SpinnerJobClient mockClient = Mockito.mock(SpinnerJobClient.class);
    Mockito.when(mockClient.getDagRunState("pyang_dev", "manual__2025-12-24T21:58:52+00:00"))
        .thenReturn("failed");

    JobHandle.Status status;
    try (SpinnerJobExecutor executorWithMock =
        new SpinnerJobExecutor() {
          @Override
          public void initialize(Map<String, String> configs) {
            this.spinnerClient = mockClient;
          }
        }) {
      executorWithMock.initialize(new HashMap<>());
      status = executorWithMock.getJobStatus(jobId);
    }

    Assertions.assertEquals(JobHandle.Status.FAILED, status);
  }

  @Test
  void testGetJobStatusWithIOExceptionThrowsRuntimeException()
      throws IOException, NoSuchJobException {
    String jobId = "pyang_dev::manual__2025-12-24T21:58:52+00:00";

    SpinnerJobClient mockClient = Mockito.mock(SpinnerJobClient.class);
    Mockito.when(mockClient.getDagRunState("pyang_dev", "manual__2025-12-24T21:58:52+00:00"))
        .thenThrow(new IOException("Network error"));

    RuntimeException exception;
    try (SpinnerJobExecutor executorWithMock =
        new SpinnerJobExecutor() {
          @Override
          public void initialize(Map<String, String> configs) {
            this.spinnerClient = mockClient;
          }
        }) {
      executorWithMock.initialize(new HashMap<>());
      exception =
          Assertions.assertThrows(
              RuntimeException.class, () -> executorWithMock.getJobStatus(jobId));
    }

    String expectedMessage = String.format("Failed to get job status for job ID: %s", jobId);
    Assertions.assertEquals(expectedMessage, exception.getMessage());
    Assertions.assertInstanceOf(IOException.class, exception.getCause());
    Assertions.assertEquals("Network error", exception.getCause().getMessage());
  }

  @Test
  void testCancelJobSuccess() throws IOException, NoSuchJobException {
    String jobId = "pyang_dev::manual__2025-12-24T21:58:52+00:00";

    SpinnerJobClient mockClient = Mockito.mock(SpinnerJobClient.class);
    Mockito.doNothing().when(mockClient).cancelDagRun(Mockito.any(DagRun.class));

    try (SpinnerJobExecutor executorWithMock =
        new SpinnerJobExecutor() {
          @Override
          public void initialize(Map<String, String> configs) {
            this.spinnerClient = mockClient;
          }
        }) {
      executorWithMock.initialize(new HashMap<>());
      Assertions.assertDoesNotThrow(() -> executorWithMock.cancelJob(jobId));
    }
    Mockito.verify(mockClient).cancelDagRun(Mockito.any(DagRun.class));
  }

  @Test
  void testCancelJobWithIOExceptionThrowsRuntimeException() throws IOException, NoSuchJobException {
    String jobId = "pyang_dev::manual__2025-12-24T21:58:52+00:00";

    SpinnerJobClient mockClient = Mockito.mock(SpinnerJobClient.class);
    Mockito.doThrow(new IOException("Cancel failed"))
        .when(mockClient)
        .cancelDagRun(Mockito.any(DagRun.class));

    RuntimeException exception;
    try (SpinnerJobExecutor executorWithMock =
        new SpinnerJobExecutor() {
          @Override
          public void initialize(Map<String, String> configs) {
            this.spinnerClient = mockClient;
          }
        }) {
      executorWithMock.initialize(new HashMap<>());
      exception =
          Assertions.assertThrows(RuntimeException.class, () -> executorWithMock.cancelJob(jobId));
    }

    String expectedMessage = String.format("Failed to cancel job for job ID: %s", jobId);
    Assertions.assertEquals(expectedMessage, exception.getMessage());
    Assertions.assertInstanceOf(IOException.class, exception.getCause());
    Assertions.assertEquals("Cancel failed", exception.getCause().getMessage());
  }

  @Test
  void testClose() throws IOException {
    SpinnerJobClient mockClient = Mockito.mock(SpinnerJobClient.class);

    SpinnerJobExecutor executorWithMock =
        new SpinnerJobExecutor() {
          @Override
          public void initialize(Map<String, String> configs) {
            this.spinnerClient = mockClient;
          }
        };
    executorWithMock.initialize(new HashMap<>());

    executorWithMock.close();

    Mockito.verify(mockClient).close();
  }
}
