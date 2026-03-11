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
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TestSpinnerJobClient {

  private SpinnerJobClient client;
  private CloseableHttpClient mockHttpClient;

  @BeforeEach
  void setUp() {
    client = new SpinnerJobClient(new HashMap<>());
    mockHttpClient = Mockito.mock(CloseableHttpClient.class);
    client.setHttpClient(mockHttpClient);
  }

  @Test
  void testSubmitDagRunSuccess() throws IOException, ParseException, URISyntaxException {
    String dagId = "test_dag";
    Map<String, Object> configs = Map.of("key", "value");
    String executionDateTime = "2026-03-05T23:41:30+00:00";
    String runId = "manual__2026-03-05T23:41:30+00:00";

    String responseJson =
        String.format(
            "{\"dag_id\":\"%s\",\"execution_date\":\"%s\",\"run_id\":\"%s\"}",
            dagId, executionDateTime, runId);

    ClassicHttpResponse mockResponse = Mockito.mock(ClassicHttpResponse.class);
    Mockito.when(mockResponse.getCode()).thenReturn(HttpStatus.SC_OK);
    Mockito.when(mockResponse.getEntity())
        .thenReturn(new StringEntity(responseJson, StandardCharsets.UTF_8));

    ArgumentCaptor<HttpPost> httpPostCaptor = ArgumentCaptor.forClass(HttpPost.class);
    Mockito.when(
            mockHttpClient.execute(
                httpPostCaptor.capture(), Mockito.any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpClientResponseHandler<?> handler = invocation.getArgument(1);
              return handler.handleResponse(mockResponse);
            });

    DagRun result = client.submitDagRun(dagId, configs);

    Assertions.assertEquals(dagId, result.getDagId());
    Assertions.assertEquals(executionDateTime, result.getExecutionDateTime());
    Assertions.assertEquals(runId, result.getRunId());
    // verify the request body
    HttpPost capturedRequest = httpPostCaptor.getValue();
    String requestBody = EntityUtils.toString(capturedRequest.getEntity(), StandardCharsets.UTF_8);

    String expectedRequestBody = "{\"conf\":{\"key\":\"value\"}}";
    Assertions.assertEquals(expectedRequestBody, requestBody);
    Assertions.assertTrue(
        capturedRequest.getUri().toString().contains("/dags/" + dagId + "/dag_runs"));
  }

  @Test
  void testSubmitDagRunFailure() throws IOException {
    String dagId = "test_dag";
    Map<String, Object> configs = Map.of("key", "value");

    ClassicHttpResponse mockResponse = Mockito.mock(ClassicHttpResponse.class);
    Mockito.when(mockResponse.getCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR);

    Mockito.when(
            mockHttpClient.execute(
                Mockito.any(HttpPost.class), Mockito.any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpClientResponseHandler<?> handler = invocation.getArgument(1);
              return handler.handleResponse(mockResponse);
            });

    IOException exception =
        Assertions.assertThrows(IOException.class, () -> client.submitDagRun(dagId, configs));

    Assertions.assertTrue(exception.getMessage().contains("Failed to submit DAG run"));
    Assertions.assertTrue(exception.getMessage().contains("500"));
  }

  @Test
  void testGetDagRunStateSuccess() throws IOException, NoSuchJobException, URISyntaxException {
    String dagId = "test_dag";
    String executionDateTime = "2026-03-05T23:41:30+00:00";

    String responseJson = "[{\"state\":\"running\"}]";

    ClassicHttpResponse mockResponse = Mockito.mock(ClassicHttpResponse.class);
    Mockito.when(mockResponse.getCode()).thenReturn(HttpStatus.SC_OK);
    Mockito.when(mockResponse.getEntity())
        .thenReturn(new StringEntity(responseJson, StandardCharsets.UTF_8));

    ArgumentCaptor<HttpGet> httpGetCaptor = ArgumentCaptor.forClass(HttpGet.class);
    Mockito.when(
            mockHttpClient.execute(
                httpGetCaptor.capture(), Mockito.any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpClientResponseHandler<?> handler = invocation.getArgument(1);
              return handler.handleResponse(mockResponse);
            });

    DagRunState result = client.getDagRunState(dagId, executionDateTime);

    Assertions.assertEquals(DagRunState.RUNNING, result);
    HttpGet capturedRequest = httpGetCaptor.getValue();
    String requestUri = capturedRequest.getUri().toString();

    Assertions.assertTrue(requestUri.contains("/dags/" + dagId + "/dag_runs?"));
    Assertions.assertTrue(
        requestUri.contains("execution_date_lte=2026-03-05T23%3A41%3A30%2B00%3A00"));
    Assertions.assertTrue(
        requestUri.contains("execution_date_gte=2026-03-05T23%3A41%3A30%2B00%3A00"));
  }

  @Test
  void testGetDagRunStateNotFound() throws IOException {
    String dagId = "test_dag";
    String executionDateTime = "2026-03-05T23:41:30+00:00";

    String responseJson = "[]";

    ClassicHttpResponse mockResponse = Mockito.mock(ClassicHttpResponse.class);
    Mockito.when(mockResponse.getCode()).thenReturn(HttpStatus.SC_OK);
    Mockito.when(mockResponse.getEntity())
        .thenReturn(new StringEntity(responseJson, StandardCharsets.UTF_8));

    Mockito.when(
            mockHttpClient.execute(
                Mockito.any(HttpGet.class), Mockito.any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpClientResponseHandler<?> handler = invocation.getArgument(1);
              return handler.handleResponse(mockResponse);
            });

    NoSuchJobException exception =
        Assertions.assertThrows(
            NoSuchJobException.class, () -> client.getDagRunState(dagId, executionDateTime));

    Assertions.assertEquals("DAG run not found.", exception.getMessage());
  }

  @Test
  void testGetDagRunStateMultipleResults() throws IOException {
    String dagId = "test_dag";
    String executionDateTime = "2026-03-05T23:41:30+00:00";

    String responseJson = "[{\"state\":\"running\"},{\"state\":\"queued\"}]";

    ClassicHttpResponse mockResponse = Mockito.mock(ClassicHttpResponse.class);
    Mockito.when(mockResponse.getCode()).thenReturn(HttpStatus.SC_OK);
    Mockito.when(mockResponse.getEntity())
        .thenReturn(new StringEntity(responseJson, StandardCharsets.UTF_8));

    Mockito.when(
            mockHttpClient.execute(
                Mockito.any(HttpGet.class), Mockito.any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpClientResponseHandler<?> handler = invocation.getArgument(1);
              return handler.handleResponse(mockResponse);
            });

    IOException exception =
        Assertions.assertThrows(
            IOException.class, () -> client.getDagRunState(dagId, executionDateTime));

    Assertions.assertTrue(exception.getMessage().contains("Expected single DAG run but found 2"));
  }

  @Test
  void testGetDagRunStateNonArrayResponse() throws IOException {
    String dagId = "test_dag";
    String executionDateTime = "2026-03-05T23:41:30+00:00";

    String responseJson = "{\"state\":\"running\"}";

    ClassicHttpResponse mockResponse = Mockito.mock(ClassicHttpResponse.class);
    Mockito.when(mockResponse.getCode()).thenReturn(HttpStatus.SC_OK);
    Mockito.when(mockResponse.getEntity())
        .thenReturn(new StringEntity(responseJson, StandardCharsets.UTF_8));

    Mockito.when(
            mockHttpClient.execute(
                Mockito.any(HttpGet.class), Mockito.any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpClientResponseHandler<?> handler = invocation.getArgument(1);
              return handler.handleResponse(mockResponse);
            });

    IOException exception =
        Assertions.assertThrows(
            IOException.class, () -> client.getDagRunState(dagId, executionDateTime));

    Assertions.assertTrue(
        exception.getMessage().contains("Expected array response from DAG runs list endpoint"));
  }

  @Test
  void testGetDagRunStateWithNullExecutionDateTime() {
    String dagId = "test_dag";

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> client.getDagRunState(dagId, null));

    Assertions.assertEquals("executionDateTime cannot be null or empty", exception.getMessage());
  }

  @Test
  void testGetDagRunStateWithEmptyExecutionDateTime() {
    String dagId = "test_dag";

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> client.getDagRunState(dagId, ""));

    Assertions.assertEquals("executionDateTime cannot be null or empty", exception.getMessage());
  }

  @Test
  void testGetDagRunStateWithInvalidExecutionDateTimeFormat() {
    String dagId = "test_dag";
    String invalidDateTime = "2026-03-05 23:41:30";

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> client.getDagRunState(dagId, invalidDateTime));

    Assertions.assertTrue(exception.getMessage().contains("Invalid executionDateTime format"));
    Assertions.assertTrue(exception.getMessage().contains(invalidDateTime));
  }

  @Test
  void testCancelDagRunSuccess() throws IOException, ParseException, URISyntaxException {
    String dagId = "test_dag";
    String executionDateTime = "2026-03-05T23:41:30+00:00";

    ClassicHttpResponse mockResponse = Mockito.mock(ClassicHttpResponse.class);
    Mockito.when(mockResponse.getCode()).thenReturn(HttpStatus.SC_OK);

    ArgumentCaptor<HttpPost> httpPostCaptor = ArgumentCaptor.forClass(HttpPost.class);
    Mockito.when(
            mockHttpClient.execute(
                httpPostCaptor.capture(), Mockito.any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpClientResponseHandler<?> handler = invocation.getArgument(1);
              return handler.handleResponse(mockResponse);
            });

    client.cancelDagRun(dagId, executionDateTime);

    HttpPost capturedRequest = httpPostCaptor.getValue();
    String requestBody = EntityUtils.toString(capturedRequest.getEntity(), StandardCharsets.UTF_8);

    String expectedRequestBody = String.format("{\"execution_date\":\"%s\"}", executionDateTime);
    Assertions.assertEquals(expectedRequestBody, requestBody);
    Assertions.assertTrue(
        capturedRequest.getUri().toString().contains("/dags/" + dagId + "/dag_runs/failed"));
  }

  @Test
  void testCancelDagRunFailure() throws IOException {
    String dagId = "test_dag";
    String executionDateTime = "2026-03-05T23:41:30+00:00";

    ClassicHttpResponse mockResponse = Mockito.mock(ClassicHttpResponse.class);
    Mockito.when(mockResponse.getCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR);

    Mockito.when(
            mockHttpClient.execute(
                Mockito.any(HttpPost.class), Mockito.any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpClientResponseHandler<?> handler = invocation.getArgument(1);
              return handler.handleResponse(mockResponse);
            });

    IOException exception =
        Assertions.assertThrows(
            IOException.class, () -> client.cancelDagRun(dagId, executionDateTime));

    Assertions.assertTrue(exception.getMessage().contains("Failed to submit DAG run"));
  }

  @Test
  void testCancelDagRunWithNullExecutionDateTime() {
    String dagId = "test_dag";

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> client.cancelDagRun(dagId, null));

    Assertions.assertEquals("executionDateTime cannot be null or empty", exception.getMessage());
  }

  @Test
  void testCancelDagRunWithEmptyExecutionDateTime() {
    String dagId = "test_dag";

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> client.cancelDagRun(dagId, ""));

    Assertions.assertEquals("executionDateTime cannot be null or empty", exception.getMessage());
  }

  @Test
  void testCancelDagRunWithInvalidExecutionDateTimeFormat() {
    String dagId = "test_dag";
    String invalidDateTime = "2026-03-05 23:41:30";

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> client.cancelDagRun(dagId, invalidDateTime));

    Assertions.assertTrue(exception.getMessage().contains("Invalid executionDateTime format"));
    Assertions.assertTrue(exception.getMessage().contains(invalidDateTime));
  }

  @Test
  void testCancelDagRunWithValidExecutionDateTimeFormats() throws IOException {
    String dagId = "test_dag";
    String[] validDateTimes = {
      "2026-03-05T23:41:30+00:00",
      "2026-03-05T23:41:30Z",
      "2026-03-05T23:41:30.123+00:00",
      "2026-03-05T23:41:30-05:00"
    };

    ClassicHttpResponse mockResponse = Mockito.mock(ClassicHttpResponse.class);
    Mockito.when(mockResponse.getCode()).thenReturn(HttpStatus.SC_OK);

    Mockito.when(
            mockHttpClient.execute(
                Mockito.any(HttpPost.class), Mockito.any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpClientResponseHandler<?> handler = invocation.getArgument(1);
              return handler.handleResponse(mockResponse);
            });

    for (String validDateTime : validDateTimes) {
      Assertions.assertDoesNotThrow(() -> client.cancelDagRun(dagId, validDateTime));
    }
  }

  @Test
  void testClose() throws IOException {
    CloseableHttpClient mockClient = Mockito.mock(CloseableHttpClient.class);
    SpinnerJobClient testClient = new SpinnerJobClient(new HashMap<>());
    testClient.setHttpClient(mockClient);

    testClient.close();

    Mockito.verify(mockClient).close();
  }
}
