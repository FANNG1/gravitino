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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;

public class SpinnerJobClient implements Closeable {
  private static final String DEFAULT_HOST = "localhost:19193";
  private static final String DEFAULT_CLUSTER = "soxpiispinner2.pinadmin.com";
  private static final String HEADER_CONFIG_PREFIX = "header.";

  private CloseableHttpClient httpClient;
  private final ObjectMapper objectMapper;
  private final String cluster;
  private final String pinterestSpinnerApiPathPrefix;
  private final Map<String, String> defaultHeaders;

  public SpinnerJobClient(Map<String, String> configs) {
    this.httpClient = HttpClients.createDefault();
    this.objectMapper = new ObjectMapper();
    this.cluster = configs.getOrDefault("cluster", DEFAULT_CLUSTER);
    String address = configs.getOrDefault("address", DEFAULT_HOST);
    this.pinterestSpinnerApiPathPrefix = String.format("http://%s/api/pinterest/v1", address);
    this.defaultHeaders = extractDefaultHeaders(configs);
  }

  private Map<String, String> extractDefaultHeaders(Map<String, String> configs) {
    Map<String, String> headers = new HashMap<>();
    configs.forEach(
        (key, value) -> {
          if (key.startsWith(HEADER_CONFIG_PREFIX)) {
            String headerName = key.substring(HEADER_CONFIG_PREFIX.length());
            headers.put(headerName, value);
          }
        });
    return headers;
  }

  private void applyDefaultHeaders(HttpRequest request) {
    for (Map.Entry<String, String> header : defaultHeaders.entrySet()) {
      request.setHeader(header.getKey(), header.getValue());
    }
  }

  @VisibleForTesting
  void setHttpClient(CloseableHttpClient httpClient) {
    this.httpClient = httpClient;
  }

  public DagRun submitDagRun(String dagId, Map<String, Object> configs) throws IOException {
    String endpoint = String.format("%s/dags/%s/dag_runs", pinterestSpinnerApiPathPrefix, dagId);
    HttpPost httpPost = new HttpPost(endpoint);
    applyDefaultHeaders(httpPost);
    httpPost.setHeader("Host", cluster);

    Map<String, Object> requestBody = new HashMap<>();
    requestBody.put("conf", configs);

    String jsonBody = objectMapper.writeValueAsString(requestBody);
    StringEntity entity = new StringEntity(jsonBody, ContentType.APPLICATION_JSON);
    httpPost.setEntity(entity);

    return httpClient.execute(
        httpPost,
        response -> {
          int statusCode = response.getCode();
          if (statusCode >= HttpStatus.SC_OK && statusCode < HttpStatus.SC_REDIRECTION) {
            String responseBody =
                EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            JsonNode jsonResponse = objectMapper.readTree(responseBody);
            String responseDagId = jsonResponse.get("dag_id").asText();
            String executionDateTime = jsonResponse.get("execution_date").asText();
            String runId = jsonResponse.get("run_id").asText();
            return new DagRun(responseDagId, executionDateTime, runId);
          } else {
            throw new IOException("Failed to submit DAG run. Response code: " + statusCode);
          }
        });
  }

  public DagRunState getDagRunState(String dagId, String executionDateTime)
      throws IOException, NoSuchJobException {
    validateExecutionDateTime(executionDateTime);

    String encodedDateTime = URLEncoder.encode(executionDateTime, StandardCharsets.UTF_8);
    String endpoint =
        String.format(
            "%s/dags/%s/dag_runs?execution_date_lte=%s&execution_date_gte=%s",
            pinterestSpinnerApiPathPrefix, dagId, encodedDateTime, encodedDateTime);
    HttpGet httpGet = new HttpGet(endpoint);
    applyDefaultHeaders(httpGet);
    httpGet.setHeader("Host", cluster);

    return httpClient.execute(
        httpGet,
        response -> {
          int statusCode = response.getCode();
          if (statusCode >= HttpStatus.SC_OK && statusCode < HttpStatus.SC_REDIRECTION) {
            String responseBody =
                EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            JsonNode jsonResponse = objectMapper.readTree(responseBody);

            if (!jsonResponse.isArray()) {
              throw new IOException("Expected array response from DAG runs list endpoint");
            }
            if (jsonResponse.isEmpty()) {
              throw new NoSuchJobException("DAG run not found.");
            }
            if (jsonResponse.size() > 1) {
              throw new IOException("Expected single DAG run but found " + jsonResponse.size());
            }

            JsonNode dagRun = jsonResponse.get(0);
            String state = dagRun.get("state").asText();
            return DagRunState.fromString(state);
          } else {
            throw new IOException("Failed to get DAG run status. Response code: " + statusCode);
          }
        });
  }

  public void cancelDagRun(String dagId, String executionDateTime) throws IOException {
    validateExecutionDateTime(executionDateTime);

    String endpoint =
        String.format("%s/dags/%s/dag_runs/failed", pinterestSpinnerApiPathPrefix, dagId);
    HttpPost httpPost = new HttpPost(endpoint);
    applyDefaultHeaders(httpPost);
    httpPost.setHeader("Host", cluster);

    Map<String, Object> requestBody = new HashMap<>();
    requestBody.put("execution_date", executionDateTime);

    String jsonBody = objectMapper.writeValueAsString(requestBody);
    StringEntity entity = new StringEntity(jsonBody, ContentType.APPLICATION_JSON);
    httpPost.setEntity(entity);

    httpClient.execute(
        httpPost,
        response -> {
          int statusCode = response.getCode();
          if (statusCode >= HttpStatus.SC_OK && statusCode < HttpStatus.SC_REDIRECTION) {
            return null; // no return value
          } else {
            throw new IOException("Failed to submit DAG run. Response code: " + statusCode);
          }
        });
  }

  private void validateExecutionDateTime(String executionDateTime) {
    if (executionDateTime == null || executionDateTime.isEmpty()) {
      throw new IllegalArgumentException("executionDateTime cannot be null or empty");
    }
    try {
      OffsetDateTime.parse(executionDateTime);
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid executionDateTime format: %s. Expected ISO-8601 format with timezone (e.g., 2026-03-05T23:41:30+00:00)",
              executionDateTime),
          e);
    }
  }

  @Override
  public void close() throws IOException {
    if (httpClient != null) {
      httpClient.close();
    }
  }
}
