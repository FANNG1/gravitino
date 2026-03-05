package com.pinterest.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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

  private final CloseableHttpClient httpClient;
  private final ObjectMapper objectMapper;
  private final String cluster;
  private final String pinterestSpinnerApiPathPrefix;
  private final String spinnerApiPathPrefix;
  private final Map<String, String> defaultHeaders;

  public SpinnerJobClient(Map<String, String> configs) {
    this.httpClient = HttpClients.createDefault();
    this.objectMapper = new ObjectMapper();
    this.cluster = configs.getOrDefault("cluster", DEFAULT_CLUSTER);
    String address = configs.getOrDefault("address", DEFAULT_HOST);
    this.pinterestSpinnerApiPathPrefix = String.format("http://%s/api/pinterest/v1", address);
    this.spinnerApiPathPrefix = String.format("http://%s/api/v1", address);
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
            String executionDate = jsonResponse.get("execution_date").asText();
            String runId = jsonResponse.get("run_id").asText();
            return new DagRun(responseDagId, executionDate, runId);
          } else {
            throw new IOException("Failed to submit DAG run. Response code: " + statusCode);
          }
        });
  }

  public String getDagRunState(String dagId, String runId) throws IOException, NoSuchJobException {
    String endpoint = String.format("%s/dags/%s/dagRuns/%s", spinnerApiPathPrefix, dagId, runId);
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
            return jsonResponse.get("state").asText();
          } else if (statusCode == HttpStatus.SC_NOT_FOUND) {
            throw new NoSuchJobException("DAG run not found.");
          } else {
            throw new IOException("Failed to get DAG run status. Response code: " + statusCode);
          }
        });
  }

  public void cancelDagRun(DagRun dagRun) throws IOException {
    String endpoint =
        String.format(
            "%s/dags/%s/dag_runs/failed", pinterestSpinnerApiPathPrefix, dagRun.getDagId());
    HttpPost httpPost = new HttpPost(endpoint);
    applyDefaultHeaders(httpPost);
    httpPost.setHeader("Host", cluster);

    Map<String, Object> requestBody = new HashMap<>();
    requestBody.put("execution_date", dagRun.getExecutionDate());

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

  @Override
  public void close() throws IOException {
    if (httpClient != null) {
      httpClient.close();
    }
  }
}
