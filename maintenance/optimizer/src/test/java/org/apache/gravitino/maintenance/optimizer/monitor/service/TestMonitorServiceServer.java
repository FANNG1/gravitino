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

package org.apache.gravitino.maintenance.optimizer.monitor.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMonitorServiceServer {

  @Test
  void testHealthEndpoint() throws Exception {
    OptimizerConfig config =
        new OptimizerConfig(
            ImmutableMap.of(OptimizerConfig.MONITOR_SERVICE_PORT_CONFIG.getKey(), "0"));
    OptimizerEnv env = OptimizerEnv.getInstance();
    env.initialize(config);

    MonitorServiceServer server = new MonitorServiceServer(env);
    server.start();
    try {
      int port = server.localPort();
      URL url = new URL("http://localhost:" + port + "/health");
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setConnectTimeout(3000);
      connection.setReadTimeout(3000);
      Assertions.assertEquals(200, connection.getResponseCode());

      try (BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
        Assertions.assertEquals("OK", reader.readLine());
      }
    } finally {
      server.stop();
    }
  }

  @Test
  void testSubmitAndStatus() throws Exception {
    OptimizerConfig config =
        new OptimizerConfig(
            ImmutableMap.of(OptimizerConfig.MONITOR_SERVICE_PORT_CONFIG.getKey(), "0"));
    OptimizerEnv env = OptimizerEnv.getInstance();
    env.initialize(config);

    MonitorServiceServer server = new MonitorServiceServer(env);
    server.start();
    try {
      ObjectMapper mapper = new ObjectMapper();
      String payload =
          mapper.writeValueAsString(
              ImmutableMap.of(
                  "identifier", "db.table1", "actionTimeSeconds", 1, "rangeSeconds", 0));

      int port = server.localPort();
      URL url = new URL("http://localhost:" + port + "/v1/monitor");
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("POST");
      connection.setDoOutput(true);
      connection.setRequestProperty("Content-Type", "application/json");
      try (OutputStream output = connection.getOutputStream()) {
        output.write(payload.getBytes(StandardCharsets.UTF_8));
      }

      Assertions.assertEquals(202, connection.getResponseCode());
      Map<?, ?> submitResponse = mapper.readValue(connection.getInputStream(), Map.class);
      String monitorId = String.valueOf(submitResponse.get("monitorId"));
      Assertions.assertFalse(monitorId.isEmpty());

      URL statusUrl = new URL("http://localhost:" + port + "/v1/monitor/" + monitorId);
      HttpURLConnection statusConnection = (HttpURLConnection) statusUrl.openConnection();
      Assertions.assertEquals(200, statusConnection.getResponseCode());
      Map<?, ?> statusResponse = mapper.readValue(statusConnection.getInputStream(), Map.class);
      Assertions.assertEquals(monitorId, String.valueOf(statusResponse.get("monitorId")));
      Assertions.assertNotNull(statusResponse.get("tableMonitorDetailInfo"));
    } finally {
      server.stop();
    }
  }

  @Test
  void testListMonitors() throws Exception {
    OptimizerConfig config =
        new OptimizerConfig(
            ImmutableMap.of(OptimizerConfig.MONITOR_SERVICE_PORT_CONFIG.getKey(), "0"));
    OptimizerEnv env = OptimizerEnv.getInstance();
    env.initialize(config);

    MonitorServiceServer server = new MonitorServiceServer(env);
    server.start();
    try {
      ObjectMapper mapper = new ObjectMapper();
      int port = server.localPort();
      String monitorId = submitMonitor(mapper, port, "db.table.list");

      URL listUrl = new URL("http://localhost:" + port + "/v1/monitor");
      HttpURLConnection listConnection = (HttpURLConnection) listUrl.openConnection();
      Assertions.assertEquals(200, listConnection.getResponseCode());
      List<?> listResponse = mapper.readValue(listConnection.getInputStream(), List.class);
      Assertions.assertFalse(listResponse.isEmpty());

      boolean found = false;
      for (Object item : listResponse) {
        Map<?, ?> entry = (Map<?, ?>) item;
        if (monitorId.equals(String.valueOf(entry.get("monitorId")))) {
          Map<?, ?> basicInfo = (Map<?, ?>) entry.get("monitorBasicInfo");
          Assertions.assertEquals("db.table.list", String.valueOf(basicInfo.get("identifier")));
          found = true;
          break;
        }
      }
      Assertions.assertTrue(found);
    } finally {
      server.stop();
    }
  }

  @Test
  void testCancelMonitor() throws Exception {
    OptimizerConfig config =
        new OptimizerConfig(
            ImmutableMap.of(OptimizerConfig.MONITOR_SERVICE_PORT_CONFIG.getKey(), "0"));
    OptimizerEnv env = OptimizerEnv.getInstance();
    env.initialize(config);

    MonitorServiceServer server = new MonitorServiceServer(env);
    server.start();
    try {
      ObjectMapper mapper = new ObjectMapper();
      int port = server.localPort();
      String monitorId = submitMonitor(mapper, port, "db.table.cancel");

      URL cancelUrl = new URL("http://localhost:" + port + "/v1/monitor/" + monitorId + "/cancel");
      HttpURLConnection cancelConnection = (HttpURLConnection) cancelUrl.openConnection();
      cancelConnection.setRequestMethod("POST");
      Assertions.assertEquals(200, cancelConnection.getResponseCode());
      Map<?, ?> cancelResponse = mapper.readValue(cancelConnection.getInputStream(), Map.class);
      Assertions.assertEquals(monitorId, String.valueOf(cancelResponse.get("monitorId")));
      Map<?, ?> detailInfo = (Map<?, ?>) cancelResponse.get("tableMonitorDetailInfo");
      Assertions.assertEquals("CANCELED", String.valueOf(detailInfo.get("state")));
    } finally {
      server.stop();
    }
  }

  private String submitMonitor(ObjectMapper mapper, int port, String identifier) throws Exception {
    String payload =
        mapper.writeValueAsString(
            ImmutableMap.of("identifier", identifier, "actionTimeSeconds", 1, "rangeSeconds", 0));
    URL url = new URL("http://localhost:" + port + "/v1/monitor");
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("POST");
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    try (OutputStream output = connection.getOutputStream()) {
      output.write(payload.getBytes(StandardCharsets.UTF_8));
    }
    Assertions.assertEquals(202, connection.getResponseCode());
    Map<?, ?> submitResponse = mapper.readValue(connection.getInputStream(), Map.class);
    String monitorId = String.valueOf(submitResponse.get("monitorId"));
    Assertions.assertFalse(monitorId.isEmpty());
    return monitorId;
  }
}
