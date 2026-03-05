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

package org.apache.gravitino.maintenance.optimizer.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.StringUtils;

final class MonitorServiceCommandUtils {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private MonitorServiceCommandUtils() {}

  static String normalizeBaseUrl(String monitorServiceUrl) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(monitorServiceUrl),
        "Missing required option --monitor-service-url for monitor service command.");
    String url = monitorServiceUrl.trim();
    if (url.endsWith("/")) {
      return url.substring(0, url.length() - 1);
    }
    return url;
  }

  static Object doGet(String url) throws Exception {
    HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    connection.setRequestMethod("GET");
    connection.setConnectTimeout(5000);
    connection.setReadTimeout(5000);
    return readResponse(connection);
  }

  static Object doPost(String url, Object payload) throws Exception {
    HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    connection.setRequestMethod("POST");
    connection.setConnectTimeout(5000);
    connection.setReadTimeout(5000);
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    if (payload != null) {
      try (OutputStream output = connection.getOutputStream()) {
        output.write(OBJECT_MAPPER.writeValueAsBytes(payload));
      }
    }
    return readResponse(connection);
  }

  static String encodePathSegment(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  private static Object readResponse(HttpURLConnection connection) throws Exception {
    int status = connection.getResponseCode();
    InputStream bodyStream =
        status >= 200 && status < 300 ? connection.getInputStream() : connection.getErrorStream();
    if (bodyStream == null) {
      Preconditions.checkArgument(
          status >= 200 && status < 300, "Monitor service request failed with status %s", status);
      return null;
    }

    if (status < 200 || status >= 300) {
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(bodyStream, StandardCharsets.UTF_8))) {
        String message = reader.lines().reduce((left, right) -> left + "\n" + right).orElse("");
        throw new IllegalArgumentException(
            String.format(
                "Monitor service request failed, status=%d, message=%s", status, message));
      }
    }

    return OBJECT_MAPPER.readValue(bodyStream, Object.class);
  }
}
