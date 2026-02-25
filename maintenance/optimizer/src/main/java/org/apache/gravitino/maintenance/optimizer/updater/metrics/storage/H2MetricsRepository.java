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

package org.apache.gravitino.maintenance.optimizer.updater.metrics.storage;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.utils.MapUtils;

/** H2-backed metrics repository that keeps file-url defaults and JDBC auto-detection behavior. */
public class H2MetricsRepository extends JdbcMetricsRepository {

  private static final String DEFAULT_H2_STORAGE_PATH = "./data/metrics.db";

  @Override
  public void initialize(Map<String, String> optimizerProperties) {
    Map<String, String> jdbcProperties =
        MapUtils.getPrefixMap(
            optimizerProperties,
            OptimizerConfig.OPTIMIZER_PREFIX + GenericJdbcMetricsRepository.JDBC_METRICS_PREFIX);

    String configuredJdbcUrl = jdbcProperties.get(GenericJdbcMetricsRepository.JDBC_URL);
    int partitionColumnLength =
        parseInt(jdbcProperties.get(GenericJdbcMetricsRepository.PARTITION_COLUMN_LENGTH), 1024);

    String jdbcUrl;
    if (StringUtils.isNotBlank(configuredJdbcUrl)) {
      jdbcUrl =
          isH2JdbcUrl(configuredJdbcUrl)
              ? constructH2JdbcUrl(configuredJdbcUrl)
              : configuredJdbcUrl;
    } else {
      jdbcUrl = constructH2JdbcUrl("jdbc:h2:file:" + resolveStoragePath(DEFAULT_H2_STORAGE_PATH));
    }

    Map<String, String> effectiveJdbcProperties = new HashMap<>(jdbcProperties);
    effectiveJdbcProperties.put(GenericJdbcMetricsRepository.JDBC_URL, jdbcUrl);
    effectiveJdbcProperties.putIfAbsent(GenericJdbcMetricsRepository.JDBC_USER, DEFAULT_USER);
    effectiveJdbcProperties.putIfAbsent(
        GenericJdbcMetricsRepository.JDBC_PASSWORD, DEFAULT_PASSWORD);
    effectiveJdbcProperties.putIfAbsent(GenericJdbcMetricsRepository.JDBC_DRIVER, "org.h2.Driver");

    DataSourceJdbcConnectionProvider connectionProvider =
        new DataSourceJdbcConnectionProvider(effectiveJdbcProperties);
    initializeStorage(connectionProvider, partitionColumnLength);
  }

  private String resolveStoragePath(String configuredPath) {
    Path path = Paths.get(configuredPath);
    if (path.isAbsolute()) {
      return configuredPath;
    }

    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    if (StringUtils.isBlank(gravitinoHome)) {
      return configuredPath;
    }

    return Paths.get(gravitinoHome, configuredPath).toString();
  }

  private String constructH2JdbcUrl(String originUrl) {
    String resolvedUrl = originUrl;
    if (!containsJdbcParam(resolvedUrl, "DB_CLOSE_DELAY")) {
      resolvedUrl = resolvedUrl + ";DB_CLOSE_DELAY=-1";
    }
    if (!containsJdbcParam(resolvedUrl, "MODE")) {
      resolvedUrl = resolvedUrl + ";MODE=MYSQL";
    }
    if (!isH2MemoryJdbcUrl(resolvedUrl) && !containsJdbcParam(resolvedUrl, "AUTO_SERVER")) {
      resolvedUrl = resolvedUrl + ";AUTO_SERVER=TRUE";
    }
    return resolvedUrl;
  }

  private boolean containsJdbcParam(String jdbcUrl, String paramName) {
    String upperUrl = jdbcUrl.toUpperCase(Locale.ROOT);
    String target = (paramName + "=").toUpperCase(Locale.ROOT);
    return upperUrl.contains(target);
  }

  private boolean isH2JdbcUrl(String jdbcUrl) {
    return jdbcUrl.toLowerCase(Locale.ROOT).startsWith("jdbc:h2:");
  }

  private boolean isH2MemoryJdbcUrl(String jdbcUrl) {
    return jdbcUrl.toLowerCase(Locale.ROOT).startsWith("jdbc:h2:mem:");
  }

  private int parseInt(String value, int defaultValue) {
    return StringUtils.isBlank(value) ? defaultValue : Integer.parseInt(value);
  }
}
