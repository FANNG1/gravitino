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

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/** Parsed and validated JDBC connection settings for metrics storage. */
public class JdbcConnectionConfig {

  private final String jdbcUrl;
  private final String username;
  private final String password;
  private final String driverClassName;
  private final int maxTotal;
  private final int minIdle;
  private final long maxWaitMillis;
  private final boolean testOnBorrow;

  private JdbcConnectionConfig(
      String jdbcUrl,
      String username,
      String password,
      String driverClassName,
      int maxTotal,
      int minIdle,
      long maxWaitMillis,
      boolean testOnBorrow) {
    this.jdbcUrl = jdbcUrl;
    this.username = username;
    this.password = password;
    this.driverClassName = driverClassName;
    this.maxTotal = maxTotal;
    this.minIdle = minIdle;
    this.maxWaitMillis = maxWaitMillis;
    this.testOnBorrow = testOnBorrow;
  }

  public static JdbcConnectionConfig fromProperties(Map<String, String> jdbcProperties) {
    Preconditions.checkArgument(jdbcProperties != null, "jdbcProperties must not be null");
    String jdbcUrl = getString(jdbcProperties, GenericJdbcMetricsRepository.JDBC_URL, null);
    String username =
        getString(
            jdbcProperties,
            GenericJdbcMetricsRepository.JDBC_USER,
            JdbcMetricsRepository.DEFAULT_USER);
    String password =
        getString(
            jdbcProperties,
            GenericJdbcMetricsRepository.JDBC_PASSWORD,
            JdbcMetricsRepository.DEFAULT_PASSWORD);
    String driverClassName =
        getString(jdbcProperties, GenericJdbcMetricsRepository.JDBC_DRIVER, "");
    int maxTotal =
        getInt(
            jdbcProperties,
            GenericJdbcMetricsRepository.POOL_MAX_SIZE,
            DataSourceJdbcConnectionProvider.DEFAULT_MAX_TOTAL);
    int minIdle =
        getInt(
            jdbcProperties,
            GenericJdbcMetricsRepository.POOL_MIN_IDLE,
            DataSourceJdbcConnectionProvider.DEFAULT_MIN_IDLE);
    long maxWaitMillis =
        getLong(
            jdbcProperties,
            GenericJdbcMetricsRepository.CONNECTION_TIMEOUT_MS,
            DataSourceJdbcConnectionProvider.DEFAULT_MAX_WAIT_MILLIS);
    boolean testOnBorrow =
        getBoolean(
            jdbcProperties,
            GenericJdbcMetricsRepository.TEST_ON_BORROW,
            DataSourceJdbcConnectionProvider.DEFAULT_TEST_ON_BORROW);

    Preconditions.checkArgument(jdbcUrl != null && !jdbcUrl.isBlank(), "jdbcUrl must not be blank");
    Preconditions.checkArgument(
        username != null && !username.isBlank(), "username must not be blank");
    Preconditions.checkArgument(maxTotal > 0, "maxTotal must be positive");
    Preconditions.checkArgument(minIdle >= 0, "minIdle must be non-negative");
    Preconditions.checkArgument(maxWaitMillis > 0, "maxWaitMillis must be positive");

    return new JdbcConnectionConfig(
        jdbcUrl,
        username,
        password,
        driverClassName,
        maxTotal,
        minIdle,
        maxWaitMillis,
        testOnBorrow);
  }

  public String jdbcUrl() {
    return jdbcUrl;
  }

  public String username() {
    return username;
  }

  public String password() {
    return password;
  }

  public String driverClassName() {
    return driverClassName;
  }

  public int maxTotal() {
    return maxTotal;
  }

  public int minIdle() {
    return minIdle;
  }

  public long maxWaitMillis() {
    return maxWaitMillis;
  }

  public boolean testOnBorrow() {
    return testOnBorrow;
  }

  private static String getString(
      Map<String, String> jdbcProperties, String key, String defaultValue) {
    return jdbcProperties.getOrDefault(key, defaultValue);
  }

  private static int getInt(Map<String, String> jdbcProperties, String key, int defaultValue) {
    return parseInt(jdbcProperties.get(key), defaultValue);
  }

  private static long getLong(Map<String, String> jdbcProperties, String key, long defaultValue) {
    return parseLong(jdbcProperties.get(key), defaultValue);
  }

  private static boolean getBoolean(
      Map<String, String> jdbcProperties, String key, boolean defaultValue) {
    return parseBoolean(jdbcProperties.get(key), defaultValue);
  }

  private static int parseInt(String value, int defaultValue) {
    return StringUtils.isBlank(value) ? defaultValue : Integer.parseInt(value);
  }

  private static long parseLong(String value, long defaultValue) {
    return StringUtils.isBlank(value) ? defaultValue : Long.parseLong(value);
  }

  private static boolean parseBoolean(String value, boolean defaultValue) {
    return StringUtils.isBlank(value) ? defaultValue : Boolean.parseBoolean(value);
  }
}
