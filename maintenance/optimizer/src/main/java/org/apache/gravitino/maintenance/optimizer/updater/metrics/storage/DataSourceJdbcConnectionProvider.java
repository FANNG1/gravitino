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

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import org.apache.commons.dbcp2.BasicDataSource;

/** DataSource-backed JDBC connection provider with basic pooling defaults. */
public class DataSourceJdbcConnectionProvider {

  public static final int DEFAULT_MAX_TOTAL = 10;
  public static final int DEFAULT_MIN_IDLE = 2;
  public static final long DEFAULT_MAX_WAIT_MILLIS = 30_000L;
  public static final boolean DEFAULT_TEST_ON_BORROW = true;
  public static final String DEFAULT_VALIDATION_QUERY = "SELECT 1";

  private final BasicDataSource dataSource;

  public DataSourceJdbcConnectionProvider(Map<String, String> jdbcProperties) {
    JdbcConnectionConfig config = JdbcConnectionConfig.fromProperties(jdbcProperties);

    BasicDataSource ds = new BasicDataSource();
    ds.setUrl(config.jdbcUrl());
    ds.setUsername(config.username());
    ds.setPassword(config.password());
    if (config.driverClassName() != null && !config.driverClassName().isBlank()) {
      ds.setDriverClassName(config.driverClassName());
    }
    ds.setMaxTotal(config.maxTotal());
    ds.setMinIdle(config.minIdle());
    ds.setMaxWait(Duration.ofMillis(config.maxWaitMillis()));
    ds.setTestOnBorrow(config.testOnBorrow());
    if (config.testOnBorrow()) {
      ds.setValidationQuery(DEFAULT_VALIDATION_QUERY);
    }
    this.dataSource = ds;
  }

  public Connection getConnection() throws SQLException {
    return dataSource.getConnection();
  }

  public void close() {
    try {
      dataSource.close();
    } catch (SQLException e) {
      throw new MetricsStorageException("Failed to close JDBC DataSource", e);
    }
  }
}
