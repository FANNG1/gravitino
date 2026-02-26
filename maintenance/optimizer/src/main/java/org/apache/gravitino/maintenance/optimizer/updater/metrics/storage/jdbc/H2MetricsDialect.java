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

package org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.jdbc;

import java.util.List;

/** H2 dialect for JDBC metrics storage schema SQL. */
public class H2MetricsDialect implements JdbcMetricsDialect {

  @Override
  public String name() {
    return "h2";
  }

  @Override
  public String createTableMetricsSql(int partitionColumnLength) {
    return "CREATE TABLE IF NOT EXISTS table_metrics ("
        + "id BIGINT AUTO_INCREMENT PRIMARY KEY, "
        + "table_identifier VARCHAR(1024) NOT NULL, "
        + "metric_name VARCHAR(1024) NOT NULL, "
        + "table_partition VARCHAR("
        + partitionColumnLength
        + "), "
        + "metric_ts BIGINT NOT NULL, "
        + "metric_value VARCHAR(1024) NOT NULL"
        + ")";
  }

  @Override
  public String createJobMetricsSql() {
    return "CREATE TABLE IF NOT EXISTS job_metrics ("
        + "id BIGINT AUTO_INCREMENT PRIMARY KEY, "
        + "job_identifier VARCHAR(1024) NOT NULL, "
        + "metric_name VARCHAR(1024) NOT NULL, "
        + "metric_ts BIGINT NOT NULL, "
        + "metric_value VARCHAR(1024) NOT NULL"
        + ")";
  }

  @Override
  public List<String> createIndexesSql() {
    return List.of(
        "CREATE INDEX IF NOT EXISTS idx_table_metrics_metric_ts ON table_metrics(metric_ts)",
        "CREATE INDEX IF NOT EXISTS idx_job_metrics_metric_ts ON job_metrics(metric_ts)",
        "CREATE INDEX IF NOT EXISTS idx_table_metrics_composite ON table_metrics(table_identifier, table_partition, metric_ts)",
        "CREATE INDEX IF NOT EXISTS idx_job_metrics_identifier_metric_ts ON job_metrics(job_identifier, metric_ts)");
  }

  @Override
  public String alterPartitionColumnSql(int partitionColumnLength) {
    return "ALTER TABLE table_metrics ALTER COLUMN table_partition VARCHAR("
        + partitionColumnLength
        + ")";
  }
}
