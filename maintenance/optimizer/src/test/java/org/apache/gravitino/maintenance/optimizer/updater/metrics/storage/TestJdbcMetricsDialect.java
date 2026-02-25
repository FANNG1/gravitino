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

import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestJdbcMetricsDialect {

  @Test
  void testH2DialectSqlSmoke() {
    JdbcMetricsDialect dialect = new H2MetricsDialect();

    Assertions.assertEquals("h2", dialect.name());
    Assertions.assertTrue(
        dialect.createTableMetricsSql(2048).contains("table_partition VARCHAR(2048)"));
    Assertions.assertTrue(
        dialect.createJobMetricsSql().contains("CREATE TABLE IF NOT EXISTS job_metrics"));
    Assertions.assertTrue(
        dialect
            .alterPartitionColumnSql(4096)
            .contains("ALTER COLUMN table_partition VARCHAR(4096)"));

    List<String> indexes = dialect.createIndexesSql();
    Assertions.assertEquals(4, indexes.size());
    Assertions.assertTrue(indexes.get(0).contains("IF NOT EXISTS"));
  }
}
