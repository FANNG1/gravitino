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

package org.apache.gravitino.optimizer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.optimizer.api.common.PartitionStatistic;
import org.apache.gravitino.optimizer.api.common.SingleStatistic;
import org.apache.gravitino.optimizer.api.common.SingleStatistic.Name;
import org.apache.gravitino.optimizer.common.PartitionImpl;
import org.apache.gravitino.optimizer.recommender.impl.GravitinoTableStatsProvider;
import org.apache.gravitino.optimizer.updater.impl.GravitinoStatsUpdater;
import org.apache.gravitino.optimizer.updater.impl.PartitionStatisticImpl;
import org.apache.gravitino.optimizer.updater.impl.SingleStatisticImpl;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

public class GravitinoStatsIT extends BaseIT {

  protected static final String METALAKE_NAME = "test_metalake";
  protected static final String GRAVITINO_CATALOG_NAME = "iceberg";
  private static final String TEST_SCHEMA = "test_schema";
  private static final String TEST_TABLE = "test_table";
  private static final String TEST_PARTITION_TABLE = "test_partition_table";
  private static ContainerSuite containerSuite = ContainerSuite.getInstance();
  private Catalog catalogClient;
  private GravitinoStatsUpdater statsUpdater;
  private GravitinoTableStatsProvider statsProvider;
  private static final String STATS_PREFIX = "custom-";

  @BeforeAll
  void init() throws Exception {
    containerSuite.startPostgreSQLContainer(TestDatabaseName.GRAVITINO_STATS_IT);
    super.startIntegrationTest();
    initMetalakeAndCatalog();
    int gravitinoPort = getGravitinoServerPort();
    String uri = String.format("http://127.0.0.1:%d", gravitinoPort);
    this.statsUpdater = new GravitinoStatsUpdater();
    statsUpdater.initialize(uri, METALAKE_NAME, GRAVITINO_CATALOG_NAME);
    this.statsProvider = new GravitinoTableStatsProvider();
    statsProvider.initialize(uri, METALAKE_NAME, GRAVITINO_CATALOG_NAME);
  }

  @Test
  void testTableStatsUpdaterAndProvider() {
    statsUpdater.updateTableStatistics(
        NameIdentifier.of(TEST_SCHEMA, TEST_TABLE),
        Arrays.asList(
            new SingleStatisticImpl(STATS_PREFIX + "row_count", StatisticValues.longValue(1000)),
            new SingleStatisticImpl(
                STATS_PREFIX + "size_in_bytes", StatisticValues.longValue(1000000)),
            new SingleStatisticImpl(
                STATS_PREFIX + Name.DATAFILE_SIZE_MSE.name(),
                StatisticValues.doubleValue(10000.1))));

    List<SingleStatistic> stats =
        statsProvider.getTableStats(NameIdentifier.of(TEST_SCHEMA, TEST_TABLE));
    Assertions.assertEquals(3, stats.size());
    stats.stream()
        .forEach(
            stat -> {
              if (stat.name().equals(STATS_PREFIX + "row_count")) {
                Assertions.assertEquals(1000L, ((StatisticValues.LongValue) stat.value()).value());
              } else if (stat.name().equals(STATS_PREFIX + "size_in_bytes")) {
                Assertions.assertEquals(
                    1000000L, ((StatisticValues.LongValue) stat.value()).value());
              } else if (stat.name().equals(STATS_PREFIX + Name.DATAFILE_SIZE_MSE.name())) {
                Assertions.assertEquals(
                    10000.1, ((StatisticValues.DoubleValue) stat.value()).value());
              } else {
                Assertions.fail("Unexpected statistic name: " + stat.name());
              }
            });
  }

  @Test
  void testTablePartitionStatsUpdaterAndProvider() {
    statsUpdater.updateTableStatistics(
        NameIdentifier.of(TEST_SCHEMA, TEST_PARTITION_TABLE),
        Arrays.asList(
            new PartitionStatisticImpl(
                STATS_PREFIX + "partition_row_count",
                StatisticValues.longValue(500),
                Arrays.asList(new PartitionImpl("col1", "1"), new PartitionImpl("col2", "2"))),
            new PartitionStatisticImpl(
                STATS_PREFIX + "partition_size_in_bytes",
                StatisticValues.longValue(500000),
                Arrays.asList(new PartitionImpl("col1", "1"), new PartitionImpl("col2", "2")))));

    List<PartitionStatistic> stats =
        statsProvider.getPartitionStats(NameIdentifier.of(TEST_SCHEMA, TEST_PARTITION_TABLE));
    Assertions.assertEquals(2, stats.size());
    stats.stream()
        .forEach(
            stat -> {
              if (stat.name().equals(STATS_PREFIX + "partition_row_count")) {
                Assertions.assertEquals(500L, ((StatisticValues.LongValue) stat.value()).value());
              } else if (stat.name().equals(STATS_PREFIX + "partition_size_in_bytes")) {
                Assertions.assertEquals(
                    500000L, ((StatisticValues.LongValue) stat.value()).value());
              } else {
                Assertions.fail("Unexpected statistic name: " + stat.name());
              }
            });
  }

  private void initMetalakeAndCatalog() {
    GravitinoMetalake metalake = client.createMetalake(METALAKE_NAME, "", new HashMap<>());

    this.catalogClient =
        metalake.createCatalog(
            GRAVITINO_CATALOG_NAME,
            Catalog.Type.RELATIONAL,
            "lakehouse-iceberg",
            "comment",
            ImmutableMap.of(
                IcebergConstants.URI,
                getPGUri(),
                IcebergConstants.CATALOG_BACKEND,
                "jdbc",
                IcebergConstants.GRAVITINO_JDBC_DRIVER,
                "org.postgresql.Driver",
                IcebergConstants.GRAVITINO_JDBC_USER,
                getPGUser(),
                IcebergConstants.GRAVITINO_JDBC_PASSWORD,
                getPGPassword(),
                IcebergConstants.WAREHOUSE,
                "file:///tmp/"));

    catalogClient.asSchemas().createSchema(TEST_SCHEMA, "comment", ImmutableMap.of());
    catalogClient
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(TEST_SCHEMA, TEST_TABLE),
            new Column[] {Column.of("col_1", Types.IntegerType.get())},
            "comment",
            ImmutableMap.of());
    catalogClient
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(TEST_SCHEMA, TEST_PARTITION_TABLE),
            new Column[] {
              Column.of("col_1", Types.IntegerType.get(), "col1"),
              Column.of("col2", Types.IntegerType.get(), "col2"),
              Column.of("col3", Types.IntegerType.get(), "col3")
            },
            "comment",
            ImmutableMap.of(),
            new Transform[] {
              Transforms.identity("col_1"), Transforms.bucket(8, new String[] {"col2"})
            });
  }

  private String getPGUri() {
    return containerSuite.getPostgreSQLContainer().getJdbcUrl(TestDatabaseName.GRAVITINO_STATS_IT);
  }

  private String getPGUser() {
    return containerSuite.getPostgreSQLContainer().getUsername();
  }

  private String getPGPassword() {
    return containerSuite.getPostgreSQLContainer().getPassword();
  }
}
