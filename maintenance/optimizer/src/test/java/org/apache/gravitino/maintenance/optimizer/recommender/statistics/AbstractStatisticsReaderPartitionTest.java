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

package org.apache.gravitino.maintenance.optimizer.recommender.statistics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AbstractStatisticsReaderPartitionTest {

  private static final String DEFAULT_CATALOG = "cat";

  @Test
  void readPartitionStatsUsesDefaultCatalogAndPreservesOrder() {
    String payload =
        "{\"identifier\":\"schema.table\",\"stats-type\":\"partition\","
            + "\"partition-path\":{\"p1\":\"v1\",\"p2\":\"v2\"},\"stats1\":\"100\"}";
    StatisticsReader reader = new PayloadStatisticsReader(payload, DEFAULT_CATALOG);
    NameIdentifier identifier = NameIdentifier.of(DEFAULT_CATALOG, "schema", "table");

    Map<PartitionPath, List<StatisticEntry<?>>> stats = reader.readPartitionStatistics(identifier);

    assertEquals(1, stats.size());
    PartitionPath path = stats.keySet().iterator().next();
    List<PartitionEntry> entries = path.entries();
    assertEquals(2, entries.size());
    assertEquals("p1", entries.get(0).partitionName());
    assertEquals("v1", entries.get(0).partitionValue());
    assertEquals("p2", entries.get(1).partitionName());
    assertEquals("v2", entries.get(1).partitionValue());
    List<StatisticEntry<?>> partitionStats = stats.get(path);
    assertNotNull(partitionStats);
    assertEquals(1, partitionStats.size());
    assertEquals(100L, partitionStats.get(0).value().value());
  }

  @Test
  void duplicatePartitionEntriesUseLastWins() {
    String payload =
        "{\"identifier\":\"schema.table\",\"stats-type\":\"partition\","
            + "\"partition-path\":{\"p\":\"a\"},\"stats1\":\"100\"}\n"
            + "{\"identifier\":\"schema.table\",\"stats-type\":\"partition\","
            + "\"partition-path\":{\"p\":\"a\"},\"stats1\":\"200\"}";
    StatisticsReader reader = new PayloadStatisticsReader(payload, DEFAULT_CATALOG);
    NameIdentifier identifier = NameIdentifier.of(DEFAULT_CATALOG, "schema", "table");

    Map<PartitionPath, List<StatisticEntry<?>>> stats = reader.readPartitionStatistics(identifier);

    assertEquals(1, stats.size());
    List<StatisticEntry<?>> partitionStats = stats.values().iterator().next();
    assertEquals(1, partitionStats.size());
    assertEquals(200L, partitionStats.get(0).value().value());
  }

  @Test
  void skipMalformedPartitionPath() {
    String payload =
        "{\"identifier\":\"schema.table\",\"stats-type\":\"partition\","
            + "\"partition-path\":\"not-an-object\",\"stats1\":\"100\"}";
    StatisticsReader reader = new PayloadStatisticsReader(payload, DEFAULT_CATALOG);
    NameIdentifier identifier = NameIdentifier.of(DEFAULT_CATALOG, "schema", "table");

    Map<PartitionPath, List<StatisticEntry<?>>> stats = reader.readPartitionStatistics(identifier);

    assertTrue(stats.isEmpty());
  }

  @Test
  void readAllPartitionStatsAggregatesMultipleTables() {
    String payload =
        "{\"identifier\":\"schema.table1\",\"stats-type\":\"partition\","
            + "\"partition-path\":{\"p\":\"a\"},\"stats1\":\"1\"}\n"
            + "{\"identifier\":\"schema.table2\",\"stats-type\":\"partition\","
            + "\"partition-path\":{\"p\":\"b\"},\"stats1\":\"2\"}";
    StatisticsReader reader = new PayloadStatisticsReader(payload, DEFAULT_CATALOG);

    Map<NameIdentifier, Map<PartitionPath, List<StatisticEntry<?>>>> stats =
        reader.readAllPartitionStatistics();

    assertEquals(2, stats.size());
    Map<PartitionPath, List<StatisticEntry<?>>> table1 =
        stats.get(NameIdentifier.of(DEFAULT_CATALOG, "schema", "table1"));
    assertNotNull(table1);
    assertEquals(1, table1.size());
    assertEquals(1L, table1.values().iterator().next().get(0).value().value());

    Map<PartitionPath, List<StatisticEntry<?>>> table2 =
        stats.get(NameIdentifier.of(DEFAULT_CATALOG, "schema", "table2"));
    assertNotNull(table2);
    assertEquals(1, table2.size());
    assertEquals(2L, table2.values().iterator().next().get(0).value().value());
  }

  @Test
  void fileReaderReturnsEmptyForEmptyFile(@TempDir Path tempDir) throws IOException {
    Path statsFile = tempDir.resolve("stats.json");
    Files.writeString(statsFile, "");
    StatisticsReader reader = new FileStatisticsReader(statsFile, DEFAULT_CATALOG);

    assertTrue(reader.readAllPartitionStatistics().isEmpty());
  }
}
