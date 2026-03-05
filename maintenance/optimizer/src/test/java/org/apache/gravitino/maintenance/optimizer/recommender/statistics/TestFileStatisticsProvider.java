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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestFileStatisticsProvider {

  @TempDir Path tempDir;

  @Test
  void testGetTableStatsMergesAndParsesNumbers() throws IOException {
    Path statsFile = tempDir.resolve("stats.json");
    Files.write(
        statsFile,
        List.of(
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"table\",\"stats1\":1,\"stats2\":2.0}",
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"table\",\"stats2\":\"4.5\",\"stats3\":\"3\"}",
            "{\"identifier\":\"schema.table\",\"stats-type\":\"table\",\"stats4\":7}",
            "{\"identifier\":\"other.table\",\"stats-type\":\"table\",\"stats1\":100}",
            "{\"identifier\":\"metalake.catalog.schema.table\",\"stats-type\":\"table\",\"stats5\":9}",
            "{\"identifier\":\"table\",\"stats-type\":\"table\",\"stats6\":11}",
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"job\",\"stats4\":10}",
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"table\",\"ignored\":\"not-number\"}",
            "malformed json"));

    FileStatisticsProvider provider = new FileStatisticsProvider();
    OptimizerEnv optimizerEnv = new OptimizerEnv(createConfig(statsFile));
    provider.initialize(optimizerEnv);

    List<StatisticEntry<?>> stats =
        provider.tableStatistics(NameIdentifier.parse("catalog.schema.table"));
    Map<String, StatisticEntry<?>> statsByName = toMap(stats);

    Assertions.assertEquals(4, statsByName.size());
    Assertions.assertEquals(1L, statsByName.get("stats1").value().value());
    Assertions.assertEquals(4.5, statsByName.get("stats2").value().value());
    Assertions.assertEquals(3L, statsByName.get("stats3").value().value());
    Assertions.assertEquals(7L, statsByName.get("stats4").value().value());
  }

  @Test
  void testInitializeRequiresFilePath() {
    FileStatisticsProvider provider = new FileStatisticsProvider();
    OptimizerEnv optimizerEnv = new OptimizerEnv(new OptimizerConfig());

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> provider.initialize(optimizerEnv));
  }

  private OptimizerConfig createConfig(Path statsFile) {
    Map<String, String> configs = new HashMap<>();
    configs.put(FileStatisticsProvider.STATISTICS_FILE_PATH_CONFIG, statsFile.toString());
    configs.put(OptimizerConfig.GRAVITINO_DEFAULT_CATALOG_CONFIG.getKey(), "catalog");
    return new OptimizerConfig(configs);
  }

  private Map<String, StatisticEntry<?>> toMap(List<StatisticEntry<?>> stats) {
    Map<String, StatisticEntry<?>> map = new HashMap<>();
    for (StatisticEntry<?> stat : stats) {
      map.put(stat.name(), stat);
    }
    return map;
  }
}
