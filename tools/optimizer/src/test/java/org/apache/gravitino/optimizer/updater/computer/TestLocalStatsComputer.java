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

package org.apache.gravitino.optimizer.updater.computer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.optimizer.common.OptimizerEnv;
import org.apache.gravitino.optimizer.common.StatsComputerContent;
import org.apache.gravitino.optimizer.common.conf.OptimizerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestLocalStatsComputer {

  @TempDir Path tempDir;

  @Test
  void testComputeTableStatsUsesReader() throws IOException {
    Path statsFile = tempDir.resolve("stats.json");
    Files.write(
        statsFile,
        List.of(
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"table\",\"s1\":1}",
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"table\",\"s1\":2}",
            "{\"identifier\":\"other\",\"stats-type\":\"table\",\"s1\":100}"));

    LocalStatsComputer computer = new LocalStatsComputer();
    OptimizerEnv env = OptimizerEnv.getInstance();
    env.initialize(createConfig());
    env.setContent(new StatsComputerContent(statsFile.toString(), null));
    computer.initialize(env);

    List<StatisticEntry<?>> stats =
        computer.computeTableStats(NameIdentifier.parse("catalog.schema.table"));
    Assertions.assertEquals(1, stats.size());
    Assertions.assertEquals("s1", stats.get(0).name());
    Assertions.assertEquals(2L, stats.get(0).value().value());
  }

  @Test
  void testComputeAllTableStats() throws IOException {
    Path statsFile = tempDir.resolve("stats.json");
    Files.write(
        statsFile,
        List.of(
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"table\",\"s1\":1,\"s2\":5}",
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"table\",\"s1\":3}",
            "{\"identifier\":\"catalog.schema.other\",\"stats-type\":\"table\",\"s1\":10}"));

    LocalStatsComputer computer = new LocalStatsComputer();
    OptimizerEnv env = OptimizerEnv.getInstance();
    env.initialize(createConfig());
    env.setContent(new StatsComputerContent(statsFile.toString(), null));
    computer.initialize(env);

    Map<NameIdentifier, List<StatisticEntry<?>>> allStats = computer.computeAllTableStats();
    Assertions.assertEquals(2, allStats.size());

    Map<String, StatisticEntry<?>> tableStats =
        toNameMap(allStats.get(NameIdentifier.parse("catalog.schema.table")));
    Assertions.assertEquals(2, tableStats.size());
    Assertions.assertEquals(3L, tableStats.get("s1").value().value());
    Assertions.assertEquals(5L, tableStats.get("s2").value().value());

    Map<String, StatisticEntry<?>> otherStats =
        toNameMap(allStats.get(NameIdentifier.parse("catalog.schema.other")));
    Assertions.assertEquals(1, otherStats.size());
    Assertions.assertEquals(10L, otherStats.get("s1").value().value());
  }

  @Test
  void testComputeTableStatsFromPayload() {
    String payload =
        String.join(
            "\n",
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"table\",\"s1\":1,\"s2\":5}",
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"table\",\"s1\":3}");

    LocalStatsComputer computer = new LocalStatsComputer();
    OptimizerEnv env = OptimizerEnv.getInstance();
    env.initialize(createConfig());
    env.setContent(new StatsComputerContent(null, payload));
    computer.initialize(env);

    List<StatisticEntry<?>> stats =
        computer.computeTableStats(NameIdentifier.parse("catalog.schema.table"));
    Map<String, StatisticEntry<?>> map = toNameMap(stats);
    Assertions.assertEquals(2, map.size());
    Assertions.assertEquals(3L, map.get("s1").value().value());
    Assertions.assertEquals(5L, map.get("s2").value().value());
  }

  private OptimizerConfig createConfig() {
    Map<String, String> configs = new HashMap<>();
    configs.put(OptimizerConfig.GRAVITINO_DEFAULT_CATALOG_CONFIG.getKey(), "catalog");
    return new OptimizerConfig(configs);
  }

  private Map<String, StatisticEntry<?>> toNameMap(List<StatisticEntry<?>> stats) {
    Map<String, StatisticEntry<?>> map = new HashMap<>();
    for (StatisticEntry<?> stat : stats) {
      map.put(stat.name(), stat);
    }
    return map;
  }
}
