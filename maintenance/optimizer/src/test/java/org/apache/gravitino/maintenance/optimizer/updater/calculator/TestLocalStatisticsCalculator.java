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

package org.apache.gravitino.maintenance.optimizer.updater.calculator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.TableStatisticsBundle;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestLocalStatisticsCalculator {

  @TempDir Path tempDir;

  @Test
  void testComputeTableStatisticsUsesReader() throws IOException {
    Path statsFile = tempDir.resolve("stats.json");
    Files.write(
        statsFile,
        List.of(
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"table\",\"s1\":1}",
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"table\",\"s1\":2}",
            "{\"identifier\":\"other\",\"stats-type\":\"table\",\"s1\":100}"));

    LocalStatisticsCalculator calculator = new LocalStatisticsCalculator();
    OptimizerEnv env = OptimizerEnv.getInstance();
    env.initialize(createConfig(statsFile.toString(), null));
    calculator.initialize(env);

    List<StatisticEntry<?>> stats =
        calculator
            .calculateTableStatistics(NameIdentifier.parse("catalog.schema.table"))
            .tableStatistics();
    Assertions.assertEquals(1, stats.size());
    Assertions.assertEquals("s1", stats.get(0).name());
    Assertions.assertEquals(2L, stats.get(0).value().value());
  }

  @Test
  void testComputeAllTableStatistics() throws IOException {
    Path statsFile = tempDir.resolve("stats.json");
    Files.write(
        statsFile,
        List.of(
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"table\",\"s1\":1,\"s2\":5}",
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"table\",\"s1\":3}",
            "{\"identifier\":\"catalog.schema.other\",\"stats-type\":\"table\",\"s1\":10}"));

    LocalStatisticsCalculator calculator = new LocalStatisticsCalculator();
    OptimizerEnv env = OptimizerEnv.getInstance();
    env.initialize(createConfig(statsFile.toString(), null));
    calculator.initialize(env);

    Map<NameIdentifier, TableStatisticsBundle> allStatistics =
        calculator.calculateBulkTableStatistics();
    Assertions.assertEquals(2, allStatistics.size());

    Map<String, StatisticEntry<?>> tableStats =
        toNameMap(
            allStatistics.get(NameIdentifier.parse("catalog.schema.table")).tableStatistics());
    Assertions.assertEquals(2, tableStats.size());
    Assertions.assertEquals(3L, tableStats.get("s1").value().value());
    Assertions.assertEquals(5L, tableStats.get("s2").value().value());

    Map<String, StatisticEntry<?>> otherStats =
        toNameMap(
            allStatistics.get(NameIdentifier.parse("catalog.schema.other")).tableStatistics());
    Assertions.assertEquals(1, otherStats.size());
    Assertions.assertEquals(10L, otherStats.get("s1").value().value());
  }

  @Test
  void testComputeTableStatisticsFromPayload() {
    String payload =
        String.join(
            "\n",
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"table\",\"s1\":1,\"s2\":5}",
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"table\",\"s1\":3}");

    LocalStatisticsCalculator calculator = new LocalStatisticsCalculator();
    OptimizerEnv env = OptimizerEnv.getInstance();
    env.initialize(createConfig(null, payload));
    calculator.initialize(env);

    List<StatisticEntry<?>> stats =
        calculator
            .calculateTableStatistics(NameIdentifier.parse("catalog.schema.table"))
            .tableStatistics();
    Map<String, StatisticEntry<?>> map = toNameMap(stats);
    Assertions.assertEquals(2, map.size());
    Assertions.assertEquals(3L, map.get("s1").value().value());
    Assertions.assertEquals(5L, map.get("s2").value().value());
  }

  @Test
  void testComputeJobStatisticsFromPayload() {
    String payload =
        String.join(
            "\n",
            "{\"identifier\":\"catalog.schema.job1\",\"stats-type\":\"job\",\"duration\":10}",
            "{\"identifier\":\"catalog.schema.job1\",\"stats-type\":\"job\",\"duration\":20,"
                + "\"planning\":5}",
            "{\"identifier\":\"catalog.schema.job2\",\"stats-type\":\"job\",\"duration\":1}");

    LocalStatisticsCalculator calculator = new LocalStatisticsCalculator();
    OptimizerEnv env = OptimizerEnv.getInstance();
    env.initialize(createConfig(null, payload));
    calculator.initialize(env);

    List<StatisticEntry<?>> stats =
        calculator.calculateJobStatistics(NameIdentifier.parse("catalog.schema.job1"));
    Map<String, StatisticEntry<?>> map = toNameMap(stats);
    Assertions.assertEquals(2, map.size());
    Assertions.assertEquals(20L, map.get("duration").value().value());
    Assertions.assertEquals(5L, map.get("planning").value().value());
  }

  @Test
  void testComputeAllJobStatistics() {
    String payload =
        String.join(
            "\n",
            "{\"identifier\":\"catalog.schema.job1\",\"stats-type\":\"job\",\"duration\":10}",
            "{\"identifier\":\"catalog.schema.job2\",\"stats-type\":\"job\",\"duration\":7}");

    LocalStatisticsCalculator calculator = new LocalStatisticsCalculator();
    OptimizerEnv env = OptimizerEnv.getInstance();
    env.initialize(createConfig(null, payload));
    calculator.initialize(env);

    Map<NameIdentifier, List<StatisticEntry<?>>> allStatistics =
        calculator.calculateAllJobStatistics();
    Assertions.assertEquals(2, allStatistics.size());
    Assertions.assertEquals(
        10L,
        toNameMap(allStatistics.get(NameIdentifier.parse("catalog.schema.job1")))
            .get("duration")
            .value()
            .value());
    Assertions.assertEquals(
        7L,
        toNameMap(allStatistics.get(NameIdentifier.parse("catalog.schema.job2")))
            .get("duration")
            .value()
            .value());
  }

  @Test
  void testJobIdentifiersDoNotApplyDefaultCatalog() {
    String payload = "{\"identifier\":\"job-1\",\"stats-type\":\"job\",\"duration\":10}";

    LocalStatisticsCalculator calculator = new LocalStatisticsCalculator();
    OptimizerEnv env = OptimizerEnv.getInstance();
    env.initialize(createConfig(null, payload));
    calculator.initialize(env);

    List<StatisticEntry<?>> stats =
        calculator.calculateJobStatistics(NameIdentifier.parse("job-1"));
    Assertions.assertEquals(1, stats.size());
    Assertions.assertEquals(10L, stats.get(0).value().value());
    Assertions.assertTrue(
        calculator.calculateJobStatistics(NameIdentifier.parse("catalog.job-1")).isEmpty());
  }

  private OptimizerConfig createConfig(String statisticsFilePath, String statisticsPayload) {
    Map<String, String> configs = new HashMap<>();
    configs.put(OptimizerConfig.GRAVITINO_DEFAULT_CATALOG_CONFIG.getKey(), "catalog");
    if (statisticsFilePath != null) {
      configs.put(LocalStatisticsCalculator.STATISTICS_FILE_PATH_CONFIG, statisticsFilePath);
    }
    if (statisticsPayload != null) {
      configs.put(LocalStatisticsCalculator.STATISTICS_PAYLOAD_CONFIG, statisticsPayload);
    }
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
