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

package org.apache.gravitino.maintenance.optimizer.updater.computer;

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
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

@EnabledIfEnvironmentVariable(named = "IT_SPARK_HOME", matches = "true")
public class TestSparkBatchStatisticsComputerWithIRC {

  @TempDir Path tempDir;

  @Test
  void testComputeTableStatisticsWithRealSpark() {
    Path sparkSubmit =
        Path.of(System.getenv().getOrDefault("IT_SPARK_HOME", ""))
            .resolve("bin")
            .resolve("spark-submit");
    Assertions.assertTrue(Files.isExecutable(sparkSubmit), "spark-submit not found");

    String sparkArgs = System.getenv().getOrDefault("IT_SPARK_ARGS", "");
    String table = System.getenv().getOrDefault("IT_TABLE_IDENTIFIER", "rest.ab.a1");
    Path script =
        resolveScriptPath(
            System.getenv()
                .getOrDefault("IT_STATS_SCRIPT", "maintenance/optimizer/bin/spark-table-stats.py"));
    Assertions.assertTrue(Files.exists(script), "stats script not found: " + script);

    Path output = tempDir.resolve("stats.json");

    SparkBatchStatisticsComputer computer = new SparkBatchStatisticsComputer();
    OptimizerEnv env = OptimizerEnv.getInstance();
    env.initialize(createConfig(script, output, sparkSubmit, sparkArgs));
    computer.initialize(env);

    TableStatisticsBundle bundle = computer.calculateTableStatistics(NameIdentifier.parse(table));
    List<StatisticEntry<?>> stats = bundle.tableStatistics();
    // Ensure we got at least the core metrics
    Map<String, StatisticEntry<?>> statsByName = toMap(stats);
    org.junit.jupiter.api.Assertions.assertTrue(statsByName.containsKey("file_count"));
    org.junit.jupiter.api.Assertions.assertTrue(statsByName.containsKey("total_size"));
  }

  private OptimizerConfig createConfig(
      Path script, Path output, Path sparkSubmit, String sparkArgs) {
    Map<String, String> configs = new HashMap<>();
    configs.put(SparkBatchStatisticsComputer.SCRIPT_PATH_CONFIG, script.toString());
    configs.put(SparkBatchStatisticsComputer.OUTPUT_PATH_CONFIG, output.toString());
    configs.put(SparkBatchStatisticsComputer.SPARK_SUBMIT_CONFIG, sparkSubmit.toString());
    configs.put(SparkBatchStatisticsComputer.SPARK_ARGS_CONFIG, sparkArgs);
    return new OptimizerConfig(configs);
  }

  private Map<String, StatisticEntry<?>> toMap(List<StatisticEntry<?>> stats) {
    Map<String, StatisticEntry<?>> map = new HashMap<>();
    for (StatisticEntry<?> stat : stats) {
      map.put(stat.name(), stat);
    }
    return map;
  }

  private Path resolveScriptPath(String scriptPathString) {
    Path scriptPath = Path.of(scriptPathString);
    if (scriptPath.isAbsolute()) {
      return scriptPath;
    }

    Path projectRoot = resolveProjectRoot();
    if (projectRoot != null) {
      return projectRoot.resolve(scriptPath).normalize();
    }

    return Path.of("").toAbsolutePath().resolve(scriptPath).normalize();
  }

  private Path resolveProjectRoot() {
    String home = System.getenv("GRAVITINO_HOME");
    if (home == null || home.isBlank()) {
      return null;
    }

    Path root = Path.of(home);
    if (!root.isAbsolute()) {
      root = Path.of("").toAbsolutePath().resolve(root);
    }

    return root;
  }
}
