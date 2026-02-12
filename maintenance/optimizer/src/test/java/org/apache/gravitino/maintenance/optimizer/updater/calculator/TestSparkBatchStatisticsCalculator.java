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

class TestSparkBatchStatisticsCalculator {

  @TempDir Path tempDir;

  @Test
  void testComputeTableStatisticsRunsScriptAndReadsOutput() throws IOException {
    Path script = tempDir.resolve("script.sh");
    Path output = tempDir.resolve("stats.json");
    Files.write(
        script,
        List.of(
            "#!/usr/bin/env bash",
            "while [[ \"$#\" -gt 0 ]]; do",
            "  case \"$1\" in",
            "    --table)",
            "      TABLE=$2",
            "      shift 2",
            "      ;;",
            "    --output)",
            "      OUTPUT=$2",
            "      shift 2",
            "      ;;",
            "    *)",
            "      shift",
            "      ;;",
            "  esac",
            "done",
            "echo '{\"identifier\":\"'\"${TABLE}\"'\",\"stats-type\":\"table\",\"file_count\":5,"
                + "\"data_files\":3,\"position_delete_files\":1,\"equality_delete_files\":1,"
                + "\"small_files\":2,\"data_size_mse\":10.5,\"avg_size\":100.0,\"total_size\":500}' > \"${OUTPUT}\""));

    SparkBatchStatisticsCalculator calculator = new SparkBatchStatisticsCalculator();
    OptimizerEnv env = OptimizerEnv.getInstance();
    env.initialize(createConfig(script, output));
    calculator.initialize(env);

    TableStatisticsBundle bundle =
        calculator.calculateTableStatistics(NameIdentifier.parse("catalog.schema.table"));
    List<StatisticEntry<?>> stats = bundle.tableStatistics();
    Map<String, StatisticEntry<?>> statsByName = toMap(stats);
    Assertions.assertEquals(8, statsByName.size());
    Assertions.assertEquals(5L, statsByName.get("file_count").value().value());
    Assertions.assertEquals(3L, statsByName.get("data_files").value().value());
    Assertions.assertEquals(1L, statsByName.get("position_delete_files").value().value());
    Assertions.assertEquals(1L, statsByName.get("equality_delete_files").value().value());
    Assertions.assertEquals(2L, statsByName.get("small_files").value().value());
    Assertions.assertEquals(10.5, statsByName.get("data_size_mse").value().value());
    Assertions.assertEquals(100.0, statsByName.get("avg_size").value().value());
    Assertions.assertEquals(500L, statsByName.get("total_size").value().value());
  }

  private OptimizerConfig createConfig(Path script, Path output) {
    Map<String, String> configs = new HashMap<>();
    configs.put(SparkBatchStatisticsCalculator.SCRIPT_PATH_CONFIG, script.toString());
    configs.put(SparkBatchStatisticsCalculator.OUTPUT_PATH_CONFIG, output.toString());
    configs.put(SparkBatchStatisticsCalculator.SPARK_SUBMIT_CONFIG, "/bin/bash");
    configs.put(OptimizerConfig.GRAVITINO_DEFAULT_CATALOG_CONFIG.getKey(), "catalog");
    return new OptimizerConfig(configs);
  }

  private Map<String, StatisticEntry<?>> toMap(List<StatisticEntry<?>> stats) {
    Map<String, StatisticEntry<?>> map = new java.util.HashMap<>();
    for (StatisticEntry<?> stat : stats) {
      map.put(stat.name(), stat);
    }
    return map;
  }
}
