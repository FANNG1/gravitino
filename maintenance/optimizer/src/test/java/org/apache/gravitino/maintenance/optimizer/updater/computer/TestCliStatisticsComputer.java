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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.TableStatisticsBundle;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.StatisticsComputerContent;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestCliStatisticsComputer {

  @Test
  void testInitializeWithTableStats() {
    CliStatisticsComputer computer = new CliStatisticsComputer();
    OptimizerEnv env =
        createOptimizerEnv(new StatisticsComputerContent(null, "table:col1=100,col2=200.5"));

    computer.initialize(env);

    TableStatisticsBundle bundle = computer.calculateTableStatistics(null);
    List<StatisticEntry<?>> stats = bundle.tableStatistics();

    Assertions.assertEquals(2, stats.size());
    Assertions.assertEquals(100L, stats.get(0).value().value());
    Assertions.assertEquals(200.5, stats.get(1).value().value());
  }

  @Test
  void testInitializeWithJobStats() {
    CliStatisticsComputer computer = new CliStatisticsComputer();
    OptimizerEnv env =
        createOptimizerEnv(new StatisticsComputerContent(null, "job:task1=50,task2=75.3"));

    computer.initialize(env);

    List<StatisticEntry<?>> stats = computer.calculateJobStatistics(null);
    Assertions.assertEquals(2, stats.size());
    Assertions.assertEquals(50L, stats.get(0).value().value());
    Assertions.assertEquals(75.3, stats.get(1).value().value());
  }

  @Test
  void testInitializeWithInvalidFormat() {
    CliStatisticsComputer computer = new CliStatisticsComputer();
    OptimizerEnv env =
        createOptimizerEnv(new StatisticsComputerContent(null, "hello:col1=100,col2=200.5"));

    Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> computer.initialize(env));
  }

  @Test
  void testInitializeWithFilePayload(@TempDir Path tempDir) throws IOException {
    CliStatisticsComputer computer = new CliStatisticsComputer();
    Path statsFile = tempDir.resolve("stats.txt");
    Files.writeString(statsFile, "table:col1=42", StandardCharsets.UTF_8);
    OptimizerEnv env = createOptimizerEnv(new StatisticsComputerContent(null, "table:col1=42"));

    computer.initialize(env);

    List<StatisticEntry<?>> stats = computer.calculateTableStatistics(null).tableStatistics();
    Assertions.assertEquals(1, stats.size());
    Assertions.assertEquals(42L, stats.get(0).value().value());
  }

  private OptimizerEnv createOptimizerEnv(StatisticsComputerContent content) {
    OptimizerEnv optimizerEnv = OptimizerEnv.getInstance();
    OptimizerConfig optimizerConfig = new OptimizerConfig();
    optimizerEnv.initialize(optimizerConfig, content);
    return optimizerEnv;
  }
}
