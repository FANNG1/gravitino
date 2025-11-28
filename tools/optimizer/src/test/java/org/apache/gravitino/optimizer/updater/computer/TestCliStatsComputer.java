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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.gravitino.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.optimizer.common.OptimizerEnv;
import org.apache.gravitino.optimizer.common.StatsComputerContent;
import org.apache.gravitino.optimizer.common.conf.OptimizerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestCliStatsComputer {

  @Test
  void testInitializeWithTableStats() {
    CliStatsComputer computer = new CliStatsComputer();
    OptimizerEnv env =
        createOptimizerEnv(new StatsComputerContent(null, "table:col1=100,col2=200.5"));

    computer.initialize(env);

    List<StatisticEntry<?>> stats = computer.computeTableStats(null);

    Assertions.assertEquals(2, stats.size());
    Assertions.assertEquals(100L, stats.get(0).value().value());
    Assertions.assertEquals(200.5, stats.get(1).value().value());
  }

  @Test
  void testInitializeWithJobStats() {
    CliStatsComputer computer = new CliStatsComputer();
    OptimizerEnv env =
        createOptimizerEnv(new StatsComputerContent(null, "job:task1=50,task2=75.3"));

    computer.initialize(env);

    List<StatisticEntry<?>> stats = computer.computeJobStats(null);
    Assertions.assertEquals(2, stats.size());
    Assertions.assertEquals(50L, stats.get(0).value().value());
    Assertions.assertEquals(75.3, stats.get(1).value().value());
  }

  @Test
  void testInitializeWithInvalidFormat() {
    CliStatsComputer computer = new CliStatsComputer();
    OptimizerEnv env =
        createOptimizerEnv(new StatsComputerContent(null, "hello:col1=100,col2=200.5"));

    Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> computer.initialize(env));
  }

  @Test
  void testInitializeWithFilePayload(@TempDir Path tempDir) throws IOException {
    CliStatsComputer computer = new CliStatsComputer();
    Path statsFile = tempDir.resolve("stats.txt");
    Files.writeString(statsFile, "table:col1=42", StandardCharsets.UTF_8);
    OptimizerEnv env = createOptimizerEnv(new StatsComputerContent(null, "table:col1=42"));

    computer.initialize(env);

    List<StatisticEntry<?>> stats = computer.computeTableStats(null);
    Assertions.assertEquals(1, stats.size());
    Assertions.assertEquals(42L, stats.get(0).value().value());
  }

  private OptimizerEnv createOptimizerEnv(StatsComputerContent content) {
    OptimizerEnv optimizerEnv = OptimizerEnv.getInstance();
    OptimizerConfig optimizerConfig = new OptimizerConfig();
    optimizerEnv.initialize(optimizerConfig, content);
    return optimizerEnv;
  }
}
