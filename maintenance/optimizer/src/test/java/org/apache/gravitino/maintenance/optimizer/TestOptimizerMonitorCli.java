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

package org.apache.gravitino.maintenance.optimizer;

import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.monitor.service.MonitorServiceServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestOptimizerMonitorCli {

  @Test
  void testMonitorCommands() throws Exception {
    OptimizerConfig config =
        new OptimizerConfig(ImmutableMap.of(OptimizerConfig.MONITOR_SERVICE_PORT, "0"));
    OptimizerEnv env = OptimizerEnv.getInstance();
    env.initialize(config);

    MonitorServiceServer server = new MonitorServiceServer(env);
    server.start();
    try {
      int port = server.localPort();
      String monitorServiceUrl = "http://localhost:" + port;
      Path confPath = Files.createTempFile("gravitino-optimizer", ".conf");

      String monitorId =
          runCommand(
                  "--type",
                  "submit-monitor",
                  "--identifier",
                  "db.table.cli",
                  "--action-time-seconds",
                  "1",
                  "--range-seconds",
                  "0",
                  "--conf-path",
                  confPath.toString(),
                  "--monitor-service-url",
                  monitorServiceUrl)
              .trim();
      Assertions.assertFalse(monitorId.isEmpty());

      String listOutput =
          runCommand(
              "--type",
              "list-monitors",
              "--conf-path",
              confPath.toString(),
              "--monitor-service-url",
              monitorServiceUrl);
      Assertions.assertTrue(listOutput.contains(monitorId), () -> "list output: " + listOutput);
      Assertions.assertTrue(
          listOutput.contains("db.table.cli"), () -> "list output: " + listOutput);

      String statusOutput =
          runCommand(
              "--type",
              "get-monitor",
              "--monitor-id",
              monitorId,
              "--conf-path",
              confPath.toString(),
              "--monitor-service-url",
              monitorServiceUrl);
      Assertions.assertTrue(statusOutput.contains("monitorId=" + monitorId));
      Assertions.assertTrue(statusOutput.contains("identifier=db.table.cli"));

      String cancelOutput =
          runCommand(
              "--type",
              "cancel-monitor",
              "--monitor-id",
              monitorId,
              "--conf-path",
              confPath.toString(),
              "--monitor-service-url",
              monitorServiceUrl);
      Assertions.assertTrue(cancelOutput.contains("monitorId=" + monitorId));
      Assertions.assertTrue(cancelOutput.contains("state=CANCELED"));
    } finally {
      server.stop();
    }
  }

  private String runCommand(String... args) {
    PrintStream originalOut = System.out;
    PrintStream originalErr = System.err;
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    PrintStream capture = new PrintStream(output, true, StandardCharsets.UTF_8);
    System.setOut(capture);
    System.setErr(capture);
    try {
      OptimizerCmd.main(args);
    } finally {
      System.setOut(originalOut);
      System.setErr(originalErr);
    }
    return output.toString(StandardCharsets.UTF_8);
  }
}
