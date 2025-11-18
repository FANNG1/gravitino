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

package org.apache.gravitino.optimizer.monitor;

import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.optimizer.common.OptimizerEnv;
import org.apache.gravitino.optimizer.common.conf.OptimizerConfig;

public class MonitorCli {

  /**
   * Runs the monitor with the given arguments.
   *
   * @param args The command-line arguments. Expected format: [0] - table identifier (e.g.,
   *     "db.table") [1] - Optimize Action time (in epoch seconds) [2] - Range hours (in hours) [3]
   *     - Optional policy type (e.g., "compaction")
   *     <p>For example: ./monitorCli db.table 1760693151 24 compaction will get the table metrics
   *     and job metrics in the range of [action time - range hours*3600, action time + range
   *     hours*3600], and then compare the metrics before and after the action time.
   */
  public void run(String[] args) {
    NameIdentifier tableIdentifier = NameIdentifier.of("db", "table");
    long time = Long.parseLong(args[1]);
    long rangeSeconds = Long.parseLong(args[2]);
    Optional<String> policyType = Optional.ofNullable(args[3]);
    OptimizerEnv optimizerEnv = OptimizerEnv.getInstance();
    optimizerEnv.initialize(new OptimizerConfig());
    new Monitor(optimizerEnv).run(tableIdentifier, time, rangeSeconds, policyType);
  }

  public static void main(String[] args) {
    new MonitorCli().run(args);
  }
}
