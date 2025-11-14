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

package org.apache.gravitino.optimizer.updater;

import java.util.LinkedList;
import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.optimizer.common.OptimizerEnv;
import org.apache.gravitino.optimizer.common.conf.OptimizerConfig;

public class UpdaterCli {
  /**
   * Computes and updates the stats for the given tables
   *
   * @param args The command-line arguments. Expected format: [0] - stats computer name (e.g.,
   *     "gravitino-table-datasize") [1] - table identifiers (e.g., "db.table,db.table2") [2] -
   *     update type (e.g., metrics or stats)
   *     <p>output: the updated stats for the tables.
   */
  public void run(String[] args) {
    List<NameIdentifier> tableIdentifiers = new LinkedList<>();
    String statsComputerName = args[0];
    OptimizerEnv optimizerEnv = OptimizerEnv.getInstance();
    optimizerEnv.initialize(new OptimizerConfig());
    new Updater(optimizerEnv).update(statsComputerName, tableIdentifiers, UpdateType.STATS);
  }

  public static void main(String[] args) {
    new UpdaterCli().run(args);
  }
}
