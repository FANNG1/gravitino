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

package org.apache.gravitino.optimizer.recommender;

import com.google.common.collect.ImmutableMap;
import java.util.LinkedList;
import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.optimizer.common.conf.OptimizerConfig;

public class RecommenderCli {
  Recommender recommender = new Recommender(new OptimizerConfig(ImmutableMap.of()));
  /**
   * Runs the recommender with the given arguments.
   *
   * @param args The command-line arguments. Expected format: [0] - policy type (e.g., "compact")
   *     [1] - table identifiers (e.g., "db.table,db.table2")
   *     <p>For example ./recommender-cli —tables tableA, tableB, tableC –policyType compaction
   *     <p>Output:
   *     <p>PolicyA: Selected tables: tableA,tableB tableA: Score: 90 jobConfig: Partitions:
   *     [day=2024, day=2025] Target-size: xxx tableB:xx PolicyB: Selected tables: tableB,tableC
   *     tableB:xx tableC:xx
   */
  public void run(String[] args) {
    List<NameIdentifier> tables = new LinkedList<>();
    recommender.recommendForPolicyType(tables, "compact");
  }

  public static void main(String[] args) {
    new RecommenderCli().run(args);
  }
}
