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

package org.apache.gravitino.maintenance.optimizer.updater;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.updater.StatisticsUpdater;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;

public class TestStatisticsUpdater implements StatisticsUpdater {

  public static final String NAME = "test-statistics-updater";

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {}

  @Override
  public void updateTableStatistics(
      NameIdentifier tableIdentifier, List<StatisticEntry<?>> tableStatistics) {}

  @Override
  public void updatePartitionStatistics(
      NameIdentifier tableIdentifier,
      Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics) {}

  @Override
  public void close() {}
}
