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

package org.apache.gravitino.optimizer.integration.test;

import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.optimizer.api.common.SingleStatistic;
import org.apache.gravitino.optimizer.api.updater.SupportTableStats;
import org.apache.gravitino.optimizer.common.OptimizerEnv;
import org.apache.gravitino.optimizer.common.PartitionImpl;
import org.apache.gravitino.optimizer.common.SinglePartition;
import org.apache.gravitino.optimizer.updater.PartitionStatisticImpl;
import org.apache.gravitino.optimizer.updater.SingleStatisticImpl;
import org.apache.gravitino.stats.StatisticValues;

public class DummyTableStatsComputer implements SupportTableStats {

  public static final String DUMMY_TABLE_STAT = "dummy-table-stat";
  public static final String TABLE_STAT_NAME = "custom-dummy-table-stat-name";

  @Override
  public String name() {
    return DUMMY_TABLE_STAT;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {}

  @Override
  public List<SingleStatistic<?>> computeTableStats(NameIdentifier tableIdentifier) {
    return Arrays.asList(
        new SingleStatisticImpl(TABLE_STAT_NAME, StatisticValues.longValue(1L)),
        new PartitionStatisticImpl(
            TABLE_STAT_NAME, StatisticValues.longValue(2L), getPartitionName()));
  }

  @VisibleForTesting
  public static List<SinglePartition> getPartitionName() {
    return Arrays.asList(new PartitionImpl("p1", "v1"));
  }
}
