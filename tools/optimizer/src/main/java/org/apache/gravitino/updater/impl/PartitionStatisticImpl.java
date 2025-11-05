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

package org.apache.gravitino.updater.impl;

import java.util.List;
import org.apache.gravitino.common.SinglePartition;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.updater.api.PartitionStatistic;

public class PartitionStatisticImpl extends SingleStatisticImpl implements PartitionStatistic {
  private List<SinglePartition> partitions;

  public PartitionStatisticImpl(
      String name, StatisticValue value, List<SinglePartition> partitions) {
    super(name, value);
    this.partitions = partitions;
  }

  @Override
  public List<SinglePartition> partitions() {
    return partitions;
  }
}
