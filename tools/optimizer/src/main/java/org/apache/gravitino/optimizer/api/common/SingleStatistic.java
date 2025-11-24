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

package org.apache.gravitino.optimizer.api.common;

import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.stats.StatisticValue;

/** Represents a single statistic with name and value. */
@DeveloperApi
public interface SingleStatistic<T> {
  enum Name {
    TABLE_STORAGE_COST,
    DATAFILE_AVG_SIZE,
    DATAFILE_NUMBER,
    DATAFILE_SIZE_MSE,
    POSITION_DELETE_FILE_NUMBER,
    EQUAL_DELETE_FILE_NUMBER,
    JOB_COST,
    JOB_DURATION,
  }

  /** Stable metric key used for lookup and reporting. */
  String name();

  /** Typed value holder for the statistic. */
  StatisticValue<T> value();
}
