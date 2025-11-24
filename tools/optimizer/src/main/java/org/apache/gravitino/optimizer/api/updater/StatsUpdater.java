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

package org.apache.gravitino.optimizer.api.updater;

import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.optimizer.api.common.Provider;
import org.apache.gravitino.optimizer.api.common.SingleStatistic;

/** Represents an updater that can update statistics for a table. */
@DeveloperApi
public interface StatsUpdater extends Provider {
  /**
   * Persist table (and optionally partition) statistics into the catalog.
   *
   * @param tableIdentifier catalog/schema/table identifier
   * @param statistics list of table and/or partition statistics
   */
  void updateTableStatistics(NameIdentifier tableIdentifier, List<SingleStatistic<?>> statistics);
}
