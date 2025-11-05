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

package org.apache.gravitino.optimizer.recommender.impl;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.optimizer.api.common.SingleStatistic;
import org.apache.gravitino.optimizer.api.recommender.TableStatsProvider;
import org.apache.gravitino.optimizer.updater.impl.SingleStatisticImpl;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.stats.PartitionStatistics;
import org.apache.gravitino.stats.Statistic;

public class GravitinoTableStatsProvider implements TableStatsProvider {

  private GravitinoClient gravitinoClient;
  private String defaultCatalogName;

  public void initialize(String uri, String metalakeName, String defaultCatalogName) {
    this.gravitinoClient = GravitinoClient.builder(uri).withMetalake(metalakeName).build();
    this.defaultCatalogName = defaultCatalogName;
  }

  @Override
  public List<SingleStatistic> getTableStats(NameIdentifier tableIdentifier) {
    Table t =
        gravitinoClient
            .loadCatalog(getCatalogName(tableIdentifier))
            .asTableCatalog()
            .loadTable(tableIdentifier);
    List<Statistic> statistics = t.supportsStatistics().listStatistics();
    return statistics.stream()
        .filter(statistic -> statistic.value().isPresent())
        .map(statistic -> new SingleStatisticImpl(statistic.name(), statistic.value().get()))
        .collect(Collectors.toList());
  }

  @Override
  public List<PartitionStatistics> getPartitionStats(NameIdentifier tableIdentifier) {
    Table t =
        gravitinoClient
            .loadCatalog(getCatalogName(tableIdentifier))
            .asTableCatalog()
            .loadTable(tableIdentifier);
    List<PartitionStatistics> partitionStatistics =
        t.supportsPartitionStatistics().listPartitionStatistics(null);
    return partitionStatistics;
  }

  private String getCatalogName(NameIdentifier tableIdentifier) {
    Namespace namespace = tableIdentifier.namespace();
    Preconditions.checkArgument(namespace != null && namespace.levels().length >= 1);
    if (namespace.levels().length == 1) {
      return defaultCatalogName;
    }

    return namespace.levels()[0];
  }
}
