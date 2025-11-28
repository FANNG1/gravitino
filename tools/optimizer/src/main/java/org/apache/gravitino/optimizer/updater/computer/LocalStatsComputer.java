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

package org.apache.gravitino.optimizer.updater.computer;

import com.google.common.base.Preconditions;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.optimizer.api.updater.SupportComputeTableStats;
import org.apache.gravitino.optimizer.common.OptimizerContent;
import org.apache.gravitino.optimizer.common.OptimizerEnv;
import org.apache.gravitino.optimizer.common.StatsComputerContent;
import org.apache.gravitino.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.optimizer.recommender.stats.FileStatsReader;
import org.apache.gravitino.optimizer.recommender.stats.PayloadStatsReader;
import org.apache.gravitino.optimizer.recommender.stats.StatsReader;

public class LocalStatsComputer implements SupportComputeTableStats {

  public static final String LOCAL_STATS_COMPUTER_NAME = "local-stats-computer";

  private StatsReader statsReader;

  @Override
  public String name() {
    return LOCAL_STATS_COMPUTER_NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    OptimizerContent content = optimizerEnv.content();
    Preconditions.checkArgument(
        content instanceof StatsComputerContent,
        "StatsComputerContent is required for LocalStatsComputer");

    StatsComputerContent statsContent = (StatsComputerContent) content;
    String defaultCatalog =
        optimizerEnv.config().get(OptimizerConfig.GRAVITINO_DEFAULT_CATALOG_CONFIG);

    if (StringUtils.isNotBlank(statsContent.statsFilePath())) {
      this.statsReader = new FileStatsReader(Path.of(statsContent.statsFilePath()), defaultCatalog);
      return;
    }

    String payload = statsContent.statsPayload();
    Preconditions.checkArgument(StringUtils.isNotBlank(payload), "Stats payload must be provided");
    this.statsReader = new PayloadStatsReader(payload, defaultCatalog);
  }

  @Override
  public List<StatisticEntry<?>> computeTableStats(NameIdentifier tableIdentifier) {
    return statsReader.readTableStats(tableIdentifier);
  }

  @Override
  public java.util.Map<NameIdentifier, List<StatisticEntry<?>>> computeAllTableStats() {
    return statsReader.readAllTableStats();
  }
}
