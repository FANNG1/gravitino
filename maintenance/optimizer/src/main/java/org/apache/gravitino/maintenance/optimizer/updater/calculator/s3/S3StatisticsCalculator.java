/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the
 * Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License.  You may obtain a copy
 * of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gravitino.maintenance.optimizer.updater.calculator.s3;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.TableAndPartitionStatistics;
import org.apache.gravitino.maintenance.optimizer.api.updater.SupportsCalculateBulkTableStatistics;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerContent;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.StatisticsInputContent;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;

public class S3StatisticsCalculator implements SupportsCalculateBulkTableStatistics {

  public static final String NAME = "s3-stats-calculator";

  private String defaultCatalog;
  private String s3Path;
  private String s3Region;

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    this.defaultCatalog =
        optimizerEnv.config().get(OptimizerConfig.GRAVITINO_DEFAULT_CATALOG_CONFIG);
    OptimizerContent optimizerContent =
        optimizerEnv
            .content()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "S3StatisticsCalculator requires runtime statistics input content."));
    Preconditions.checkArgument(
        optimizerContent instanceof StatisticsInputContent,
        "S3StatisticsCalculator expects StatisticsInputContent, but got: %s",
        optimizerContent.getClass().getSimpleName());
    StatisticsInputContent statisticsInputContent = (StatisticsInputContent) optimizerContent;

    this.s3Path = statisticsInputContent.filePath();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(s3Path), "S3 path must be provided for S3StatisticsCalculator");
    this.s3Region = optimizerEnv.config().get(OptimizerConfig.S3_REGION_CONFIG);
  }

  @Override
  public TableAndPartitionStatistics calculateTableStatistics(NameIdentifier tableIdentifier) {
    Preconditions.checkArgument(tableIdentifier != null, "tableIdentifier must not be null");
    try (S3StatisticsImporter statsImporter =
        new S3StatisticsImporter(s3Path, s3Region, defaultCatalog)) {
      return statsImporter.readTableStatistics(tableIdentifier);
    } catch (IOException e) {
      throw new RuntimeException("Failed to compute table statistics", e);
    }
  }

  @Override
  public Map<NameIdentifier, TableAndPartitionStatistics> calculateBulkTableStatistics() {
    try (S3StatisticsImporter statsImporter =
        new S3StatisticsImporter(s3Path, s3Region, defaultCatalog)) {
      return statsImporter.bulkReadAllTableStatistics();
    } catch (IOException e) {
      throw new RuntimeException("Failed to compute table statistics", e);
    }
  }
}
