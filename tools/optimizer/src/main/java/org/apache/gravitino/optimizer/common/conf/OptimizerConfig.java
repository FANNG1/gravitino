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

package org.apache.gravitino.optimizer.common.conf;

import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;
import org.apache.gravitino.optimizer.recommender.job.GravitinoJobSubmitter;
import org.apache.gravitino.optimizer.recommender.policy.GravitinoPolicyProvider;
import org.apache.gravitino.optimizer.recommender.stats.GravitinoStatsProvider;
import org.apache.gravitino.optimizer.recommender.table.GravitinoTableMetadataProvider;

public class OptimizerConfig extends Config {

  public static final String GRAVITINO_URI = "gravitino-uri";
  public static final String GRAVITINO_METALAKE = "gravitino-metalake";
  public static final String GRAVITINO_DEFAULT_CATALOG = "gravitino-default-catalog";

  private static final String RECOMMENDER_PREFIX = "recommender.";

  private static final String STATS_PROVIDER = RECOMMENDER_PREFIX + "stats-provider";
  private static final String POLICY_PROVIDER = RECOMMENDER_PREFIX + "policy-provider";
  private static final String TABLE_META_PROVIDER = RECOMMENDER_PREFIX + "table-meta-provider";
  private static final String JOB_SUBMITTER = RECOMMENDER_PREFIX + "job-submitter";

  public static final ConfigEntry<String> STATS_PROVIDER_CONFIG =
      new ConfigBuilder(STATS_PROVIDER)
          .doc("The stats provider for the recommender.")
          .version(ConfigConstants.VERSION_1_1_0)
          .stringConf()
          .createWithDefault(GravitinoStatsProvider.GRAVITINO_STATS_PROVIDER_NAME);

  public static final ConfigEntry<String> POLICY_PROVIDER_CONFIG =
      new ConfigBuilder(POLICY_PROVIDER)
          .doc("The policy provider for the recommender.")
          .version(ConfigConstants.VERSION_1_1_0)
          .stringConf()
          .createWithDefault(GravitinoPolicyProvider.GRAVITINO_POLICY_PROVIDER_NAME);

  public static final ConfigEntry<String> TABLE_META_PROVIDER_CONFIG =
      new ConfigBuilder(TABLE_META_PROVIDER)
          .doc("The table meta provider for the recommender.")
          .version(ConfigConstants.VERSION_1_1_0)
          .stringConf()
          .createWithDefault(GravitinoTableMetadataProvider.GRAVITINO_TABLE_METADATA_PROVIDER_NAME);

  public static final ConfigEntry<String> JOB_SUBMITTER_CONFIG =
      new ConfigBuilder(JOB_SUBMITTER)
          .doc("The job submitter for the recommender.")
          .version(ConfigConstants.VERSION_1_1_0)
          .stringConf()
          .createWithDefault(GravitinoJobSubmitter.GRAVITINO_JOB_SUBMITTER_NAME);

  public static final ConfigEntry<String> GRAVITINO_URI_CONFIG =
      new ConfigBuilder(GRAVITINO_URI)
          .doc("The URI of the Gravitino server.")
          .version(ConfigConstants.VERSION_1_1_0)
          .stringConf()
          .createWithDefault("http://localhost:8090");

  public static final ConfigEntry<String> GRAVITINO_METALAKE_CONFIG =
      new ConfigBuilder(GRAVITINO_METALAKE)
          .doc("The metalake name in Gravitino.")
          .version(ConfigConstants.VERSION_1_1_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();
  public static final ConfigEntry<String> GRAVITINO_DEFAULT_CATALOG_CONFIG =
      new ConfigBuilder(GRAVITINO_DEFAULT_CATALOG)
          .doc("The default catalog name in Gravitino.")
          .version(ConfigConstants.VERSION_1_1_0)
          .stringConf()
          .create();

  public OptimizerConfig() {
    super(false);
  }

  public OptimizerConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }
}
