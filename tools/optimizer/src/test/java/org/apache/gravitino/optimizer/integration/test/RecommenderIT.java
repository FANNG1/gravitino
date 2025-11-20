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

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.optimizer.api.common.SingleStatistic.Name;
import org.apache.gravitino.optimizer.api.recommender.PolicyActor.JobExecuteContext;
import org.apache.gravitino.optimizer.api.updater.StatsUpdater;
import org.apache.gravitino.optimizer.recommender.Recommender;
import org.apache.gravitino.optimizer.recommender.util.PolicyUtils;
import org.apache.gravitino.optimizer.updater.GravitinoStatsUpdater;
import org.apache.gravitino.optimizer.updater.SingleStatisticImpl;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/*
 * 1. update table stats
 * 2. add policy
 * 3. run recommender to get optimize result
 */
public class RecommenderIT extends GravitinoOptimizerEnvIT {

  private StatsUpdater statsUpdater;
  private static final String STATS_PREFIX = "custom-";

  private static final String DATAFILE_MSE =
      STATS_PREFIX + Name.DATAFILE_SIZE_MSE.name().toLowerCase();
  private static final String DELETE_FILE_NUM =
      STATS_PREFIX + Name.POSITION_DELETE_FILE_NUMBER.name().toLowerCase();

  @BeforeAll
  void init() {
    this.statsUpdater = new GravitinoStatsUpdater();
    statsUpdater.initialize(optimizerEnv);
  }

  @Test
  void testRecommendNonPartitionTable() {

    String policyForDelete = "policyForDelete";
    String policyForSmallFile = "policyForSmallFile";

    String tableWithSmallFile = "tableWithSmallFile";
    String tableWithDeleteFile = "tableWithDeleteFile";
    String tableWithoutCompaction = "tableWithoutCompaction";

    // create table
    createTable(tableWithSmallFile);
    createTable(tableWithDeleteFile);
    createTable(tableWithoutCompaction);

    // policy1 give more weight to datafile_mse
    createPolicy(
        policyForSmallFile,
        ImmutableMap.of(
            "min_datafile_mse",
            1000,
            PolicyUtils.TRIGGER_EXPR,
            DATAFILE_MSE + " > min_datafile_mse || " + DELETE_FILE_NUM + " > 0",
            PolicyUtils.SCORE_EXPR,
            DATAFILE_MSE + " + " + DELETE_FILE_NUM + " * 10"),
        PolicyUtils.COMPACTION_POLICY_TYPE);

    // policy2 give more weight to delete file number
    createPolicy(
        policyForDelete,
        ImmutableMap.of(
            "min_datafile_mse",
            1000,
            PolicyUtils.TRIGGER_EXPR,
            DATAFILE_MSE + " > min_datafile_mse || " + DELETE_FILE_NUM + " > 1",
            PolicyUtils.SCORE_EXPR,
            DATAFILE_MSE + "/100 + " + DELETE_FILE_NUM + " * 100"),
        PolicyUtils.COMPACTION_POLICY_TYPE);

    associatePoliciesToSchema(policyForSmallFile, TEST_SCHEMA);
    associatePoliciesToSchema(policyForDelete, TEST_SCHEMA);

    // update table stats with high datafile_mse and low delete file number
    statsUpdater.updateTableStatistics(
        NameIdentifier.of(TEST_SCHEMA, tableWithSmallFile),
        Arrays.asList(
            new SingleStatisticImpl(DELETE_FILE_NUM, StatisticValues.longValue(2)),
            new SingleStatisticImpl(DATAFILE_MSE, StatisticValues.doubleValue(10000.1))));

    // update table stats with low datafile_mse and high delete file number
    statsUpdater.updateTableStatistics(
        NameIdentifier.of(TEST_SCHEMA, tableWithDeleteFile),
        Arrays.asList(
            new SingleStatisticImpl(DELETE_FILE_NUM, StatisticValues.longValue(100)),
            new SingleStatisticImpl(DATAFILE_MSE, StatisticValues.doubleValue(100.1))));

    // table 3 should not be triggered by any policy
    statsUpdater.updateTableStatistics(
        NameIdentifier.of(TEST_SCHEMA, tableWithoutCompaction),
        Arrays.asList(
            new SingleStatisticImpl(DELETE_FILE_NUM, StatisticValues.longValue(0)),
            new SingleStatisticImpl(DATAFILE_MSE, StatisticValues.doubleValue(0))));

    Recommender recommender = new Recommender(optimizerEnv);
    List<JobExecuteContext> jobs =
        recommender.recommendForOnePolicy(
            Arrays.asList(
                NameIdentifier.of(TEST_SCHEMA, tableWithSmallFile),
                NameIdentifier.of(TEST_SCHEMA, tableWithDeleteFile),
                NameIdentifier.of(TEST_SCHEMA, tableWithoutCompaction)),
            policyForSmallFile);
    Assertions.assertEquals(2, jobs.size());

    // policyForSmallFile will select table with small file first
    Assertions.assertEquals(tableWithSmallFile, jobs.get(0).identifier().name());
    Assertions.assertEquals(tableWithDeleteFile, jobs.get(1).identifier().name());

    jobs =
        recommender.recommendForOnePolicy(
            Arrays.asList(
                NameIdentifier.of(TEST_SCHEMA, tableWithSmallFile),
                NameIdentifier.of(TEST_SCHEMA, tableWithDeleteFile),
                NameIdentifier.of(TEST_SCHEMA, tableWithoutCompaction)),
            policyForDelete);
    Assertions.assertEquals(2, jobs.size());

    // policyForDelete will select table with delete file first
    Assertions.assertEquals(tableWithDeleteFile, jobs.get(0).identifier().name());
    Assertions.assertEquals(tableWithSmallFile, jobs.get(1).identifier().name());
  }

  @Test
  void testCompactionPartitionTable() {}
}
