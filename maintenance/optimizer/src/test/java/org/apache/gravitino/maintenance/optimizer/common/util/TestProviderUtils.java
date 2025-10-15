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

package org.apache.gravitino.maintenance.optimizer.common.util;

import org.apache.gravitino.maintenance.optimizer.api.monitor.JobProvider;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricsProvider;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobSubmitter;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StatisticsProvider;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyProvider;
import org.apache.gravitino.maintenance.optimizer.api.recommender.TableMetadataProvider;
import org.apache.gravitino.maintenance.optimizer.monitor.callback.MonitorCallbackForTest;
import org.apache.gravitino.maintenance.optimizer.monitor.job.JobProviderForTest;
import org.apache.gravitino.maintenance.optimizer.monitor.metrics.MetricsProviderForTest;
import org.apache.gravitino.maintenance.optimizer.api.updater.MetricsUpdater;
import org.apache.gravitino.maintenance.optimizer.api.updater.StatisticsUpdater;
import org.apache.gravitino.maintenance.optimizer.monitor.job.DummyJobProvider;
import org.apache.gravitino.maintenance.optimizer.monitor.metrics.GravitinoMetricsProvider;
import org.apache.gravitino.maintenance.optimizer.recommender.job.GravitinoJobSubmitter;
import org.apache.gravitino.maintenance.optimizer.recommender.job.NoopJobSubmitter;
import org.apache.gravitino.maintenance.optimizer.recommender.statistics.GravitinoStatisticsProvider;
import org.apache.gravitino.maintenance.optimizer.recommender.strategy.GravitinoStrategyProvider;
import org.apache.gravitino.maintenance.optimizer.recommender.table.GravitinoTableMetadataProvider;
import org.apache.gravitino.maintenance.optimizer.updater.GravitinoStatisticsUpdater;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.GravitinoMetricsUpdater;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestProviderUtils {

  @Test
  public void testCreateStrategyProviderInstance() {
    StrategyProvider strategyProvider =
        ProviderUtils.createStrategyProviderInstance(GravitinoStrategyProvider.NAME);
    Assertions.assertNotNull(strategyProvider);
    Assertions.assertTrue(strategyProvider instanceof GravitinoStrategyProvider);
  }

  @Test
  public void testCreateJobSubmitterInstance() {
    JobSubmitter jobSubmitter =
        ProviderUtils.createJobSubmitterInstance(GravitinoJobSubmitter.NAME);
    Assertions.assertNotNull(jobSubmitter);
    Assertions.assertTrue(jobSubmitter instanceof GravitinoJobSubmitter);

    jobSubmitter = ProviderUtils.createJobSubmitterInstance(NoopJobSubmitter.NAME);
    Assertions.assertNotNull(jobSubmitter);
    Assertions.assertTrue(jobSubmitter instanceof NoopJobSubmitter);
  }

  @Test
  public void testCreateStatisticsProviderInstance() {
    StatisticsProvider statisticsProvider =
        ProviderUtils.createStatisticsProviderInstance(GravitinoStatisticsProvider.NAME);
    Assertions.assertNotNull(statisticsProvider);
    Assertions.assertTrue(statisticsProvider instanceof GravitinoStatisticsProvider);
  }

  @Test
  public void testCreateTableMetadataProviderInstance() {
    TableMetadataProvider tableMetadataProvider =
        ProviderUtils.createTableMetadataProviderInstance(GravitinoTableMetadataProvider.NAME);
    Assertions.assertNotNull(tableMetadataProvider);
    Assertions.assertTrue(tableMetadataProvider instanceof GravitinoTableMetadataProvider);
  }

  @Test
  public void testCreateMonitorProviders() {
    Assertions.assertTrue(
        ProviderUtils.createMetricsProviderInstance(MetricsProviderForTest.NAME)
            instanceof MetricsProviderForTest);
    Assertions.assertTrue(
        ProviderUtils.createJobProviderInstance(JobProviderForTest.NAME)
            instanceof JobProviderForTest);
    Assertions.assertTrue(
        ProviderUtils.createMonitorCallbackInstance(MonitorCallbackForTest.NAME)
            instanceof MonitorCallbackForTest);
  }

  @Test
  public void testCreateStatisticsUpdaterInstance() {
    StatisticsUpdater statisticsUpdater =
        ProviderUtils.createStatisticsUpdaterInstance(GravitinoStatisticsUpdater.NAME);
    Assertions.assertNotNull(statisticsUpdater);
    Assertions.assertTrue(statisticsUpdater instanceof GravitinoStatisticsUpdater);
  }

  @Test
  public void testCreateMetricsUpdaterInstance() {
    MetricsUpdater metricsUpdater =
        ProviderUtils.createMetricsUpdaterInstance(GravitinoMetricsUpdater.NAME);
    Assertions.assertNotNull(metricsUpdater);
    Assertions.assertTrue(metricsUpdater instanceof GravitinoMetricsUpdater);
  }

  @Test
  public void testCreateMetricsProviderInstance() {
    MetricsProvider metricsProvider =
        ProviderUtils.createMetricsProviderInstance(GravitinoMetricsProvider.NAME);
    Assertions.assertNotNull(metricsProvider);
    Assertions.assertTrue(metricsProvider instanceof GravitinoMetricsProvider);
  }

  @Test
  public void testCreateJobsProviderInstance() {
    JobProvider jobProvider = ProviderUtils.createJobProviderInstance(DummyJobProvider.NAME);
    Assertions.assertNotNull(jobProvider);
    Assertions.assertTrue(jobProvider instanceof DummyJobProvider);
  }
}
