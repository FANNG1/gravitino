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

package org.apache.gravitino.maintenance.optimizer.updater.metrics;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.common.MetricSampleImpl;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.PartitionMetricSampleImpl;
import org.apache.gravitino.maintenance.optimizer.common.StatisticEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.util.StatisticValueUtils;
import org.apache.gravitino.maintenance.optimizer.recommender.util.PartitionUtils;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.MetricRecord;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.MetricsRepository;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class TestGravitinoMetricsUpdater {

  @Test
  void testUpdateTableMetricsPersistsTableAndPartitionSamples() throws Exception {
    GravitinoMetricsUpdater updater = new GravitinoMetricsUpdater();
    MetricsRepository repository = Mockito.mock(MetricsRepository.class);
    setMetricsRepository(updater, repository);
    NameIdentifier tableId = NameIdentifier.of("catalog", "db", "table");

    updater.updateTableMetrics(
        tableId,
        List.of(
            new MetricSampleImpl(100L, stat("row_count", StatisticValues.longValue(10L))),
            new PartitionMetricSampleImpl(
                101L,
                stat("file_count", StatisticValues.longValue(3L)),
                PartitionPath.of(List.of(new PartitionEntryImpl("dt", "2026-01-01"))))));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Optional<String>> partitionCaptor = ArgumentCaptor.forClass(Optional.class);
    ArgumentCaptor<MetricRecord> recordCaptor = ArgumentCaptor.forClass(MetricRecord.class);

    Mockito.verify(repository, Mockito.times(2))
        .storeTableMetric(
            Mockito.eq(tableId),
            Mockito.anyString(),
            partitionCaptor.capture(),
            recordCaptor.capture());

    List<Optional<String>> partitions = partitionCaptor.getAllValues();
    Assertions.assertEquals(Optional.empty(), partitions.get(0));
    PartitionPath partitionPath =
        PartitionPath.of(List.of(new PartitionEntryImpl("dt", "2026-01-01")));
    Assertions.assertEquals(
        Optional.of(PartitionUtils.encodePartitionPath(partitionPath)),
        partitions.get(1),
        "Partition path should be encoded");

    List<MetricRecord> records = recordCaptor.getAllValues();
    Assertions.assertEquals(100L, records.get(0).getTimestamp());
    Assertions.assertEquals(101L, records.get(1).getTimestamp());

    StatisticValue<?> firstValue = StatisticValueUtils.fromString(records.get(0).getValue());
    StatisticValue<?> secondValue = StatisticValueUtils.fromString(records.get(1).getValue());
    Assertions.assertEquals(10L, firstValue.value());
    Assertions.assertEquals(3L, secondValue.value());
  }

  @Test
  void testUpdateJobMetricsPersistsSamples() throws Exception {
    GravitinoMetricsUpdater updater = new GravitinoMetricsUpdater();
    MetricsRepository repository = Mockito.mock(MetricsRepository.class);
    setMetricsRepository(updater, repository);
    NameIdentifier jobId = NameIdentifier.of("catalog", "db", "job");

    updater.updateJobMetrics(
        jobId,
        List.of(
            new MetricSampleImpl(200L, stat("duration", StatisticValues.longValue(20L))),
            new MetricSampleImpl(201L, stat("planning", StatisticValues.doubleValue(1.5D)))));

    ArgumentCaptor<MetricRecord> recordCaptor = ArgumentCaptor.forClass(MetricRecord.class);
    Mockito.verify(repository, Mockito.times(2))
        .storeJobMetric(Mockito.eq(jobId), Mockito.anyString(), recordCaptor.capture());

    List<MetricRecord> records = recordCaptor.getAllValues();
    Assertions.assertEquals(200L, records.get(0).getTimestamp());
    Assertions.assertEquals(201L, records.get(1).getTimestamp());

    StatisticValue<?> firstValue = StatisticValueUtils.fromString(records.get(0).getValue());
    StatisticValue<?> secondValue = StatisticValueUtils.fromString(records.get(1).getValue());
    Assertions.assertEquals(20L, firstValue.value());
    Assertions.assertEquals(1.5D, secondValue.value());
  }

  @Test
  void testCloseDelegatesToRepository() throws Exception {
    GravitinoMetricsUpdater updater = new GravitinoMetricsUpdater();
    MetricsRepository repository = Mockito.mock(MetricsRepository.class);
    setMetricsRepository(updater, repository);

    updater.close();

    Mockito.verify(repository).close();
  }

  private StatisticEntryImpl<?> stat(String name, StatisticValue<?> value) {
    return new StatisticEntryImpl<>(name, value);
  }

  private void setMetricsRepository(GravitinoMetricsUpdater updater, MetricsRepository repository)
      throws ReflectiveOperationException {
    Field field = GravitinoMetricsUpdater.class.getDeclaredField("metricsStorage");
    field.setAccessible(true);
    field.set(updater, repository);
  }
}
