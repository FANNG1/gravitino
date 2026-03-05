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

package com.pinterest.maintenance.optimizer.recommender.job;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergColumn;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction.CompactionJobContext;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPinterestCompactionJobAdapter {

  private final PinterestCompactionJobAdapter adapter = new PinterestCompactionJobAdapter();

  @Test
  void testJobConfigWithoutPartitions() {
    String dbName = "default";
    String tableName = "table";
    String jobTemplateName = "compaction-job";
    CompactionJobContext context =
        new CompactionJobContext(
            NameIdentifier.of("catalog", dbName, tableName),
            Collections.emptyMap(),
            jobTemplateName,
            new Column[] {},
            new Transform[] {},
            Collections.emptyList());

    Map<String, String> jobConfig = adapter.jobConfig(context);

    Assertions.assertNotNull(jobConfig);
    Assertions.assertEquals(2, jobConfig.size());
    Assertions.assertEquals(dbName + "." + tableName, jobConfig.get("table"));
    Assertions.assertEquals("", jobConfig.get("partitions"));
  }

  @Test
  void testJobConfigWithSinglePartition() {
    String dbName = "default";
    String tableName = "table";
    Column[] columns =
        new Column[] {
          IcebergColumn.builder().withName("dt").withType(Types.VarCharType.of(10)).build()
        };
    Transform[] partitioning = new Transform[] {Transforms.identity("dt")};
    String jobTemplateName = "compaction-job";
    List<PartitionPath> partitions =
        List.of(PartitionPath.of(List.of(new PartitionEntryImpl("dt", "2026-01-01"))));
    CompactionJobContext context =
        new CompactionJobContext(
            NameIdentifier.of("catalog", dbName, tableName),
            Collections.emptyMap(),
            jobTemplateName,
            columns,
            partitioning,
            partitions);

    Map<String, String> jobConfig = adapter.jobConfig(context);

    Assertions.assertNotNull(jobConfig);
    Assertions.assertEquals(2, jobConfig.size());
    Assertions.assertEquals(dbName + "." + tableName, jobConfig.get("table"));
    Assertions.assertEquals("(dt = \\\"2026-01-01\\\")", jobConfig.get("partitions"));
  }

  @Test
  void testJobConfigWithMultiplePartitions() {
    String dbName = "default";
    String tableName = "table";
    Column[] columns =
        new Column[] {
          IcebergColumn.builder().withName("dt").withType(Types.VarCharType.of(10)).build(),
          IcebergColumn.builder().withName("hr").withType(Types.VarCharType.of(2)).build()
        };
    Transform[] partitioning =
        new Transform[] {Transforms.identity("dt"), Transforms.identity("hr")};
    String jobTemplateName = "compaction-job";
    List<PartitionPath> partitions =
        List.of(
            PartitionPath.of(
                List.of(
                    new PartitionEntryImpl("dt", "2026-01-01"),
                    new PartitionEntryImpl("hr", "00"))),
            PartitionPath.of(
                List.of(
                    new PartitionEntryImpl("dt", "2026-01-02"),
                    new PartitionEntryImpl("hr", "01"))));
    CompactionJobContext context =
        new CompactionJobContext(
            NameIdentifier.of("catalog", dbName, tableName),
            Collections.emptyMap(),
            jobTemplateName,
            columns,
            partitioning,
            partitions);

    Map<String, String> jobConfig = adapter.jobConfig(context);

    Assertions.assertNotNull(jobConfig);
    Assertions.assertEquals(2, jobConfig.size());
    Assertions.assertEquals(dbName + "." + tableName, jobConfig.get("table"));
    Assertions.assertEquals(
        "(dt = \\\"2026-01-01\\\" AND hr = \\\"00\\\") OR (dt = \\\"2026-01-02\\\" AND hr = \\\"01\\\")",
        jobConfig.get("partitions"));
  }

  @Test
  void testSupportsBatchJob() {
    Assertions.assertTrue(adapter.supportsBatchJob());
  }

  @Test
  void testJobConfigWithPartitionsEscapesDoubleQuotes() {
    String dbName = "default";
    String tableName = "table";
    Column[] columns =
        new Column[] {
          IcebergColumn.builder().withName("str").withType(Types.StringType.get()).build(),
          IcebergColumn.builder().withName("vc").withType(Types.VarCharType.of(10)).build(),
          IcebergColumn.builder().withName("ch").withType(Types.FixedCharType.of(5)).build(),
          IcebergColumn.builder().withName("dt").withType(Types.DateType.get()).build(),
          IcebergColumn.builder()
              .withName("ts")
              .withType(Types.TimestampType.withoutTimeZone())
              .build(),
          IcebergColumn.builder().withName("tm").withType(Types.TimeType.get()).build()
        };
    Transform[] partitioning =
        new Transform[] {
          Transforms.identity("str"),
          Transforms.identity("vc"),
          Transforms.identity("ch"),
          Transforms.identity("dt"),
          Transforms.identity("ts"),
          Transforms.identity("tm")
        };
    String jobTemplateName = "compaction-job";
    List<PartitionPath> partitions =
        List.of(
            PartitionPath.of(
                List.of(
                    new PartitionEntryImpl("str", "hello"),
                    new PartitionEntryImpl("vc", "world"),
                    new PartitionEntryImpl("ch", "abc"),
                    new PartitionEntryImpl("dt", "2026-01-01"),
                    new PartitionEntryImpl("ts", "2026-01-01T12:00:00"),
                    new PartitionEntryImpl("tm", "12:00:00"))));
    CompactionJobContext context =
        new CompactionJobContext(
            NameIdentifier.of("catalog", dbName, tableName),
            Collections.emptyMap(),
            jobTemplateName,
            columns,
            partitioning,
            partitions);

    Map<String, String> jobConfig = adapter.jobConfig(context);

    Assertions.assertNotNull(jobConfig);
    String partitionsValue = jobConfig.get("partitions");
    Assertions.assertEquals(
        "(str = \\\"hello\\\" AND vc = \\\"world\\\" AND ch = \\\"abc\\\" AND dt = \\\"2026-01-01\\\" AND ts = \\\"2026-01-01T12:00:00\\\" AND tm = \\\"12:00:00\\\")",
        partitionsValue);
  }

  @Test
  void testJobConfigWithPartitionsNoEscaping() {
    String dbName = "default";
    String tableName = "table";
    Column[] columns =
        new Column[] {
          IcebergColumn.builder().withName("b").withType(Types.ByteType.get()).build(),
          IcebergColumn.builder().withName("s").withType(Types.ShortType.get()).build(),
          IcebergColumn.builder().withName("i").withType(Types.IntegerType.get()).build(),
          IcebergColumn.builder().withName("l").withType(Types.LongType.get()).build(),
          IcebergColumn.builder().withName("f").withType(Types.FloatType.get()).build(),
          IcebergColumn.builder().withName("d").withType(Types.DoubleType.get()).build(),
          IcebergColumn.builder().withName("dec").withType(Types.DecimalType.of(10, 2)).build(),
          IcebergColumn.builder().withName("bool").withType(Types.BooleanType.get()).build()
        };
    Transform[] partitioning =
        new Transform[] {
          Transforms.identity("b"),
          Transforms.identity("s"),
          Transforms.identity("i"),
          Transforms.identity("l"),
          Transforms.identity("f"),
          Transforms.identity("d"),
          Transforms.identity("dec"),
          Transforms.identity("bool")
        };
    String jobTemplateName = "compaction-job";
    List<PartitionPath> partitions =
        List.of(
            PartitionPath.of(
                List.of(
                    new PartitionEntryImpl("b", "1"),
                    new PartitionEntryImpl("s", "2"),
                    new PartitionEntryImpl("i", "3"),
                    new PartitionEntryImpl("l", "4"),
                    new PartitionEntryImpl("f", "5.5"),
                    new PartitionEntryImpl("d", "6.6"),
                    new PartitionEntryImpl("dec", "7.77"),
                    new PartitionEntryImpl("bool", "true"))));
    CompactionJobContext context =
        new CompactionJobContext(
            NameIdentifier.of("catalog", dbName, tableName),
            Collections.emptyMap(),
            jobTemplateName,
            columns,
            partitioning,
            partitions);

    Map<String, String> jobConfig = adapter.jobConfig(context);

    Assertions.assertNotNull(jobConfig);
    String partitionsValue = jobConfig.get("partitions");
    Assertions.assertEquals(
        "(b = 1 AND s = 2 AND i = 3 AND l = 4 AND f = 5.5 AND d = 6.6 AND dec = 7.77 AND bool = true)",
        partitionsValue);
  }

  @Test
  void testJobConfigWithBooleanPartition() {
    String dbName = "default";
    String tableName = "table";
    Column[] columns =
        new Column[] {
          IcebergColumn.builder().withName("flag").withType(Types.BooleanType.get()).build()
        };
    Transform[] partitioning = new Transform[] {Transforms.identity("flag")};
    String jobTemplateName = "compaction-job";
    List<PartitionPath> partitions =
        List.of(
            PartitionPath.of(List.of(new PartitionEntryImpl("flag", "TRUE"))),
            PartitionPath.of(List.of(new PartitionEntryImpl("flag", "FALSE"))));
    CompactionJobContext context =
        new CompactionJobContext(
            NameIdentifier.of("catalog", dbName, tableName),
            Collections.emptyMap(),
            jobTemplateName,
            columns,
            partitioning,
            partitions);

    Map<String, String> jobConfig = adapter.jobConfig(context);

    Assertions.assertNotNull(jobConfig);
    String partitionsValue = jobConfig.get("partitions");
    Assertions.assertEquals("(flag = true) OR (flag = false)", partitionsValue);
  }

  @Test
  void testJobConfigWithPartitionsEscapingQuotesAndBackslashes() {
    String dbName = "default";
    String tableName = "table";
    Column[] columns =
        new Column[] {
          IcebergColumn.builder().withName("path").withType(Types.VarCharType.of(100)).build()
        };
    Transform[] partitioning = new Transform[] {Transforms.identity("path")};
    String jobTemplateName = "compaction-job";
    List<PartitionPath> partitions =
        List.of(PartitionPath.of(List.of(new PartitionEntryImpl("path", "a\"b\\c"))));
    CompactionJobContext context =
        new CompactionJobContext(
            NameIdentifier.of("catalog", dbName, tableName),
            Collections.emptyMap(),
            jobTemplateName,
            columns,
            partitioning,
            partitions);

    Map<String, String> jobConfig = adapter.jobConfig(context);

    Assertions.assertNotNull(jobConfig);
    String partitionsValue = jobConfig.get("partitions");
    Assertions.assertEquals("(path = \\\"a\\\"b\\\\c\\\")", partitionsValue);
  }
}
