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

package org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestCompactionJobContext {

  @Test
  void jobContextExposesConfiguredFields() {
    NameIdentifier tableId = NameIdentifier.of("db", "table");
    Map<String, String> jobOptions = Map.of("k1", "v1");
    String jobTemplateName = "template-name";
    Column[] columns = new Column[] {Column.of("col1", Types.IntegerType.get())};
    Transform[] partitioning = new Transform[] {Transforms.identity("col1")};
    List<PartitionEntry> entries = List.of(new PartitionEntryImpl("col1", "1"));
    List<PartitionPath> partitions = List.of(PartitionPath.of(entries));

    CompactionJobContext context =
        new CompactionJobContext(
            tableId, jobOptions, jobTemplateName, columns, partitioning, partitions);

    Assertions.assertEquals(tableId, context.nameIdentifier());
    Assertions.assertEquals(jobOptions, context.jobOptions());
    Assertions.assertEquals(jobTemplateName, context.jobTemplateName());
    Assertions.assertArrayEquals(columns, context.getColumns());
    Assertions.assertArrayEquals(partitioning, context.getPartitioning());
    Assertions.assertEquals(partitions, context.getPartitions());
  }
}
