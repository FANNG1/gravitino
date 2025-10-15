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
package org.apache.gravitino.maintenance.optimizer.recommender.job;

import java.util.Arrays;
import java.util.List;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionEntry;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPartitionUtils {

  private static Column column(String name, org.apache.gravitino.rel.types.Type type) {
    return ColumnDTO.builder().withName(name).withDataType(type).build();
  }

  @Test
  void testIdentityStringPartition() {
    List<PartitionEntry> partitions = Arrays.asList(new PartitionEntryImpl("colA", "O'Hara"));
    Column[] columns = new Column[] {column("colA", Types.StringType.get())};
    Transform[] transforms = new Transform[] {Transforms.identity("colA")};

    String where = PartitionUtils.getWhereClauseForPartition(partitions, columns, transforms);

    Assertions.assertEquals("colA = 'O''Hara'", where);
  }

  @Test
  void testIdentityTimestampPartition() {
    List<PartitionEntry> partitions =
        Arrays.asList(new PartitionEntryImpl("ts", "2024-01-01 00:00:00"));
    Column[] columns = new Column[] {column("ts", Types.TimestampType.withTimeZone())};
    Transform[] transforms = new Transform[] {Transforms.identity("ts")};

    String where = PartitionUtils.getWhereClauseForPartition(partitions, columns, transforms);

    Assertions.assertEquals("ts = TIMESTAMP '2024-01-01 00:00:00'", where);
  }

  @Test
  void testBucketAndIdentityPartition() {
    List<PartitionEntry> partitions =
        Arrays.asList(new PartitionEntryImpl("colA", "abc"), new PartitionEntryImpl("bucket", "1"));
    Column[] columns =
        new Column[] {
          column("colA", Types.StringType.get()), column("colB", Types.IntegerType.get())
        };
    Transform[] transforms =
        new Transform[] {Transforms.identity("colA"), Transforms.bucket(2, new String[] {"colB"})};

    String where = PartitionUtils.getWhereClauseForPartition(partitions, columns, transforms);

    Assertions.assertEquals("colA = 'abc' AND bucket(colB, 2) = 1", where);
  }

  @Test
  void testTruncateAndYearPartition() {
    List<PartitionEntry> partitions =
        Arrays.asList(
            new PartitionEntryImpl("truncate", "prefix"), new PartitionEntryImpl("year", "2024"));
    Column[] columns =
        new Column[] {
          column("colC", Types.StringType.get()),
          column("colD", Types.TimestampType.withoutTimeZone())
        };
    Transform[] transforms =
        new Transform[] {Transforms.truncate(5, "colC"), Transforms.year("colD")};

    String where = PartitionUtils.getWhereClauseForPartition(partitions, columns, transforms);

    Assertions.assertEquals("truncate(colC, 5) = 'prefix' AND year(colD) = 2024", where);
  }
}
