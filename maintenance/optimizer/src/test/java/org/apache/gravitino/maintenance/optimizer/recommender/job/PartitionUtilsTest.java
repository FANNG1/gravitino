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
 * Unless required by applicable law or agreed in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.maintenance.optimizer.recommender.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionEntry;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

public class PartitionUtilsTest {

  @Test
  public void whereClauseCombinesMultiplePartitionsWithParenthesesForStringColumns() {
    Column[] columns =
        new Column[] {
          Column.of("a", Types.StringType.get()), Column.of("b", Types.StringType.get())
        };

    // partitions: two partitions, each with two partition entry values corresponding to transforms
    List<PartitionEntry> p1 =
        Arrays.<PartitionEntry>asList(
            new PartitionEntryImpl("p1", "x1"), new PartitionEntryImpl("p1", "y1"));
    List<PartitionEntry> p2 =
        Arrays.<PartitionEntry>asList(
            new PartitionEntryImpl("p2", "x2"), new PartitionEntryImpl("p2", "y2"));
    List<List<PartitionEntry>> partitions = Arrays.<List<PartitionEntry>>asList(p1, p2);

    Transform[] partitioning = new Transform[] {Transforms.identity("a"), Transforms.identity("b")};

    String where = PartitionUtils.getWhereClauseForPartitions(partitions, columns, partitioning);

    assertEquals("(a = 'x1' AND b = 'y1') AND (a = 'x2' AND b = 'y2')", where);
  }

  @Test
  public void whereClauseEmitsNumericLiteralsWithoutQuotes() {
    Column[] columns =
        new Column[] {
          Column.of("id", Types.LongType.get()), Column.of("score", Types.DoubleType.get())
        };

    List<PartitionEntry> p1 =
        Arrays.<PartitionEntry>asList(
            new PartitionEntryImpl("p1", "123"), new PartitionEntryImpl("p1", "45.6"));
    List<PartitionEntry> p2 =
        Arrays.<PartitionEntry>asList(
            new PartitionEntryImpl("p2", "456"), new PartitionEntryImpl("p2", "78.9"));
    List<List<PartitionEntry>> partitions = Arrays.<List<PartitionEntry>>asList(p1, p2);

    Transform[] partitioning =
        new Transform[] {Transforms.identity("id"), Transforms.identity("score")};

    String where = PartitionUtils.getWhereClauseForPartitions(partitions, columns, partitioning);

    assertEquals("(id = 123 AND score = 45.6) AND (id = 456 AND score = 78.9)", where);
  }

  @Test
  public void identityOnDateColumnIsFormattedAsDateLiteral() {
    Column[] columns = new Column[] {Column.of("dt", Types.DateType.get())};

    // Single partition case: outer list size = 1, inner list size = 1, partitioning length = 1
    List<PartitionEntry> p1 =
        Arrays.<PartitionEntry>asList(new PartitionEntryImpl("p", "2024-01-01"));
    List<List<PartitionEntry>> partitions = Arrays.<List<PartitionEntry>>asList(p1);

    Transform[] partitioning = new Transform[] {Transforms.identity("dt")};

    String where = PartitionUtils.getWhereClauseForPartitions(partitions, columns, partitioning);

    assertEquals("(dt = DATE '2024-01-01')", where);
  }

  @Test
  public void nullPartitionsThrowsIllegalArgumentException() {
    Column[] columns = new Column[] {Column.of("a", Types.StringType.get())};
    Transform[] partitioning = new Transform[] {Transforms.identity("a")};

    assertThrows(
        IllegalArgumentException.class,
        () -> PartitionUtils.getWhereClauseForPartitions(null, columns, partitioning));
  }

  @Test
  public void emptyColumnsThrowsIllegalArgumentException() {
    List<PartitionEntry> p1 = Arrays.<PartitionEntry>asList(new PartitionEntryImpl("p", "v"));
    List<List<PartitionEntry>> partitions = Arrays.<List<PartitionEntry>>asList(p1);
    Column[] columns = new Column[0];
    Transform[] partitioning = new Transform[] {Transforms.identity("p")};

    assertThrows(
        IllegalArgumentException.class,
        () -> PartitionUtils.getWhereClauseForPartitions(partitions, columns, partitioning));
  }
}
