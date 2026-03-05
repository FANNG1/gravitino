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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction.CompactionJobContext;
import org.apache.gravitino.maintenance.optimizer.recommender.job.GravitinoJobAdapter;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Type;

public class PinterestCompactionJobAdapter implements GravitinoJobAdapter {

  public static final String JOB_TEMPLATE_NAME = "pinterest-compaction";

  @Override
  public Map<String, String> jobConfig(JobExecutionContext jobExecutionContext) {
    Preconditions.checkArgument(
        jobExecutionContext instanceof CompactionJobContext,
        "jobExecutionContext must be CompactionJobExecutionContext");
    CompactionJobContext jobContext = (CompactionJobContext) jobExecutionContext;
    return ImmutableMap.of(
        "table", getTableName(jobContext),
        "partitions", getPartitions(jobContext));
  }

  @Override
  public boolean supportsBatchJob() {
    return true;
  }

  private String getTableName(CompactionJobContext jobContext) {
    if (!jobContext.nameIdentifier().hasNamespace()) {
      throw new IllegalArgumentException("There is no database name in the name identifier");
    }
    int dbNameIndex = jobContext.nameIdentifier().namespace().length() - 1;
    String dbName = jobContext.nameIdentifier().namespace().level(dbNameIndex);
    String tableName = jobContext.nameIdentifier().name();
    return String.format("%s.%s", dbName, tableName);
  }

  private String getPartitions(CompactionJobContext jobContext) {
    List<PartitionPath> partitions = jobContext.getPartitions();
    if (partitions.isEmpty()) {
      return "";
    }

    Map<String, Column> columnNameToColumnMap =
        Arrays.stream(jobContext.getColumns())
            .collect(Collectors.toMap(Column::name, Function.identity()));

    return partitions.stream()
        .map(partitionPath -> getPartitionPredicate(partitionPath, columnNameToColumnMap))
        .map(predicate -> "(" + predicate + ")")
        .collect(Collectors.joining(" OR "));
  }

  private static String getPartitionPredicate(
      PartitionPath partitionPath, Map<String, Column> columnNameToColumnMap) {
    List<String> predicates = Lists.newArrayListWithExpectedSize(partitionPath.entries().size());
    for (PartitionEntry partitionEntry : partitionPath.entries()) {
      String partitionName = partitionEntry.partitionName();
      String partitionValue =
          formatPartitionLiteral(
              columnNameToColumnMap.get(partitionName), partitionEntry.partitionValue());
      predicates.add(String.format("%s = %s", partitionName, partitionValue));
    }
    return String.join(" AND ", predicates);
  }

  private static String formatPartitionLiteral(Column column, String rawValue) {
    Type type = column.dataType();
    return switch (type.name()) {
      case BOOLEAN -> rawValue.toLowerCase(Locale.ROOT);
      case BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, DECIMAL -> rawValue;
      default -> "\\\"" + escapeStringLiteral(rawValue) + "\\\"";
    };
  }

  private static String escapeStringLiteral(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}
