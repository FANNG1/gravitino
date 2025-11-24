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

package org.apache.gravitino.optimizer.updater.computer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Data;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.optimizer.api.updater.SupportTableStats;
import org.apache.gravitino.optimizer.common.OptimizerEnv;
import org.apache.gravitino.optimizer.updater.StatisticEntryImpl;
import org.apache.gravitino.optimizer.updater.util.ToStatistic;
import org.apache.gravitino.stats.StatisticValues;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;

public class IcebergTableDataStatsComputer implements SupportTableStats {

  public static final String NAME = "gravitino-iceberg-datasize";
  private SparkSession sparkSession;
  private static String TABLE_STATS_SQL_TEMPLATE =
      "SELECT \n"
          + "    COUNT(*) AS file_count,\n"
          + "    SUM(CASE WHEN content = 0 THEN 1 ELSE 0 END) AS data_files,\n"
          + "    SUM(CASE WHEN content = 1 THEN 1 ELSE 0 END) AS position_delete_files,\n"
          + "    SUM(CASE WHEN content = 2 THEN 1 ELSE 0 END) AS equality_delete_files,\n"
          + "    SUM(CASE WHEN file_size_in_bytes < 100000 THEN 1 ELSE 0 END) AS small_files,\n"
          + "   AVG(POWER(100000 - LEAST(100000, file_size_in_bytes), 2)) AS data_size_mse,\n"
          + "    AVG(file_size_in_bytes) AS avg_size,\n"
          + "    SUM(file_size_in_bytes) AS total_size\n"
          + "FROM %s.files";

  private static String PARTITIONED_TABLE_STATS_SQL_TEMPLATE =
      "SELECT \n"
          + "    partition,\n"
          + "    COUNT(*) AS file_count,\n"
          + "    SUM(CASE WHEN content = 0 THEN 1 ELSE 0 END) AS data_files,\n"
          + "    SUM(CASE WHEN content = 1 THEN 1 ELSE 0 END) AS position_delete_files,\n"
          + "    SUM(CASE WHEN content = 2 THEN 1 ELSE 0 END) AS equality_delete_files,\n"
          + "    SUM(CASE WHEN file_size_in_bytes < 100000 THEN 1 ELSE 0 END) AS small_files,\n"
          + "   AVG(POWER(100000 - LEAST(100000, file_size_in_bytes), 2)) AS data_size_mse,\n"
          + "    AVG(file_size_in_bytes) AS avg_size,\n"
          + "    SUM(file_size_in_bytes) AS total_size\n"
          + "FROM %s.files\n"
          + "GROUP BY partition\n"
          + "ORDER BY partition";

  @VisibleForTesting
  public void setSparkSessionForTest(SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }

  public List<StatisticEntry<?>> computeTableStats(NameIdentifier tableIdentifier) {
    // For non-partitioned table return table stats
    TableStats tableStats = getTableStats(tableIdentifier);
    return tableStats.toStatistic();
    // For partitioned table, return partition stats
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {}

  private SparkSession getSparkSession() {
    if (sparkSession == null)
      this.sparkSession = SparkSession.builder().appName(NAME).getOrCreate();
    return sparkSession;
  }

  private TableStats getTableStats(NameIdentifier nameIdentifier) {
    Preconditions.checkArgument(
        nameIdentifier.namespace().levels().length == 2,
        "Iceberg table identifier should contain catalog and schema");
    String sql = String.format(TABLE_STATS_SQL_TEMPLATE, nameIdentifier);
    Dataset<Row> df = getSparkSession().sql(sql);
    Row row = df.collectAsList().get(0);
    TableStats tableStats =
        TableStats.builder()
            .dataFiles(row.getAs("data_files"))
            .positionDeleteFiles(row.getAs("position_delete_files"))
            .equalityDeleteFiles(row.getAs("equality_delete_files"))
            .smallFiles(row.getAs("small_files"))
            .dataSizeMSE(row.getAs("data_size_mse"))
            .build();
    return tableStats;
  }

  private Map<GenericRowWithSchema, TableStats> getPartitionedTableStats() {
    String sql = String.format(PARTITIONED_TABLE_STATS_SQL_TEMPLATE, "rest", "db_table");
    Dataset<Row> df = getSparkSession().sql(sql);
    List<Row> rows = df.collectAsList();
    Map<GenericRowWithSchema, TableStats> tableStatsMap = new java.util.HashMap<>();
    for (Row row : rows) {
      // partition is a GenericRowWithSchema
      GenericRowWithSchema partition = row.getAs("partition");
      Object a = partition.get(0);
      Object b = partition.get(1);
      System.out.println(a);
      System.out.println(b);
      TableStats tableStats =
          TableStats.builder()
              .dataFiles(row.getAs("data_files"))
              .positionDeleteFiles(row.getAs("position_delete_files"))
              .equalityDeleteFiles(row.getAs("equality_delete_files"))
              .smallFiles(row.getAs("small_files"))
              .dataSizeMSE(row.getAs("data_size_mse"))
              .build();
      tableStatsMap.put(partition, tableStats);
    }
    return tableStatsMap;
  }

  @Data
  @Builder
  public static class TableStats implements ToStatistic {
    public long dataFiles;
    public long positionDeleteFiles;
    public long equalityDeleteFiles;
    public long smallFiles;
    public long dataSizeMSE;

    @Override
    public List<StatisticEntry<?>> toStatistic() {
      return List.of(
          new StatisticEntryImpl("data_files", StatisticValues.longValue(dataFiles)),
          new StatisticEntryImpl(
              "position_delete_files", StatisticValues.longValue(positionDeleteFiles)),
          new StatisticEntryImpl(
              "equality_delete_files", StatisticValues.longValue(equalityDeleteFiles)),
          new StatisticEntryImpl("small_files", StatisticValues.longValue(smallFiles)),
          new StatisticEntryImpl("data_size_mse", StatisticValues.longValue(dataSizeMSE)));
    }
  }
}
