package org.apache.gravitino.updater.impl.iceberg;

import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Data;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.stats.StatisticValues;
import org.apache.gravitino.updater.api.SingleStatistic;
import org.apache.gravitino.updater.api.SupportTableStats;
import org.apache.gravitino.updater.impl.SingleStatisticImpl;
import org.apache.gravitino.updater.impl.util.ToStatistic;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;

public class IcebergTableDataStatsComputer implements SupportTableStats {

  private SparkSession sparkSession;
  private String catalogName = "rest";

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
          + "FROM %s.%s.files";

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
          + "FROM %s.%s.files\n"
          + "GROUP BY partition\n"
          + "ORDER BY partition";

  public IcebergTableDataStatsComputer(SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }

  public List<SingleStatistic<?>> computeTableStats(NameIdentifier tableIdentifier) {
    // For non-partitioned table return table stats
    TableStats tableStats = getTableStats(tableIdentifier);
    return tableStats.toStatistic();
    // For partitioned table, return partition stats
  }

  @Override
  public String name() {
    return "";
  }

  private TableStats getTableStats(NameIdentifier nameIdentifier) {
    String sql = String.format(TABLE_STATS_SQL_TEMPLATE, catalogName, nameIdentifier.toString());
    Dataset<Row> df = sparkSession.sql(sql);
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
    Dataset<Row> df = sparkSession.sql(sql);
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
    public List<SingleStatistic<?>> toStatistic() {
      return List.of(
          new SingleStatisticImpl("data_files", StatisticValues.longValue(dataFiles)),
          new SingleStatisticImpl(
              "position_delete_files", StatisticValues.longValue(positionDeleteFiles)),
          new SingleStatisticImpl(
              "equality_delete_files", StatisticValues.longValue(equalityDeleteFiles)),
          new SingleStatisticImpl("small_files", StatisticValues.longValue(smallFiles)),
          new SingleStatisticImpl("data_size_mse", StatisticValues.longValue(dataSizeMSE)));
    }
  }
}
