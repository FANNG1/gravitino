package org.apache.gravitino.updater.impl.iceberg;

import org.apache.gravitino.NameIdentifier;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestIcebergTableDataStatsComputer {

  private IcebergTableDataStatsComputer statsComputer;

  @BeforeAll
  public void setUp() {
    String icebergRESTUri = String.format("http://127.0.0.1:%d/iceberg/", 9001);
    SparkConf sparkConf =
        new SparkConf()
            .set(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .set("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
            .set("spark.sql.catalog.rest.type", "rest")
            .set("spark.sql.catalog.rest.uri", icebergRESTUri)
            .set("spark.locality.wait.node", "0");

    SparkSession sparkSession =
        SparkSession.builder()
            .appName("IcebergTableDataStatsComputerTest")
            .master("local")
            .config(sparkConf)
            .getOrCreate();
    statsComputer = new IcebergTableDataStatsComputer(sparkSession);
  }

  @Test
  public void testComputeTableStats() {
    statsComputer.computeTableStats(NameIdentifier.of("ab", "p1"));
  }
}
