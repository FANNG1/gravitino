/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.spark;

import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.METASTORE_URIS;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.client.GravitinoMetaLake;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.HiveContainer;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.SparkUtilIT;
import com.datastrato.gravitino.spark.GravitinoSparkConfig;
import com.datastrato.gravitino.spark.plugin.GravitinoSparkPlugin;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Setup Hive, Gravitino, Spark, Metalake environment to execute SparkSQL. */
public class SparkBaseIT extends SparkUtilIT {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractIT.class);
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  protected final String hiveCatalogName = "hive";
  private final String metalakeName = "test";

  private SparkSession sparkSession;
  private String hiveMetastoreUri;
  private String gravitinoUrl;

  @Override
  protected SparkSession getSparkSession() {
    Assertions.assertNotNull(sparkSession);
    return sparkSession;
  }

  @BeforeAll
  void startup() throws Exception {
    initHiveEnv();
    initGravitinoEnv();
    initMetalakeCatalogs();
    initSparkEnv();
    LOG.info(
        "Startup Spark env succ, gravitino url: {}, hive metastore uri: {}",
        gravitinoUrl,
        hiveMetastoreUri);
  }

  @AfterAll
  void stop() {
    if (sparkSession != null) {
      sparkSession.close();
    }
  }

  private void initMetalakeCatalogs() {
    client.createMetalake(NameIdentifier.of(metalakeName), "", Collections.emptyMap());
    GravitinoMetaLake metalake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Map<String, String> properties = Maps.newHashMap();
    properties.put(METASTORE_URIS, hiveMetastoreUri);

    metalake.createCatalog(
        NameIdentifier.of(metalakeName, hiveCatalogName),
        Catalog.Type.RELATIONAL,
        "hive",
        "",
        properties);
  }

  private void initGravitinoEnv() {
    // Gravitino server is already started by AbstractIT, just construct gravitinoUrl
    int gravitinoPort = getGravitinoServerPort();
    gravitinoUrl = String.format("http://127.0.0.1:%d", gravitinoPort);
  }

  private void initHiveEnv() {
    containerSuite.startHiveContainer();
    hiveMetastoreUri =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);
  }

  private void initSparkEnv() {
    sparkSession =
        SparkSession.builder()
            .master("local[1]")
            .appName("Spark connector integration test")
            .config("spark.plugins", GravitinoSparkPlugin.class.getName())
            .config(GravitinoSparkConfig.GRAVITINO_URL, gravitinoUrl)
            .config(GravitinoSparkConfig.GRAVITINO_METALAKE, metalakeName)
            .enableHiveSupport()
            .getOrCreate();
  }
}
