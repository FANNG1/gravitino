/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.hive;

import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.spark.GravitinoCatalogAdaptor;
import com.datastrato.gravitino.spark.GravitinoSparkConfig;
import com.datastrato.gravitino.spark.PropertiesConverter;
import com.datastrato.gravitino.spark.table.GravitinoBaseTable;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.kyuubi.spark.connector.hive.HiveTableCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** HiveAdaptor provides specific operations for Hive Catalog to adapt to GravitinoCatalog. */
public class HiveAdaptor implements GravitinoCatalogAdaptor {

  @Override
  public PropertiesConverter getPropertiesConverter() {
    return new HivePropertiesConverter();
  }

  @Override
  public GravitinoBaseTable createSparkTable(
      Identifier identifier,
      Table gravitinoTable,
      TableCatalog sparkCatalog,
      PropertiesConverter propertiesConverter) {
    return new GravitinoHiveTable(identifier, gravitinoTable, sparkCatalog, propertiesConverter);
  }

  @Override
  public TableCatalog createAndInitSparkCatalog(
      String name, CaseInsensitiveStringMap options, Map<String, String> catalogProperties) {
    Preconditions.checkArgument(
        catalogProperties != null, "Hive Catalog properties should not be null");
    String metastoreUri = catalogProperties.get(GravitinoSparkConfig.GRAVITINO_HIVE_METASTORE_URI);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metastoreUri),
        "Couldn't get "
            + GravitinoSparkConfig.GRAVITINO_HIVE_METASTORE_URI
            + " from hive catalog properties");

    TableCatalog hiveCatalog = new HiveTableCatalog();
    HashMap<String, String> all = new HashMap<>(options);
    all.put(GravitinoSparkConfig.SPARK_HIVE_METASTORE_URI, metastoreUri);
    hiveCatalog.initialize(name, new CaseInsensitiveStringMap(all));

    return hiveCatalog;
  }
}