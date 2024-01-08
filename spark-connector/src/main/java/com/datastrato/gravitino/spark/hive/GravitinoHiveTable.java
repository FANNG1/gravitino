/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.hive;

import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.spark.PropertiesConverter;
import com.datastrato.gravitino.spark.table.GravitinoBaseTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCatalog;

public class GravitinoHiveTable extends GravitinoBaseTable implements SupportsRead, SupportsWrite {
  public GravitinoHiveTable(
      Identifier identifier,
      Table gravitinoTable,
      TableCatalog sparkCatalog,
      PropertiesConverter propertiesConverter) {
    super(identifier, gravitinoTable, sparkCatalog, propertiesConverter);
  }
}
