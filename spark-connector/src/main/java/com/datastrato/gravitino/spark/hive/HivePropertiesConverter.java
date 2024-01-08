/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.hive;

import com.datastrato.gravitino.spark.PropertiesConverter;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class HivePropertiesConverter implements PropertiesConverter {

  static final Map<String, String> providerMap =
      ImmutableMap.of(
          "sequencefile", "SEQUENCEFILE",
          "rcfile", "RCFILE",
          "orc", "ORC",
          "parquet", "PARQUET",
          "textfile", "TEXTFILE",
          "avro", "AVRO");

  static final Map<String, String> propertyMap =
      ImmutableMap.of(
          "hive.output-format",
          "output-format",
          "hive.input-format",
          "input-format",
          "hive.serde",
          "serde-lib");

  @Override
  public Map<String, String> toGravitinoSchemaProperties(Map<String, String> properties) {
    return properties;
  }

  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, String> properties) {
    Map<String, String> gravitinoTableProperties = new HashMap<>(properties);
    String provider = properties.get("provider");
    if (provider == null) {
      provider = properties.get("hive.stored-as");
      if (provider != null) {
        gravitinoTableProperties.put("provider", "hive");
      }
    }

    if (provider != null) {
      String platform = providerMap.get(provider.toLowerCase(Locale.ROOT));
      if (platform != null) {
        gravitinoTableProperties.put("format", platform);
      }
    }

    propertyMap.forEach(
        (sparkProperty, gravitinoProperty) -> {
          if (gravitinoTableProperties.containsKey(sparkProperty)) {
            String value = gravitinoTableProperties.remove(sparkProperty);
            gravitinoTableProperties.put(gravitinoProperty, value);
          }
        });

    String external = properties.get("external");
    if ("true".equalsIgnoreCase(external)) {
      gravitinoTableProperties.put("table-type", "EXTERNAL_TABLE");
    }
    return gravitinoTableProperties;
  }

  @Override
  public Map<String, String> fromGravitinoSchemaProperties(Map<String, String> properties) {
    return properties;
  }

  @Override
  public Map<String, String> fromGravitinoTableProperties(Map<String, String> properties) {
    // hive table type could be: EXTERNAL_TABLE, MANAGED_TABLE, VIEW
    String tableType = properties.get("table-type");
    if ("EXTERNAL_TABLE".equalsIgnoreCase(tableType)) {
      // the table is trusted to external if properties contains external
      properties.put("external", "true");
    }
    return properties;
  }
}
