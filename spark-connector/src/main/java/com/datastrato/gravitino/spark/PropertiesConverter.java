/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark;

import java.util.Map;

public interface PropertiesConverter {
  Map<String, String> toGravitinoSchemaProperties(Map<String, String> properties);

  Map<String, String> toGravitinoTableProperties(Map<String, String> properties);

  Map<String, String> fromGravitinoSchemaProperties(Map<String, String> properties);

  Map<String, String> fromGravitinoTableProperties(Map<String, String> properties);
}
