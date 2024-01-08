/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.datastrato.gravitino.integration.test.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;

/**
 * Provide helper methods to get SparkSQL result, will be reused by SparkIT and IcebergRESTServiceIT
 *
 * <p>Referred from spark/v3.4/spark/src/test/java/org/apache/iceberg/spark/SparkTestBase.java
 */
public abstract class SparkUtilIT extends AbstractIT {

  protected abstract SparkSession getSparkSession();

  protected Set<String> getCatalogs() {
    return convertToStringSet(sql("show catalogs"), 0);
  }

  protected Set<String> getDatabases() {
    return convertToStringSet(sql("show databases"), 0);
  }

  protected List<String> getTableColumns(String tableName) {
    List<Object[]> objects = sql("desc table extended " + tableName);
    List<String> columns = new ArrayList<>();
    objects.stream()
        .anyMatch(
            row -> {
              String columName = (String) row[0];
              if (StringUtils.isNoneBlank(columName)) {
                columns.add(columName);
                return false;
              }
              return true;
            });
    return columns;
  }

  protected List<Object[]> sql(String query, Object... args) {
    List<Row> rows = getSparkSession().sql(String.format(query, args)).collectAsList();
    return rowsToJava(rows);
  }

  protected Map<String, String> getTableInfo(String tableName) {
    return convertToStringMap(sql("desc table extended " + tableName));
  }

  /** Check whether all child map content is in parent map */
  protected void checkMapContains(Map<String, String> child, Map<String, String> parent) {
    child.forEach(
        (k, v) -> {
          Assertions.assertTrue(parent.containsKey(k));
          Assertions.assertEquals(v, parent.get(k));
        });
  }

  /** Mainly used to debug */
  protected void printObjects(List<Object[]> objects) {
    objects.stream()
        .forEach(
            row -> {
              String oneRow =
                  Arrays.stream(row).map(o -> String.valueOf(o)).collect(Collectors.joining(","));
            });
  }

  private List<Object[]> rowsToJava(List<Row> rows) {
    return rows.stream().map(this::toJava).collect(Collectors.toList());
  }

  private Object[] toJava(Row row) {
    return IntStream.range(0, row.size())
        .mapToObj(
            pos -> {
              if (row.isNullAt(pos)) {
                return null;
              }
              Object value = row.get(pos);
              if (value instanceof Row) {
                return toJava((Row) value);
              } else if (value instanceof scala.collection.Seq) {
                return row.getList(pos);
              } else if (value instanceof scala.collection.Map) {
                return row.getJavaMap(pos);
              }
              return value;
            })
        .toArray(Object[]::new);
  }

  private Set<String> convertToStringSet(List<Object[]> objects, int index) {
    return objects.stream().map(row -> String.valueOf(row[index])).collect(Collectors.toSet());
  }

  private List<String> convertToStringList(List<Object[]> objects, int index) {
    return objects.stream().map(row -> String.valueOf(row[index])).collect(Collectors.toList());
  }

  private static Map<String, String> convertToStringMap(List<Object[]> objects) {
    return objects.stream()
        .collect(
            Collectors.toMap(
                row -> String.valueOf(row[0]),
                row -> String.valueOf(row[1]),
                (oldValue, newValue) -> oldValue));
  }
}
