/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.util.spark;

import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfo.SparkColumnInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;

public class SparkTableInfoChecker {
  SparkTableInfo expectedTableInfo = new SparkTableInfo();
  List<CheckField> checkFields = new ArrayList<>();

  private SparkTableInfoChecker() {}

  public static SparkTableInfoChecker create() {
    return new SparkTableInfoChecker();
  }

  public enum CheckField {
    NAME,
    COLUMN,
    PROPERTY,
    COMMENT,
    TYPE,
    PROVIDER,
    LOCATION
  }

  public SparkTableInfoChecker withName(String name) {
    this.expectedTableInfo.setName(name);
    this.checkFields.add(CheckField.NAME);
    return this;
  }

  public SparkTableInfoChecker withColumns(List<SparkColumnInfo> columns) {
    this.expectedTableInfo.setColumns(columns);
    this.checkFields.add(CheckField.COLUMN);
    return this;
  }

  public SparkTableInfoChecker withProperties(Map<String, String> tableProperties) {
    this.expectedTableInfo.setTableProperties(tableProperties);
    this.checkFields.add(CheckField.PROPERTY);
    return this;
  }

  public SparkTableInfoChecker withProvider(String provider) {
    this.expectedTableInfo.setProvider(provider);
    this.checkFields.add(CheckField.PROVIDER);
    return this;
  }

  public SparkTableInfoChecker withType(String type) {
    this.expectedTableInfo.setType(type);
    this.checkFields.add(CheckField.TYPE);
    return this;
  }

  public SparkTableInfoChecker withLocation(String location) {
    this.expectedTableInfo.setLocation(location);
    this.checkFields.add(CheckField.LOCATION);
    return this;
  }

  public SparkTableInfoChecker withComment(String comment) {
    this.expectedTableInfo.setComment(comment);
    this.checkFields.add(CheckField.COMMENT);
    return this;
  }

  public void check(SparkTableInfo realTableInfo) {
    checkFields.stream()
        .forEach(
            checkField -> {
              switch (checkField) {
                case NAME:
                  Assertions.assertEquals(expectedTableInfo.name, realTableInfo.name);
                  break;
                case COLUMN:
                  Assertions.assertEquals(expectedTableInfo.columns, realTableInfo.columns);
                  break;
                case PROPERTY:
                  expectedTableInfo.tableProperties.forEach(
                      (k, v) -> {
                        Assertions.assertTrue(
                            realTableInfo.tableProperties.containsKey(k),
                            k + " not in table properties");
                        Assertions.assertEquals(v, realTableInfo.tableProperties.get(k));
                      });
                  break;
                case COMMENT:
                  Assertions.assertEquals(expectedTableInfo.comment, realTableInfo.comment);
                  break;
                case TYPE:
                  Assertions.assertEquals(expectedTableInfo.type, realTableInfo.type);
                  break;
                case PROVIDER:
                  Assertions.assertEquals(expectedTableInfo.provider, realTableInfo.provider);
                  break;
                case LOCATION:
                  Assertions.assertEquals(expectedTableInfo.location, realTableInfo.location);
                  // the underlying catalog may rewrite the location, like rewrite `a` to `file://a`
                  // Assertions.assertTrue(
                  //    realTableInfo.location.contains(expectedTableInfo.location));
                  break;
                default:
                  Assertions.fail(checkField + " not checked");
                  break;
              }
            });
  }
}
