/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.util.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;
import org.junit.jupiter.api.Assertions;

/*
+----------------------------+-------------+-------+
    |col_name                    |data_type    |comment|
    +----------------------------+-------------+-------+
    |id                          |int          |null   |
    |name                        |string       |null   |
    |age                         |int          |null   |
    |                            |             |       |
    |# Detailed Table Information|             |       |
    |Name                        |databaseTable|       |
    |Type                        |MANAGED      |       |
    |Table Properties            |[]           |       |
    +----------------------------+-------------+-------+

    id,int,null
    name,string,null
    age,int,null
    ,,
    # Detailed Table Information,,
    Name,databaseTable,
    Type,MANAGED,
    Table Properties,[],
 */

@Data
public class SparkTableInfo {
  private static final String NULL_COMMENT_STRING = "";
  String name;
  List<SparkColumnInfo> columns;
  String type;
  Map<String, String> tableProperties;
  String comment;
  String location;
  String provider;
  String owner;

  List<String> unknownItems = new ArrayList<>();

  public SparkTableInfo() {}

  public String getName() {
    return name;
  }

  public List<SparkColumnInfo> getColumns() {
    return columns;
  }

  @Data
  public static class SparkColumnInfo {
    String name;
    String type;
    String comment;

    private SparkColumnInfo(String name, String type, String comment) {
      this.name = name;
      this.type = type;
      this.comment = comment;
    }

    public static SparkColumnInfo of(String name, String type) {
      return of(name, type, NULL_COMMENT_STRING);
    }

    public static SparkColumnInfo of(String name, String type, String comment) {
      return new SparkColumnInfo(name, type, comment);
    }
  }

  public enum Stage {
    COLLUMN,
    TABLE_INFO,
  }

  public static SparkTableInfo getSparkTableInfo(List<Object[]> rows) {
    Stage stage = Stage.COLLUMN;
    SparkTableInfo tableInfo = new SparkTableInfo();
    for (Object[] os : rows) {
      String[] items =
          Arrays.stream(os)
              .map(
                  o -> {
                    if (o == null) {
                      return null;
                    } else {
                      return o.toString();
                    }
                  })
              .toArray(String[]::new);
      Assertions.assertTrue(items.length == 3);
      if (stage.equals(Stage.COLLUMN)) {
        if (items[0].isEmpty() == false) {
          tableInfo.addColumn(SparkColumnInfo.of(items[0], items[1], items[2]));
        } else {
          stage = Stage.TABLE_INFO;
        }
      } else if (stage.equals(Stage.TABLE_INFO)) {
        if (items[0].startsWith("# Detailed Table Information")) {
          continue;
        }
        switch (items[0]) {
          case "Name":
            tableInfo.name = items[1];
            break;
          case "Type":
            tableInfo.type = items[1];
            break;
          case "Table Properties":
            tableInfo.tableProperties = parseTableProperty(items[1]);
            break;
          case "Comment":
            tableInfo.comment = items[1];
            break;
          case "Provider":
            tableInfo.provider = items[1];
            break;
          case "Location":
            tableInfo.location = items[1];
            break;
          case "Owner":
            tableInfo.owner = items[1];
            break;
          default:
            tableInfo.addUnknownItem(String.join("@", items));
        }
      }
    }
    return tableInfo;
  }

  private static Map<String, String> parseTableProperty(String str) {
    Map<String, String> resultMap = new HashMap<>();

    Assertions.assertEquals('[', str.charAt(0));
    Assertions.assertEquals(']', str.charAt(str.length() - 1));
    String trimmedStr = str.substring(1, str.length() - 1).trim();

    String[] pairs = trimmedStr.split("\\s*,\\s*");

    for (String pair : pairs) {
      String[] keyValue = pair.split("=", 2);
      if (keyValue.length == 2) {
        resultMap.put(keyValue[0].trim(), keyValue[1].trim());
      }
    }
    return resultMap;
  }

  private void addColumn(SparkColumnInfo column) {
    if (columns == null) {
      columns = new ArrayList<>();
    }
    columns.add(column);
  }

  private void addUnknownItem(String item) {
    unknownItems.add(item);
  }
}
