/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.spark;

import static org.apache.hadoop.hive.serde.serdeConstants.DATE_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.INT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.TINYINT_TYPE_NAME;

import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfo;
import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfo.SparkColumnInfo;
import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfoChecker;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.platform.commons.util.StringUtils;

@Tag("gravitino-docker-it")
@TestInstance(Lifecycle.PER_CLASS)
public class SparkIT extends SparkEnvIT {

  private static final String SELECT_ALL_TEMPLATE = "SELECT * FROM %s";
  private static final String INSERT_WITHOUT_PARTITION_TEMPLATE = "INSERT INTO %s VALUES (%s)";
  private static final String INSERT_WITH_PARTITION_TEMPLATE =
      "INSERT INTO %s.%s PARTITION (%s) VALUES (%s)";

  private static final Map<String, String> typeConstant =
      ImmutableMap.of(
          TINYINT_TYPE_NAME,
          "1",
          INT_TYPE_NAME,
          "2",
          DATE_TYPE_NAME,
          "'2023-01-01'",
          STRING_TYPE_NAME,
          "'gravitino_it_test'");

  @BeforeEach
  void init() {
    sql("use " + hiveCatalogName);
  }

  @Test
  public void testLoadCatalogs() {
    Set<String> catalogs = getCatalogs();
    Assertions.assertTrue(catalogs.contains(hiveCatalogName));
  }

  @Test
  public void testCreateAndLoadSchema() {
    String testDatabaseName = "t_create1";
    dropDatabase(testDatabaseName);
    sql("create database " + testDatabaseName);
    Map<String, String> databaseMeta = getDatabaseMetadata(testDatabaseName);
    Assertions.assertFalse(databaseMeta.containsKey("Comment"));
    Assertions.assertTrue(databaseMeta.containsKey("Location"));
    Assertions.assertEquals("datastrato", databaseMeta.get("Owner"));
    String properties = databaseMeta.get("Properties");
    Assertions.assertTrue(StringUtils.isBlank(properties));

    testDatabaseName = "t_create2";
    dropDatabase(testDatabaseName);
    sql(
        String.format(
            "CREATE DATABASE %s COMMENT 'comment' LOCATION '/user'\n"
                + " WITH DBPROPERTIES (ID=001);",
            testDatabaseName));
    databaseMeta = getDatabaseMetadata(testDatabaseName);
    String comment = databaseMeta.get("Comment");
    Assertions.assertEquals("comment", comment);
    Assertions.assertEquals("datastrato", databaseMeta.get("Owner"));
    properties = databaseMeta.get("Properties");
    Assertions.assertEquals("((ID,001))", properties);
  }

  @Test
  public void testDropSchema() {
    String testDatabaseName = "t_drop";
    Set<String> databases = getDatabases();
    Assertions.assertFalse(databases.contains(testDatabaseName));

    sql("create database " + testDatabaseName);
    databases = getDatabases();
    Assertions.assertTrue(databases.contains(testDatabaseName));

    sql("drop database " + testDatabaseName);
    databases = getDatabases();
    Assertions.assertFalse(databases.contains(testDatabaseName));

    Assertions.assertThrowsExactly(
        NoSuchNamespaceException.class, () -> sql("drop database notExists"));
  }

  @Test
  void testSimpleTable() {
    String tableName = "simpleTable";
    dropTable(tableName);
    String sql = String.format("CREATE TABLE %s (id INT, name STRING, age INT)", tableName);
    sql(sql);
    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(
                Arrays.asList(
                    SparkColumnInfo.of("id", "int", null),
                    SparkColumnInfo.of("name", "string", null),
                    SparkColumnInfo.of("age", "int", null)))
            // .withProvider("hive")
            .withType("MANAGED")
            .withProperties(
                ImmutableMap.of(
                    "table-type", "MANAGED_TABLE",
                    "input-format", "org.apache.hadoop.mapred.TextInputFormat",
                    "output-format", "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "serde-lib", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"));
    checker.check(tableInfo);

    checkTableReadWrite(tableInfo);
  }

  @Test
  public void testDropTable() {
    sql("drop table notExists");

    String tableName = "dropTable";
    String createTableSQL = getCreateSimpleTableString(tableName);

    sql(createTableSQL);
    SparkTableInfo tableInfo = getTableInfo(tableName);

    dropTable(tableName);
    tableInfo = getTableInfo(tableName);
  }

  @Test
  public void testRenameTable() {
    String tableName = "rename1";
    String createTableSQL = getCreateSimpleTableString(tableName);
    sql("RENAME rename1 to rename2");
  }

  // SparkSQL doesn't support not null, create a complex example
  @Test
  public void testCreateDatasourceTable() {
    String tableName = "datasourceTextTable";
    String location = "hdfs://127.0.0.1:9000/tmp/" + tableName;
    dropTable(tableName);
    sql(
        String.format(
            "CREATE TABLE %s (id INT COMMENT 'id comment', name STRING, age INT)"
                + " OPTIONS ('parquet.bloom.filter.enabled'='true')"
                + " TBLPROPERTIES ('foo'='bar') LOCATION '%s' COMMENT 'hi';",
            tableName, location));
    SparkTableInfo tableInfo = getTableInfo(tableName);
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(
                Arrays.asList(
                    SparkColumnInfo.of("id", "int", "id comment"),
                    SparkColumnInfo.of("name", "string", null),
                    SparkColumnInfo.of("age", "int", null)))
            .withProperties(
                ImmutableMap.of(
                    "foo", "bar",
                    "table-type", "MANAGED_TABLE",
                    "input-format", "org.apache.hadoop.mapred.TextInputFormat",
                    "output-format", "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "serde-lib", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                    "parquet.bloom.filter.enabled", "true"))
            .withType("MANAGED")
            // provider hive?
            .withProvider(null)
            .withLocation(location)
            .withComment("hi");
    checker.check(tableInfo);
    checkTableReadWrite(tableInfo);
  }

  // using PARQUET
  @Test
  public void testCreateDatasourceParquetTable() {
    String tableName = "datasourceParquetTable";
    dropTable(tableName);
    sql(
        String.format(
            "CREATE TABLE %s (id INT COMMENT 'id comment', name STRING, age INT)"
                + " USING parquet;",
            tableName));
    SparkTableInfo tableInfo = getTableInfo(tableName);
    System.out.println(tableInfo);
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(
                Arrays.asList(
                    SparkColumnInfo.of("id", "int", "id comment"),
                    SparkColumnInfo.of("name", "string", null),
                    SparkColumnInfo.of("age", "int", null)))
            .withProperties(
                ImmutableMap.of(
                    "table-type", "MANAGED_TABLE",
                    "input-format", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "output-format",
                        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "serde-lib", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"))
            .withProvider("parquet");

    checker.check(tableInfo);
  }

  // STORED AS PARQUET
  @Test
  public void testCreateHiveParquetTable() {
    String tableName = "create_hive_parquet_table";

    dropTable(tableName);
    sql(
        "CREATE TABLE "
            + tableName
            + "(id INT COMMENT 'id comment', name STRING, age INT)\n"
            + "    STORED AS PARQUET \n"
            + "    OPTIONS ('parquet.bloom.filter.enabled'='true')");
    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(
                Arrays.asList(
                    SparkColumnInfo.of("id", "int", "id comment"),
                    SparkColumnInfo.of("name", "string", null),
                    SparkColumnInfo.of("age", "int", null)))
            .withProperties(
                ImmutableMap.of(
                    "table-type", "MANAGED_TABLE",
                    "input-format", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "output-format",
                        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "serde-lib", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    "parquet.bloom.filter.enabled", "true"))
            .withProvider("hive");
    System.out.println(tableInfo);
    checker.check(tableInfo);
    checkTableReadWrite(tableInfo);
  }

  @Test
  public void testCreateHiveExternalTable() {
    String tableName = "external_" + System.currentTimeMillis();
    String location = "hdfs://127.0.0.1:9000/tmp/" + tableName;

    dropTable(tableName);
    String sql =
        String.format(
            "CREATE EXTERNAL TABLE %s (id INT COMMENT 'id comment', name STRING, age INT) STORED AS PARQUET LOCATION '%s';",
            tableName, location);
    sql(sql);

    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(
                Arrays.asList(
                    SparkColumnInfo.of("id", "int", "id comment"),
                    SparkColumnInfo.of("name", "string", null),
                    SparkColumnInfo.of("age", "int", null)))
            .withProperties(
                ImmutableMap.of(
                    "table-type", "EXTERNAL_TABLE",
                    "input-format", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "output-format",
                        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "serde-lib", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"))
            .withProvider("hive")
            .withType("EXTERNAL")
            .withLocation(location);
    checker.check(tableInfo);
    checkTableReadWrite(tableInfo);
  }

  @Test
  public void testCreateHiveTableWithFormat() {
    String tableName = "default.avroExample";
    dropTable(tableName);
    String sql =
        "CREATE TABLE avroExample\n"
            + "    (id INT COMMENT 'id comment', name STRING, age INT)\n"
            + "    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'\n"
            + "    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'\n"
            + "        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat';";
    sql(sql);
    SparkTableInfo tableInfo = getTableInfo(tableName);
    System.out.println(tableInfo);
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withProperties(
                ImmutableMap.of(
                    "input-format", "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
                    "output-format", "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat",
                    "serde-lib", "org.apache.hadoop.hive.serde2.avro.AvroSerDe"))
            .withProvider(null);
    checker.check(tableInfo);
    checkTableReadWrite(tableInfo);
  }

  @Test
  public void testCreateHiveTextTable() {
    String tableName = "hiveTextTable";

    dropTable(tableName);
    String sql =
        "CREATE TABLE "
            + tableName
            + "(id INT COMMENT 'id comment', name STRING, age INT)\n"
            + "    PARTITIONED BY (age)\n"
            + "    CLUSTERED BY (Id) SORTED BY (name) INTO 4 buckets \n"
            + "    ROW FORMAT DELIMITED FIELDS TERMINATED BY '@' COLLECTION ITEMS TERMINATED BY ':' MAP KEYS TERMINATED BY ':' LINES TERMINATED BY '\\n' \n"
            + "    STORED AS TEXTFILE TBLPROPERTIES ('foo'='bar')\n"
            + "    LOCATION 'hdfs://127.0.0.1:9000/tmp/abc' \n"
            + "    COMMENT 'hi';";
    sql(sql);
    SparkTableInfo tableInfo = getTableInfo(tableName);
    System.out.println(tableInfo);
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(
                Arrays.asList(
                    SparkColumnInfo.of("id", "int", "id comment"),
                    SparkColumnInfo.of("name", "string", null),
                    SparkColumnInfo.of("age", "int", null)))
            .withProperties(
                ImmutableMap.of(
                    "foo", "bar",
                    "table-type", "MANAGED_TABLE",
                    "input-format", "org.apache.hadoop.mapred.TextInputFormat",
                    "output-format", "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "serde-lib", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                    //  "option.line.delim", ":",
                    "option.mapkey.delim", ":",
                    "option.colelction.delim", ":"))
            .withProvider("hive")
            .withComment("hi");
    checker.check(tableInfo);
    System.out.println(tableInfo);
    checkTableReadWrite(tableInfo);

    // sql("create table student1 as select * from default.student");
  }

  @Test
  public void createDropTable() {}

  private void checkTableReadWrite(SparkTableInfo table) {
    String name = table.getName();
    String insertValues =
        table.getColumns().stream()
            .map(columnInfo -> typeConstant.get(columnInfo.getType()))
            .map(Object::toString)
            .collect(Collectors.joining(","));

    sql(String.format(INSERT_WITHOUT_PARTITION_TEMPLATE, name, insertValues));

    // remove "'" from values, such as 'a' is trans to a
    String checkValues =
        table.getColumns().stream()
            .map(columnInfo -> typeConstant.get(columnInfo.getType()))
            .map(Object::toString)
            .map(
                s -> {
                  String tmp = org.apache.commons.lang3.StringUtils.removeEnd(s, "'");
                  tmp = org.apache.commons.lang3.StringUtils.removeStart(tmp, "'");
                  return tmp;
                })
            .collect(Collectors.joining(","));

    List<String> queryResult =
        sql(String.format(SELECT_ALL_TEMPLATE, name)).stream()
            .map(
                line ->
                    Arrays.stream(line)
                        .map(item -> item.toString())
                        .collect(Collectors.joining(",")))
            .collect(Collectors.toList());
    Assertions.assertTrue(
        queryResult.size() == 1, "should just one row, table content: " + queryResult);
    Assertions.assertEquals(checkValues, queryResult.get(0));
  }

  private String getCreateSimpleTableString(String tableName) {
    return String.format(
        "CREATE TABLE %s (id INT COMMENT 'id comment', name STRING, age INT)", tableName);
  }

  private List<SparkColumnInfo> getSimpleTableColumn() {
    return Arrays.asList(
        SparkColumnInfo.of("id", "int", "id comment"),
        SparkColumnInfo.of("name", "string", null),
        SparkColumnInfo.of("age", "int", null));
  }

  public void testFunction() {
    sql("select current_date(), unix_timestamp();");
  }

  public void testView() {
    sql("create database if not exists v");
    sql("use f");
    // sql("create GLOBAL TEMPORARY VIEW IF NOT EXISTS view_student as select * from
    // student limit 2;");
    // sql("create view view_student1 as select * from student limit 2;");
    // sql("select * from view_student1").show();
  }

  public void testFederatinoQuery() {
    sql("use hive");
    sql("create database if not exists f");
    sql("drop table if exists f.student");
    sql("CREATE TABLE f.student (id INT, name STRING, age INT)");
    sql("INSERT into f.student VALUES(0, 'aa', 10), (1,'bb', 12);");
    sql("desc table EXTENDED f.student");

    sql("create database if not exists hive1.f1");
    sql("drop table if exists hive1.f1.scores");
    sql("CREATE TABLE hive1.f1.scores (id INT, score INT)");
    sql("desc table EXTENDED hive1.f1.scores");
    sql("INSERT into hive1.f1.scores VALUES(0, 100), (1, 98)");
    sql(
        "select f.student.id, name, age, score from hive.f.student JOIN hive1.f1.scores ON f.student.id = hive1.f1.scores.id");
  }

  public void testTestCreateDatabase() {
    sql("create database if not exists db_create2");
    sql("show databases");
  }

  public void testHiveDML() {
    sql("create database if not exists db");
    sql("drop table if exists db.student");
    sql("use db");
    sql("CREATE TABLE student (id INT, name STRING, age INT)");
    sql("desc db.student");
    sql("INSERT into db.student VALUES(0, 'aa', 10), (1,'bb', 12);");
    sql("drop table if exists db.student1");
    sql("create table db.student1 as select * from db.student limit 1");
    sql("INSERT into db.student1 select * from db.student limit 1");
    sql("select * from db.student1;");
  }

  /*
  @Test
  public void testSpark() {
    sql(
        "CREATE TABLE if NOT EXISTS sales ( id INT, name STRING, age INT ) PARTITIONED BY (country STRING, state STRING)");
    sql("desc table extended sales").show();
    sql(
        "INSERT INTO sales PARTITION (country='USA', state='CA') VALUES (1, 'John', 25);");
    sparkSession
        .sql("INSERT INTO sales PARTITION (country='Canada', state='ON') VALUES (2, 'Alice', 30);")
        .explain("extended");
    // sql("select * from sales where country = 'USA'").explain("extended");

    // insert into select xx
    sql(
        "CREATE TABLE IF NOT EXISTS target_table (id INT, name STRING, age INT) PARTITIONED BY (p INT)");
    sparkSession
        .sql(
            "INSERT INTO target_table PARTITION ( p = 1 ) SELECT id, name, age FROM sales WHERE country='USA' AND state='CA'")
        .explain("extended");
    sql("select * from target_table").show();

    // create table as select
    // sql("CREATE TABLE IF NOT EXISTS target_table2 as select * from
    // sales").explain("formatted");
    sparkSession
        .sql("CREATE TABLE IF NOT EXISTS target_table2 as select * from sales")
        .explain("extended");
  }
   */
}
