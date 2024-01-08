/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.spark;

import java.util.Set;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@Tag("gravitino-docker-it")
@TestInstance(Lifecycle.PER_CLASS)
public class SparkIT extends SparkBaseIT {

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
  public void testSchema() {
    String testDatabaseName = "t1";
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

  public void testCreateDatasourceTable() {
    sql(
        "CREATE TABLE student_parquet(id INT, name STRING, age INT) USING PARQUET"
            + " OPTIONS ('parquet.bloom.filter.enabled'='true', "
            + "'parquet.bloom.filter.enabled#age'='false');");
  }

  public void testCreateHiveTable() {
    sql("use default");
    sql("drop table if exists student");
    sql("drop table if exists student1");
    sql(
        "CREATE TABLE default.student (id INT, name STRING, age INT)\n"
            //     + "    USING CSV\n"
            + "    PARTITIONED BY (age)\n"
            + "    CLUSTERED BY (Id) SORTED BY (name) INTO 4 buckets ROW FORMAT DELIMITED FIELDS TERMINATED BY ','\n"
            + "    STORED AS TEXTFILE TBLPROPERTIES ('foo'='bar')\n"
            + "    LOCATION '/tmp/family/' \n"
            + "    COMMENT 'this is a comment';");
    sql("create table student1 as select * from default.student");
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
