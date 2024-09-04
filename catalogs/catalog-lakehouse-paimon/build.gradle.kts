/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
description = "catalog-lakehouse-paimon"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

val scalaVersion: String = project.properties["scalaVersion"] as? String ?: extra["defaultScalaVersion"].toString()
val sparkVersion: String = libs.versions.spark34.get()
val sparkMajorVersion: String = sparkVersion.substringBeforeLast(".")
val paimonVersion: String = libs.versions.paimon.get()

dependencies {
  implementation(project(":api")) {
    exclude("*")
  }
  implementation(project(":common")) {
    exclude("*")
  }
  implementation(project(":core")) {
    exclude("*")
  }
  implementation(libs.bundles.paimon) {
    exclude("com.sun.jersey")
    exclude("javax.servlet")
    exclude("org.apache.hive")
    exclude("org.apache.hbase")
    exclude("it.unimi.dsi")
    exclude("org.mortbay.jetty")
    exclude("org.apache.parquet")
    exclude("org.mortbay.jetty:jetty")
    exclude("org.mortbay.jetty:jetty-util")
    exclude("org.mortbay.jetty:jetty-sslengine")
    exclude("org.eclipse.jetty.aggregate:jetty-all")
  }
  implementation(libs.bundles.log4j)
  implementation(libs.commons.lang3)
  implementation(libs.guava)
  implementation(libs.hadoop2.common) {
    exclude("com.github.spotbugs")
    exclude("com.sun.jersey")
    exclude("javax.servlet")
    exclude("org.mortbay.jetty")
  }

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(project(":clients:client-java"))
  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(project(":server"))
  testImplementation(project(":server-common")) {
    exclude("org.mortbay.jetty")
    exclude("com.sun.jersey.contribs")
  }
  testImplementation("org.apache.spark:spark-hive_$scalaVersion:$sparkVersion") {
    exclude("org.apache.hadoop")
    exclude("org.rocksdb")
  }
  testImplementation("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion") {
    exclude("org.apache.avro")
    exclude("org.apache.hadoop")
    exclude("org.apache.zookeeper")
    exclude("io.dropwizard.metrics")
    exclude("org.rocksdb")
  }
  testImplementation("org.apache.paimon:paimon-spark-$sparkMajorVersion:$paimonVersion") {
    exclude("org.apache.hadoop")
  }
  testImplementation(libs.slf4j.api)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.mysql.driver)
  testImplementation(libs.bundles.log4j)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.testcontainers)

  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks {
  val runtimeJars by registering(Copy::class) {
    from(configurations.runtimeClasspath)
    into("build/libs")
  }

  val copyCatalogLibs by registering(Copy::class) {
    dependsOn("jar", "runtimeJars")
    from("build/libs")
    into("$rootDir/distribution/package/catalogs/lakehouse-paimon/libs")
  }

  val copyCatalogConfig by registering(Copy::class) {
    from("src/main/resources")
    into("$rootDir/distribution/package/catalogs/lakehouse-paimon/conf")

    include("lakehouse-paimon.conf")
    include("core-site.xml.template")
    include("hdfs-site.xml.template")

    rename { original ->
      if (original.endsWith(".template")) {
        original.replace(".template", "")
      } else {
        original
      }
    }

    exclude { details ->
      details.file.isDirectory()
    }

    fileMode = 0b111101101
  }

  register("copyLibAndConfig", Copy::class) {
    dependsOn(copyCatalogLibs, copyCatalogConfig)
  }
}

tasks.test {
  val skipUTs = project.hasProperty("skipTests")
  if (skipUTs) {
    // Only run integration tests
    include("**/integration/**")
  }

  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    // Exclude integration tests
    exclude("**/integration/**")
  } else {
    dependsOn(tasks.jar)
  }
}

tasks.getByName("generateMetadataFileForMavenJavaPublication") {
  dependsOn("runtimeJars")
}
