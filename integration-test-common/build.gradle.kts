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
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  id("java")
  alias(libs.plugins.shadow)
}

repositories {
  mavenCentral()
}

dependencies {
  testImplementation(project(":api"))
  testImplementation(project(":clients:client-java"))
  testImplementation(project(":common"))
  testImplementation(project(":core")) {
    exclude(group = "org.rocksdb", module = "rocksdbjni")
  }
  testImplementation(project(":server")) {
    exclude(group = "org.rocksdb", module = "rocksdbjni")
  }
  testImplementation(project(":server-common")) {
    exclude(group = "org.rocksdb", module = "rocksdbjni")
  }
  testImplementation(project(":authorizations:authorization-ranger"))
  testImplementation(libs.bundles.jetty)
  testImplementation(libs.bundles.jersey)
  testImplementation(libs.bundles.jwt)
  testImplementation(libs.bundles.log4j)
  testImplementation(libs.commons.cli)
  testImplementation(libs.commons.lang3)
  testImplementation(libs.commons.io)
  testImplementation(libs.guava)
  testImplementation(libs.httpclient5)
  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainers.mysql)
  testImplementation(libs.testcontainers.postgresql)
  testImplementation(libs.ranger.intg) {
    exclude("org.apache.hadoop", "hadoop-common")
    exclude("org.apache.hive", "hive-storage-api")
    exclude("org.apache.lucene")
    exclude("org.apache.solr")
    exclude("org.apache.kafka")
    exclude("org.elasticsearch")
    exclude("org.elasticsearch.client")
    exclude("org.elasticsearch.plugin")
    exclude("com.amazonaws", "aws-java-sdk-bundle")
  }
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.engine)
  testImplementation(libs.bundles.jersey)
}

val testShadowJar by tasks.registering(ShadowJar::class) {
  isZip64 = true
  configurations = listOf(
    project.configurations.runtimeClasspath.get(),
    project.configurations.testRuntimeClasspath.get()
  )
  archiveClassifier.set("tests-shadow")
  relocate("org.eclipse.jetty", "org.apache.gravitino.it.shaded.org.eclipse.jetty")
  from(sourceSets["test"].output)
}

tasks.jar {
  dependsOn(testShadowJar)
}

tasks.test {
  useJUnitPlatform()
}

configurations {
  create("testArtifacts")
}

artifacts {
  add("testArtifacts", testShadowJar)
}
