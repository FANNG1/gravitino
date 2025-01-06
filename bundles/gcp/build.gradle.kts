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
  `maven-publish`
  id("java")
  alias(libs.plugins.shadow)
}

dependencies {
  compileOnly(project(":api"))
  compileOnly(project(":catalogs:catalog-common"))
  compileOnly(project(":catalogs:catalog-hadoop"))
  compileOnly(project(":core"))
  compileOnly(project(":clients:client-java"))

  compileOnly(libs.hadoop3.client.api)
  compileOnly(libs.hadoop3.client.runtime)
  compileOnly(libs.hadoop3.gcs)
  compileOnly(libs.slf4j.api)

  implementation(project(":catalogs:catalog-common")) {
    exclude("*")
  }
  implementation(project(":catalogs:hadoop-common")) {
    exclude("*")
  }
  implementation(project(":clients:filesystem-hadoop3-common")) {
    exclude("*")
  }

  implementation(libs.commons.lang3)
  // runtime used
  implementation(libs.commons.logging)
  implementation(libs.google.auth.credentials)
  implementation(libs.google.auth.http)

  testImplementation(project(":api"))
  testImplementation(project(":core"))
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.withType(ShadowJar::class.java) {
  isZip64 = true
  configurations = listOf(project.configurations.runtimeClasspath.get())
  archiveClassifier.set("")

  // Relocate dependencies to avoid conflicts
  relocate("org.apache.httpcomponents", "org.apache.gravitino.gcp.shaded.org.apache.httpcomponents")
  relocate("org.apache.commons", "org.apache.gravitino.gcp.shaded.org.apache.commons")
  relocate("com.google.common", "org.apache.gravitino.gcp.shaded.com.google.common")
  relocate("com.fasterxml", "org.apache.gravitino.gcp.shaded.com.fasterxml")
  relocate("com.fasterxml.jackson", "org.apache.gravitino.gcp.shaded.com.fasterxml.jackson")
}

tasks.jar {
  dependsOn(tasks.named("shadowJar"))
  archiveClassifier.set("empty")
}

tasks.compileJava {
  dependsOn(":catalogs:catalog-hadoop:runtimeJars")
}
