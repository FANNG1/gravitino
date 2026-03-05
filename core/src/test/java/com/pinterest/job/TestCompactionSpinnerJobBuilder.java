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

package com.pinterest.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.job.JobTemplate;
import org.apache.gravitino.job.ShellJobTemplate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCompactionSpinnerJobBuilder {

  private final CompactionSpinnerJobBuilder builder = new CompactionSpinnerJobBuilder();
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  void testGetJobTemplateName() {
    String templateName = builder.getJobTemplateName();
    Assertions.assertEquals("pinterest-compaction", templateName);
  }

  @Test
  void testGetDagId() {
    String dagId = builder.getDagId();
    Assertions.assertEquals("iceberg_table_optimization_actor", dagId);
  }

  @Test
  void testGetJobConfigWithSingleJob() {
    Map<String, String> customFields =
        ImmutableMap.of(
            "table", "default.my_table",
            "partitions", "(dt = \"2026-01-01\")");
    JobTemplate jobTemplate = createJobTemplate(customFields);

    Map<String, Object> jobConfig = builder.getJobConfig(jobTemplate);

    Assertions.assertNotNull(jobConfig);
    Assertions.assertEquals(1, jobConfig.size());
    Assertions.assertTrue(jobConfig.containsKey("jobs"));

    @SuppressWarnings("unchecked")
    List<Map<String, String>> jobs = (List<Map<String, String>>) jobConfig.get("jobs");

    Assertions.assertEquals(1, jobs.size());
    Map<String, String> job = jobs.get(0);
    Assertions.assertEquals("default.my_table", job.get("dataset_name"));
    Assertions.assertEquals(
        "CALL system.rewrite_data_files(table => 'default.my_table', where => \"(dt = \"2026-01-01\")\")",
        job.get("query"));
  }

  @Test
  void testGetJobConfigWithMissingTableField() {
    Map<String, String> customFields = ImmutableMap.of("partitions", "(dt = \"2026-01-01\")");
    JobTemplate jobTemplate = createJobTemplate(customFields);

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> builder.getJobConfig(jobTemplate));

    Assertions.assertTrue(exception.getMessage().contains("Missing required config field: table"));
  }

  @Test
  void testGetJobConfigWithEmptyTableField() {
    Map<String, String> customFields =
        ImmutableMap.of("table", "", "partitions", "(dt = \"2026-01-01\")");
    JobTemplate jobTemplate = createJobTemplate(customFields);

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> builder.getJobConfig(jobTemplate));

    Assertions.assertTrue(exception.getMessage().contains("Missing required config field: table"));
  }

  @Test
  void testGetJobConfigWithNoPartitionsPredicate() {
    Map<String, String> customFields = ImmutableMap.of("table", "default.my_table");
    JobTemplate jobTemplate = createJobTemplate(customFields);

    Map<String, Object> jobConfig = builder.getJobConfig(jobTemplate);

    Assertions.assertNotNull(jobConfig);
    Assertions.assertEquals(1, jobConfig.size());
    Assertions.assertTrue(jobConfig.containsKey("jobs"));

    @SuppressWarnings("unchecked")
    List<Map<String, String>> jobs = (List<Map<String, String>>) jobConfig.get("jobs");

    Assertions.assertEquals(1, jobs.size());
    Map<String, String> job = jobs.get(0);
    Assertions.assertEquals("default.my_table", job.get("dataset_name"));
    Assertions.assertEquals(
        "CALL system.rewrite_data_files(table => 'default.my_table')", job.get("query"));
  }

  @Test
  void testGetJobConfigWithBatchJobs() throws Exception {
    List<Map<String, String>> inputJobs = new ArrayList<>();

    Map<String, String> job1 = new LinkedHashMap<>();
    job1.put("table", "db1.table1");
    inputJobs.add(job1);

    Map<String, String> job2 = new LinkedHashMap<>();
    job2.put("table", "db2.table2");
    job2.put("partitions", "(dt='2026-01-02')");
    inputJobs.add(job2);

    Map<String, String> job3 = new LinkedHashMap<>();
    job3.put("table", "db3.table3");
    job3.put("partitions", "(dt='2026-01-03' AND hr='10') OR (dt='2026-01-04' AND hr='11')");
    inputJobs.add(job3);

    String jobsJson = objectMapper.writeValueAsString(inputJobs);
    Map<String, String> customFields = ImmutableMap.of("jobs", jobsJson);

    JobTemplate jobTemplate = createJobTemplate(customFields);
    Map<String, Object> jobConfig = builder.getJobConfig(jobTemplate);

    Assertions.assertNotNull(jobConfig);
    Assertions.assertTrue(jobConfig.containsKey("jobs"));

    @SuppressWarnings("unchecked")
    List<Map<String, String>> outputJobs = (List<Map<String, String>>) jobConfig.get("jobs");

    Assertions.assertEquals(3, outputJobs.size());

    Map<String, String> outputJob0 = outputJobs.get(0);
    Assertions.assertEquals("db1.table1", outputJob0.get("dataset_name"));
    Assertions.assertEquals(
        "CALL system.rewrite_data_files(table => 'db1.table1')", outputJob0.get("query"));

    Map<String, String> outputJob1 = outputJobs.get(1);
    Assertions.assertEquals("db2.table2", outputJob1.get("dataset_name"));
    Assertions.assertEquals(
        "CALL system.rewrite_data_files(table => 'db2.table2', where => \"(dt='2026-01-02')\")",
        outputJob1.get("query"));

    Map<String, String> outputJob2 = outputJobs.get(2);
    Assertions.assertEquals("db3.table3", outputJob2.get("dataset_name"));
    Assertions.assertEquals(
        "CALL system.rewrite_data_files(table => 'db3.table3', where => \"(dt='2026-01-03' AND hr='10') OR (dt='2026-01-04' AND hr='11')\")",
        outputJob2.get("query"));
  }

  @Test
  void testGetJobConfigWithInvalidJobsJson() {
    Map<String, String> customFields = ImmutableMap.of("jobs", "invalid-json");

    JobTemplate jobTemplate = createJobTemplate(customFields);

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> builder.getJobConfig(jobTemplate));

    Assertions.assertTrue(exception.getMessage().contains("Failed to parse jobs JSON"));
  }

  @Test
  void testGetJobConfigWithEmptyJobsArray() {
    Map<String, String> customFields = ImmutableMap.of("jobs", "[]");

    JobTemplate jobTemplate = createJobTemplate(customFields);

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> builder.getJobConfig(jobTemplate));

    Assertions.assertTrue(exception.getMessage().contains("Jobs array cannot be empty"));
  }

  @Test
  void testGetJobConfigWithEmptyJobsString() {
    Map<String, String> customFields = ImmutableMap.of("jobs", "");

    JobTemplate jobTemplate = createJobTemplate(customFields);

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> builder.getJobConfig(jobTemplate));

    Assertions.assertTrue(
        exception.getMessage().contains("Missing or empty required config field"));
  }

  @Test
  void testGetJobConfigWithMissingTableInJobsArray() throws Exception {
    List<Map<String, String>> inputJobs = new ArrayList<>();
    Map<String, String> job1 = new LinkedHashMap<>();
    job1.put("table", "db1.table1");
    job1.put("partitions", "(dt='2026-01-01')");
    inputJobs.add(job1);

    Map<String, String> job2 = new LinkedHashMap<>();
    job2.put("partitions", "(dt='2026-01-02')");
    inputJobs.add(job2);

    String jobsJson = objectMapper.writeValueAsString(inputJobs);
    Map<String, String> customFields = ImmutableMap.of("jobs", jobsJson);

    JobTemplate jobTemplate = createJobTemplate(customFields);

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> builder.getJobConfig(jobTemplate));

    Assertions.assertTrue(exception.getMessage().contains("Missing required field 'table'"));
  }

  @Test
  void testGetJobConfigWithEmptyTableInJobsArray() throws Exception {
    List<Map<String, String>> inputJobs = new ArrayList<>();
    Map<String, String> job1 = new LinkedHashMap<>();
    job1.put("table", "db1.table1");
    job1.put("partitions", "(dt='2026-01-01')");
    inputJobs.add(job1);

    Map<String, String> job2 = new LinkedHashMap<>();
    job2.put("table", "");
    job2.put("partitions", "(dt='2026-01-02')");
    inputJobs.add(job2);

    String jobsJson = objectMapper.writeValueAsString(inputJobs);
    Map<String, String> customFields = ImmutableMap.of("jobs", jobsJson);

    JobTemplate jobTemplate = createJobTemplate(customFields);

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> builder.getJobConfig(jobTemplate));

    Assertions.assertTrue(exception.getMessage().contains("Missing required field 'table'"));
  }

  private JobTemplate createJobTemplate(Map<String, String> customFields) {
    return ShellJobTemplate.builder()
        .withName("pinterest-compaction")
        .withCustomFields(customFields)
        .withExecutable("mock-executable")
        .build();
  }
}
