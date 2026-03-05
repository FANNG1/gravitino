package com.pinterest.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.job.JobTemplate;

public class CompactionSpinnerJobBuilder implements SpinnerJobBuilder {

  private static final String TABLE_CONFIG_NAME = "table";
  private static final String PARTITIONS_CONFIG_NAME = "partitions";
  private static final String JOBS_CONFIG_NAME = "jobs";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String DATASET_NAME_SPINNER_CONFIG_NAME = "dataset_name";
  private static final String QUERY_SPINNER_CONFIG_NAME = "query";

  @Override
  public String getJobTemplateName() {
    return "pinterest-compaction";
  }

  @Override
  public String getDagId() {
    return "iceberg_table_optimization_actor";
  }

  @Override
  public Map<String, Object> getJobConfig(JobTemplate jobTemplate) {
    Map<String, String> configs = jobTemplate.customFields();

    if (configs.containsKey(JOBS_CONFIG_NAME)) {
      return buildJobConfigFromBatchJobs(configs);
    } else {
      return buildJobConfigFromSingleJob(configs);
    }
  }

  private Map<String, Object> buildJobConfigFromBatchJobs(Map<String, String> configs) {
    String jobsJson = configs.get(JOBS_CONFIG_NAME);
    if (jobsJson == null || jobsJson.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Missing or empty required config field: %s", JOBS_CONFIG_NAME));
    }

    List<Map<String, String>> inputJobs;
    try {
      inputJobs = OBJECT_MAPPER.readValue(jobsJson, new TypeReference<>() {});
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          String.format("Failed to parse jobs JSON: %s", jobsJson), e);
    }

    if (inputJobs == null || inputJobs.isEmpty()) {
      throw new IllegalArgumentException("Jobs array cannot be empty");
    }

    List<Map<String, String>> outputJobs = new ArrayList<>();
    for (Map<String, String> inputJob : inputJobs) {
      String datasetName = inputJob.get(TABLE_CONFIG_NAME);
      if (datasetName == null || datasetName.isEmpty()) {
        throw new IllegalArgumentException(
            String.format("Missing required field '%s' in job config", TABLE_CONFIG_NAME));
      }

      String whereClause = inputJob.get(PARTITIONS_CONFIG_NAME);
      Map<String, String> outputJob = buildJobEntry(datasetName, whereClause);
      outputJobs.add(outputJob);
    }

    return Map.of(JOBS_CONFIG_NAME, outputJobs);
  }

  private Map<String, Object> buildJobConfigFromSingleJob(Map<String, String> configs) {
    String datasetName = configs.get(TABLE_CONFIG_NAME);
    if (datasetName == null || datasetName.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Missing required config field: %s", TABLE_CONFIG_NAME));
    }

    String whereClause = configs.get(PARTITIONS_CONFIG_NAME);
    Map<String, String> jobEntry = buildJobEntry(datasetName, whereClause);

    return Map.of(JOBS_CONFIG_NAME, List.of(jobEntry));
  }

  private static Map<String, String> buildJobEntry(String datasetName, String whereClause) {
    String query;
    if (whereClause == null || whereClause.isEmpty()) {
      query = String.format("CALL system.rewrite_data_files(table => '%s')", datasetName);
    } else {
      query =
          String.format(
              "CALL system.rewrite_data_files(table => '%s', where => \"%s\")",
              datasetName, whereClause);
    }

    Map<String, String> jobEntry = new LinkedHashMap<>();
    jobEntry.put(DATASET_NAME_SPINNER_CONFIG_NAME, datasetName);
    jobEntry.put(QUERY_SPINNER_CONFIG_NAME, query);
    return jobEntry;
  }
}
