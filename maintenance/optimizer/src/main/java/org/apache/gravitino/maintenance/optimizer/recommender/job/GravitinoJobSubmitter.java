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

package org.apache.gravitino.maintenance.optimizer.recommender.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.pinterest.maintenance.optimizer.recommender.job.PinterestCompactionJobAdapter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobSubmitter;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.common.util.GravitinoClientUtils;
import org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction.CompactionStrategyHandler;

/** Submits optimizer jobs to Gravitino using job template adapters. */
public class GravitinoJobSubmitter implements JobSubmitter {

  public static final String NAME = "gravitino-job-submitter";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private GravitinoClient gravitinoClient;
  private OptimizerEnv optimizerEnv;
  private OptimizerConfig optimizerConfig;

  /**
   * Returns the provider name for configuration lookup.
   *
   * @return provider name
   */
  private final Map<String, Class<? extends GravitinoJobAdapter>> jobAdapters =
      ImmutableMap.of(
          CompactionStrategyHandler.NAME, GravitinoCompactionJobAdapter.class,
          PinterestCompactionJobAdapter.JOB_TEMPLATE_NAME, PinterestCompactionJobAdapter.class);

  @Override
  public String name() {
    return NAME;
  }

  /**
   * Initializes the submitter with a Gravitino client derived from the optimizer configuration.
   *
   * @param optimizerEnv optimizer environment
   */
  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    this.optimizerEnv = optimizerEnv;
    this.optimizerConfig = optimizerEnv.config();
  }

  /**
   * Submits a job through Gravitino using the resolved job adapter.
   *
   * @param jobTemplateName template name used to select an adapter
   * @param jobExecutionContext execution context for the job
   * @return submitted job identifier
   */
  @Override
  public String submitJob(String jobTemplateName, JobExecutionContext jobExecutionContext) {
    GravitinoJobAdapter jobAdapter = loadJobAdapter(jobTemplateName);
    return submitJobInternal(
        jobTemplateName, buildJobConfig(optimizerConfig, jobExecutionContext, jobAdapter));
  }

  @Override
  public boolean supportsBatchJob(String jobTemplateName) {
    return loadJobAdapter(jobTemplateName).supportsBatchJob();
  }

  @Override
  public String submitBatchJob(
      String jobTemplateName, List<JobExecutionContext> jobExecutionContexts) {
    GravitinoJobAdapter jobAdapter = loadJobAdapter(jobTemplateName);
    return submitJobInternal(
        jobTemplateName, buildJobConfig(optimizerConfig, jobExecutionContexts, jobAdapter));
  }

  /** Closes the underlying Gravitino client. */
  @Override
  public void close() throws Exception {
    if (gravitinoClient != null) {
      gravitinoClient.close();
    }
  }

  private String submitJobInternal(String jobTemplateName, Map<String, String> jobConfig) {
    ensureClientInitialized();
    return gravitinoClient.runJob(jobTemplateName, jobConfig).jobId();
  }

  private void ensureClientInitialized() {
    if (gravitinoClient == null) {
      if (optimizerEnv == null) {
        throw new IllegalStateException("Job submitter is not initialized");
      }
      this.gravitinoClient = GravitinoClientUtils.createClient(optimizerEnv);
    }
  }

  /**
   * Merge job configs with precedence: optimizer config < adapter config.
   *
   * <p>Typical use cases:
   *
   * <ul>
   *   <li>Optimizer config: shared engine/runtime defaults (for example, Spark settings).
   *   <li>Adapter config: adapter-specific parameters (for example, WHERE filters) required by the
   *       job template.
   * </ul>
   */
  @VisibleForTesting
  static Map<String, String> buildJobConfig(
      OptimizerConfig optimizerConfig,
      JobExecutionContext jobExecutionContext,
      GravitinoJobAdapter jobAdapter) {
    Map<String, String> submitterConfigs =
        optimizerConfig == null ? Map.of() : optimizerConfig.jobSubmitterConfigs();
    Map<String, String> adapterConfigs =
        jobAdapter == null ? Map.of() : jobAdapter.jobConfig(jobExecutionContext);

    Map<String, String> mergedConfigs = new LinkedHashMap<>();
    mergedConfigs.putAll(submitterConfigs);
    mergedConfigs.putAll(adapterConfigs);
    return mergedConfigs;
  }

  /**
   * Builds job configuration for batch job submission.
   *
   * <p>For batch jobs, this method merges shared configs from the optimizer with per-dataset
   * configs generated by the job adapter. Each dataset's config is collected into a list and
   * serialized as a JSON array under the "jobs" key.
   *
   * <p>Example output: { "jobs": "[{\"table\":\"db1.table1\",\"partitions\":\"(dt='2024-01-01')\"},
   * {\"table\":\"db2.table2\",\"partitions\":\"(dt='2024-01-02')\"}]" }
   *
   * @param optimizerConfig optimizer configuration with shared settings
   * @param jobExecutionContexts list of contexts for each dataset
   * @param jobAdapter adapter to generate per-dataset configs
   * @return merged configuration map with serialized jobs array
   */
  @VisibleForTesting
  static Map<String, String> buildJobConfig(
      OptimizerConfig optimizerConfig,
      List<JobExecutionContext> jobExecutionContexts,
      GravitinoJobAdapter jobAdapter) {
    Map<String, String> submitterConfigs =
        optimizerConfig == null ? Map.of() : optimizerConfig.jobSubmitterConfigs();

    Map<String, String> mergedConfigs = new LinkedHashMap<>();
    mergedConfigs.putAll(submitterConfigs);

    if (jobAdapter != null && jobExecutionContexts != null && !jobExecutionContexts.isEmpty()) {
      List<Map<String, String>> jobs = new ArrayList<>();
      for (JobExecutionContext context : jobExecutionContexts) {
        Map<String, String> datasetConfig = jobAdapter.jobConfig(context);
        jobs.add(datasetConfig);
      }

      try {
        mergedConfigs.put("jobs", OBJECT_MAPPER.writeValueAsString(jobs));
      } catch (JsonProcessingException e) {
        throw new RuntimeException("Failed to serialize job configs to JSON", e);
      }
    }

    return mergedConfigs;
  }

  @VisibleForTesting
  GravitinoJobAdapter loadJobAdapter(String jobTemplateName) {
    Class<? extends GravitinoJobAdapter> jobAdapterClz = jobAdapters.get(jobTemplateName);
    if (jobAdapterClz == null) {
      String jobAdapterClassName =
          optimizerConfig == null ? null : optimizerConfig.getJobAdapterClassName(jobTemplateName);
      if (StringUtils.isBlank(jobAdapterClassName)) {
        throw new IllegalArgumentException("No job adapter found for template: " + jobTemplateName);
      }
      try {
        Class<?> rawClass = Class.forName(jobAdapterClassName);
        if (!GravitinoJobAdapter.class.isAssignableFrom(rawClass)) {
          throw new IllegalArgumentException(
              "Configured job adapter class does not implement GravitinoJobAdapter: "
                  + jobAdapterClassName);
        }
        jobAdapterClz = rawClass.asSubclass(GravitinoJobAdapter.class);
      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to load job adapter class '"
                + jobAdapterClassName
                + "' for template: "
                + jobTemplateName,
            e);
      }
    }
    try {
      return jobAdapterClz.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to create job adapter for template: " + jobTemplateName, e);
    }
  }
}
