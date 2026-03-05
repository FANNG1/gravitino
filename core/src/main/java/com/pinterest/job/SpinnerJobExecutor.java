package com.pinterest.job;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.connector.job.JobExecutor;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.JobTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpinnerJobExecutor implements JobExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(SpinnerJobExecutor.class);

  @VisibleForTesting SpinnerJobClient spinnerClient;

  private final Map<String, SpinnerJobBuilder> jobBuilderRegistry;

  public SpinnerJobExecutor() {
    this.jobBuilderRegistry =
        SpinnerJobBuilderFactory.createJobBuilders().stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    SpinnerJobBuilder::getJobTemplateName, builder -> builder));
  }

  @Override
  public void initialize(Map<String, String> configs) {
    this.spinnerClient = new SpinnerJobClient(configs);
  }

  @Override
  public String submitJob(JobTemplate jobTemplate) {
    SpinnerJobBuilder jobBuilder = getJobBuilder(jobTemplate);
    String dagId = jobBuilder.getDagId();
    Map<String, Object> configs = jobBuilder.getJobConfig(jobTemplate);
    try {
      DagRun dagRun = spinnerClient.submitDagRun(dagId, configs);
      String jobExecutionId = serializeJobId(dagRun);
      LOG.info("Submitted Spinner job: {}", jobExecutionId);
      return jobExecutionId;
    } catch (Exception e) {
      throw new RuntimeException("Failed to submit job", e);
    }
  }

  private SpinnerJobBuilder getJobBuilder(JobTemplate jobTemplate) {
    SpinnerJobBuilder jobBuilder = jobBuilderRegistry.get(jobTemplate.name());
    if (jobBuilder == null) {
      throw new IllegalArgumentException("Unknown job template name: " + jobTemplate.name());
    }
    return jobBuilder;
  }

  private String serializeJobId(DagRun dagRun) {
    return dagRun.getDagId() + "::" + dagRun.getRunId();
  }

  private String[] deserializeJobId(String jobId) throws NoSuchJobException {
    String[] parts = jobId.split("::", 2);
    if (parts.length != 2) {
      throw new NoSuchJobException("Invalid job ID format: %s", jobId);
    }
    return parts;
  }

  @Override
  public JobHandle.Status getJobStatus(String jobId) throws NoSuchJobException {
    String[] parts = deserializeJobId(jobId);
    String dagId = parts[0];
    String runId = parts[1];
    try {
      String state = spinnerClient.getDagRunState(dagId, runId);
      return convertDagState(state);
    } catch (IOException e) {
      throw new RuntimeException("Failed to get job status for job ID: " + jobId, e);
    }
  }

  private JobHandle.Status convertDagState(String state) {
    // TODO roma: revise the state conversion logic later
    // TODO roma: how to handle `skipped` & `none` Spinner state?
    return switch (state) {
      case "queued", "scheduled" -> JobHandle.Status.QUEUED;
      case "running", "up_for_retry", "up_for_reschedule" -> JobHandle.Status.STARTED;
      case "success" -> JobHandle.Status.SUCCEEDED;
      case "failed", "upstream_failed" -> JobHandle.Status.FAILED;
      default -> throw new IllegalArgumentException("Invalid DAG state: " + state);
    };
  }

  @Override
  public void cancelJob(String jobId) throws NoSuchJobException {
    String[] parts = deserializeJobId(jobId);
    String dagId = parts[0];
    String runId = parts[1];
    try {
      spinnerClient.cancelDagRun(new DagRun(dagId, runId));
      LOG.info("Cancelling Spinner job: {}", jobId);
    } catch (IOException e) {
      throw new RuntimeException("Failed to cancel job for job ID: " + jobId, e);
    }
  }

  @Override
  public void close() throws IOException {
    if (spinnerClient != null) {
      spinnerClient.close();
    }
  }
}
