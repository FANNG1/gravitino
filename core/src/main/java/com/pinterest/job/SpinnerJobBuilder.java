package com.pinterest.job;

import java.util.Map;
import org.apache.gravitino.job.JobTemplate;

public interface SpinnerJobBuilder {
  String getJobTemplateName();

  String getDagId();

  Map<String, Object> getJobConfig(JobTemplate jobTemplate);
}
