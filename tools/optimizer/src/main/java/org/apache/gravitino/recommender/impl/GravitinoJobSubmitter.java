package org.apache.gravitino.recommender.impl;

import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.recommender.api.JobSubmiter;
import org.apache.gravitino.recommender.api.PolicyActor.JobConfig;

public class GravitinoJobSubmitter implements JobSubmiter {

  GravitinoClient gravitinoClient;

  @Override
  public void submitJob(String policyType, JobConfig job) {
    gravitinoClient.runJob(getJobTemplateName(policyType), job.config());
  }

  public String getJobTemplateName(String policyType) {
    return policyType + "-job-template";
  }
}
