package org.apache.gravitino.recommender.impl;

import org.apache.gravitino.recommender.api.JobSubmiter;
import org.apache.gravitino.recommender.api.PolicyActor.JobConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoopJobSubmitter implements JobSubmiter {
  private final Logger LOG = LoggerFactory.getLogger(NoopJobSubmitter.class);

  @Override
  public void submitJob(String policyType, JobConfig job) {
    LOG.info("NoopJobSubmitter submitJob: policyType={}, job={}", policyType, job);
  }
}
