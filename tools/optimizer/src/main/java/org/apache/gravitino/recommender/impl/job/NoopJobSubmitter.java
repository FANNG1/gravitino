package org.apache.gravitino.recommender.impl.job;

import org.apache.gravitino.recommender.api.JobSubmitter;
import org.apache.gravitino.recommender.api.PolicyActor.JobExecuteContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoopJobSubmitter implements JobSubmitter {
  private final Logger LOG = LoggerFactory.getLogger(NoopJobSubmitter.class);

  @Override
  public String submitJob(String policyType, JobExecuteContext job) {
    LOG.info("NoopJobSubmitter submitJob: policyType={}, job={}", policyType, job);
    return "";
  }
}
