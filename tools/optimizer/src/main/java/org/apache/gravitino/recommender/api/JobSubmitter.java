package org.apache.gravitino.recommender.api;

import org.apache.gravitino.recommender.api.PolicyActor.JobExecuteContext;

public interface JobSubmitter {
  String submitJob(String policyType, JobExecuteContext job);
}
