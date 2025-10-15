package org.apache.gravitino.recommender.api;

import org.apache.gravitino.recommender.api.PolicyActor.JobConfig;

public interface JobSubmiter {
  void submitJob(String policyType, JobConfig job);
}
