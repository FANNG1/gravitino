package org.apache.gravitino.recommender.impl.job;

import java.util.Map;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.recommender.api.JobSubmitter;
import org.apache.gravitino.recommender.api.PolicyActor.JobExecuteContext;

@SuppressWarnings("unused")
public class GravitinoJobSubmitter implements JobSubmitter {

  GravitinoClient gravitinoClient;

  private Map<String, GravitinoJobAdapter> jobAdapters;

  public GravitinoJobSubmitter() {
    // get all job adapters from the classpath
  }

  @Override
  public String submitJob(String policyType, JobExecuteContext job) {
    GravitinoJobAdapter jobAdapter = loadJobAdapter(policyType, job);
    if (jobAdapter == null) {
      throw new IllegalArgumentException("No job adapter found for policy type: " + policyType);
    }
    return gravitinoClient.runJob(jobAdapter.jobTemplateName(), jobAdapter.jobConfig()).jobId();
  }

  private GravitinoJobAdapter loadJobAdapter(
      String policyType, JobExecuteContext jobExecuteContext) {
    GravitinoCompactionJobAdapter gravitinoCompactionJobAdapter =
        new GravitinoCompactionJobAdapter();
    gravitinoCompactionJobAdapter.initialize(jobExecuteContext);
    return gravitinoCompactionJobAdapter;
  }
}
