package org.apache.gravitino.recommender.impl.job;

import java.util.Map;
import org.apache.gravitino.recommender.api.PolicyActor.JobExecuteContext;

public interface GravitinoJobAdapter {
  String policyType();

  void initialize(JobExecuteContext jobExecuteContext);

  String jobTemplateName();

  Map<String, String> jobConfig();
}
