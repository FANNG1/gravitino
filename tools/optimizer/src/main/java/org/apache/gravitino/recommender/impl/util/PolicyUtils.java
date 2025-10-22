package org.apache.gravitino.recommender.impl.util;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.policy.Policy;

public class PolicyUtils {

  public static final String JOB_ROLE_PREFIX = "job.";

  public static String getTriggerExpression(Policy policy) {
    return policy.content().properties().get("compaction.trigger-expr");
  }

  public static String getJobTemplateName(Policy policy) {
    return policy.content().properties().get("job.template-name");
  }

  public static String getScoreExpression(Policy policy) {
    return policy.content().properties().get("compaction.score-expr");
  }

  @SuppressWarnings("EmptyCatch")
  public static Map<String, Object> getJobConfigFromPolicy(Policy policy) {
    Map<String, Object> jobConfig = new HashMap<>();
    // Todo: get job config from rule not properties
    policy
        .content()
        .properties()
        .forEach(
            (k, v) -> {
              if (k.startsWith(JOB_ROLE_PREFIX)) {
                try {
                  long longValue = Long.parseLong(v);
                  jobConfig.put(k.substring(JOB_ROLE_PREFIX.length()), longValue);
                } catch (NumberFormatException e) {
                }
              }
            });
    return jobConfig;
  }
}
