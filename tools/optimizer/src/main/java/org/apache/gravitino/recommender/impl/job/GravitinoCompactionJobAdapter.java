package org.apache.gravitino.recommender.impl.job;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.gravitino.recommender.api.PolicyActor.JobExecuteContext;
import org.apache.gravitino.recommender.impl.actor.CompactionJobContext;
import org.apache.gravitino.recommender.impl.util.PolicyUtils;

public class GravitinoCompactionJobAdapter implements GravitinoJobAdapter {

  private CompactionJobContext jobContext;

  @Override
  public String policyType() {
    return "compaction";
  }

  @Override
  public void initialize(JobExecuteContext jobExecuteContext) {
    this.jobContext = (CompactionJobContext) jobExecuteContext;
  }

  @Override
  public String jobTemplateName() {
    return PolicyUtils.getJobTemplateName(jobContext.policy());
  }

  @Override
  public Map<String, String> jobConfig() {
    return ImmutableMap.of(
        "table", getTableName(), "where", getWhereClause(), "options", getOptions());
  }

  private String getTableName() {
    return jobContext.name().toString();
  }

  private String getWhereClause() {
    // generate where clause from jobContext.partitionNames()
    return "";
  }

  private String getOptions() {
    return convertMapToString(jobContext.config());
  }

  public static String convertMapToString(Map<String, Object> map) {
    if (map == null || map.isEmpty()) {
      return "map()";
    }

    StringBuilder sb = new StringBuilder();
    sb.append("map(");

    boolean isFirstEntry = true;
    for (Entry<String, Object> entry : map.entrySet()) {
      if (!isFirstEntry) {
        sb.append(", ");
      }
      sb.append("'").append(entry.getKey()).append("', '").append(entry.getValue()).append("'");
      isFirstEntry = false;
    }

    sb.append(")");
    return sb.toString();
  }
}
