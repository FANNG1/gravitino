/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.optimizer.recommender.job;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.gravitino.optimizer.api.recommender.PolicyActor.JobExecuteContext;
import org.apache.gravitino.optimizer.recommender.actor.CompactionJobContext;
import org.apache.gravitino.optimizer.recommender.util.PolicyUtils;

public class GravitinoCompactionJobAdapter implements GravitinoJobAdapter {

  private CompactionJobContext jobContext;

  @Override
  public String policyType() {
    return PolicyUtils.COMPACTION_POLICY_TYPE;
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
    return jobContext.identifier().toString();
  }

  private String getWhereClause() {
    // generate where clause from jobContext.partitionNames()
    return "";
  }

  private String getOptions() {
    Map<String, Object> map = jobContext.config();
    return convertMapToString(map);
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
