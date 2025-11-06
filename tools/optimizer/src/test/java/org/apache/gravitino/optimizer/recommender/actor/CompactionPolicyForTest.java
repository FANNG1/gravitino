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

package org.apache.gravitino.optimizer.recommender.actor;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.optimizer.api.common.policy.RecommenderPolicy;
import org.apache.gravitino.optimizer.recommender.util.PolicyUtils;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

public class CompactionPolicyForTest implements RecommenderPolicy {

  @Override
  public String name() {
    return "compaction-policy-for-test";
  }

  @Override
  public String policyType() {
    return "compaction";
  }

  @Override
  public PolicyContent content() {
    return new PolicyContent() {
      @Override
      public Set<MetadataObject.Type> supportedObjectTypes() {
        return Set.of(MetadataObject.Type.TABLE);
      }

      @Override
      public Map<String, String> properties() {
        return ImmutableMap.of(
            "compaction.trigger-expr",
            "datafile_mse > min_datafile_mse",
            "compaction.score-expr",
            "datafile_mse * delete_file_num");
      }

      @Override
      public Map<String, Object> rules() {
        return ImmutableMap.of(
            "min_datafile_mse",
            1000,
            PolicyUtils.JOB_ROLE_PREFIX + RewriteDataFiles.TARGET_FILE_SIZE_BYTES,
            1024,
            "compaction.trigger-expr",
            "datafile_mse > min_datafile_mse",
            "compaction.score-expr",
            "datafile_mse * delete_file_num");
      }
    };
  }

  @Override
  public Optional<String> jobTemplateName() {
    return Optional.empty();
  }
}
