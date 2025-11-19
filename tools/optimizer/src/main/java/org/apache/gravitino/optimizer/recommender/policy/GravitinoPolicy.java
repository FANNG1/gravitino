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

package org.apache.gravitino.optimizer.recommender.policy;

import com.google.common.annotations.VisibleForTesting;
import java.util.Optional;
import org.apache.gravitino.optimizer.api.common.RecommenderPolicy;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;

public class GravitinoPolicy implements RecommenderPolicy {

  @VisibleForTesting public static final String POLICY_TYPE_KEY = "gravitino.policy.type";

  @VisibleForTesting
  public static final String JOB_TEMPLATE_NAME_KEY = "gravitino.policy.job.template-name";

  private Policy policy;

  public GravitinoPolicy(Policy policy) {
    this.policy = policy;
  }

  @Override
  public String name() {
    return policy.name();
  }

  @Override
  public String policyType() {
    return policy.content().properties().get(POLICY_TYPE_KEY);
  }

  @Override
  public PolicyContent content() {
    return policy.content();
  }

  @Override
  public Optional<String> jobTemplateName() {
    return Optional.ofNullable(policy.content().properties().get(JOB_TEMPLATE_NAME_KEY));
  }
}
