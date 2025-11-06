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

package org.apache.gravitino.optimizer.recommender.impl;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.optimizer.api.common.policy.RecommenderPolicy;
import org.apache.gravitino.optimizer.api.recommender.PolicyProvider;
import org.apache.gravitino.optimizer.common.OptimizerEnv;
import org.apache.gravitino.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.optimizer.recommender.policy.GravitinoPolicy;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.rel.Table;

public class GravitinoPolicyProvider implements PolicyProvider {

  public static final String GRAVITINO_POLICY_PROVIDER_NAME = "gravitino";
  private GravitinoClient gravitinoClient;

  private String defaultCatalogName;

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    OptimizerConfig config = optimizerEnv.config();
    String uri = config.get(OptimizerConfig.GRAVITINO_URI_CONFIG);
    String metalake = config.get(OptimizerConfig.GRAVITINO_METALAKE_CONFIG);
    this.gravitinoClient = GravitinoClient.builder(uri).withMetalake(metalake).build();
    this.defaultCatalogName = config.get(OptimizerConfig.GRAVITINO_DEFAULT_CATALOG_CONFIG);
  }

  @Override
  public String name() {
    return GRAVITINO_POLICY_PROVIDER_NAME;
  }

  @Override
  public List<RecommenderPolicy> getTablePolicy(NameIdentifier tableIdentifier) {
    Table t =
        gravitinoClient
            .loadCatalog(getCatalogName(tableIdentifier))
            .asTableCatalog()
            .loadTable(tableIdentifier);
    String[] policyNames = t.supportsPolicies().listPolicies();
    List<RecommenderPolicy> policies =
        Arrays.stream(policyNames)
            .map(t.supportsPolicies()::getPolicy)
            .filter(Objects::nonNull)
            .map(this::toRecommenderPolicy)
            .collect(Collectors.toList());
    return policies;
  }

  @Override
  public RecommenderPolicy getPolicy(String policyName) {
    return toRecommenderPolicy(gravitinoClient.getPolicy(policyName));
  }

  private String getCatalogName(NameIdentifier tableIdentifier) {
    Namespace namespace = tableIdentifier.namespace();
    Preconditions.checkArgument(namespace != null && namespace.levels().length >= 1);
    if (namespace.levels().length == 1) {
      return defaultCatalogName;
    }

    return namespace.levels()[0];
  }

  private RecommenderPolicy toRecommenderPolicy(Policy policy) {
    return new GravitinoPolicy(policy);
  }
}
