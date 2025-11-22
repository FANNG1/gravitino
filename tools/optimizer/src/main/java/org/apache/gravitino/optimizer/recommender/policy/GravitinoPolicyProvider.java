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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.optimizer.api.common.RecommenderPolicy;
import org.apache.gravitino.optimizer.api.recommender.PolicyProvider;
import org.apache.gravitino.optimizer.common.OptimizerEnv;
import org.apache.gravitino.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.optimizer.common.util.IdentifierUtils;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.rel.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitinoPolicyProvider implements PolicyProvider {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoPolicyProvider.class);

  public static final String GRAVITINO_POLICY_PROVIDER_NAME = "gravitino-policy-provider";
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
    LOG.info(
        "Get table policy: tableIdentifier={}, catalog={}, table={}",
        tableIdentifier,
        IdentifierUtils.getCatalogNameFromTableIdentifier(tableIdentifier, defaultCatalogName),
        IdentifierUtils.removeCatalogFromIdentifier(tableIdentifier));
    Table t =
        gravitinoClient
            .loadCatalog(
                IdentifierUtils.getCatalogNameFromTableIdentifier(
                    tableIdentifier, defaultCatalogName))
            .asTableCatalog()
            .loadTable(IdentifierUtils.removeCatalogFromIdentifier(tableIdentifier));
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

  private RecommenderPolicy toRecommenderPolicy(Policy policy) {
    return new GravitinoPolicy(policy);
  }
}
