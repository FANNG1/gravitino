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

package org.apache.gravitino.recommender.impl;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.recommender.api.PolicyProvider;
import org.apache.gravitino.rel.Table;

public class GravitinoPolicyProvider implements PolicyProvider {

  GravitinoClient client;

  @Override
  public List<Policy> getTablePolicy(NameIdentifier tableIdentifier) {
    Table t = client.loadCatalog("").asTableCatalog().loadTable(tableIdentifier);
    String[] policyNames = t.supportsPolicies().listPolicies();
    List<Policy> policies =
        Arrays.stream(policyNames)
            .map(t.supportsPolicies()::getPolicy)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    return policies;
  }

  @Override
  public Policy getPolicy(String policyName) {
    return client.getPolicy(policyName);
  }
}
