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

package org.apache.gravitino.optimizer.integration.test;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.optimizer.api.common.RecommenderPolicy;
import org.apache.gravitino.optimizer.recommender.policy.GravitinoPolicyProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class GravitinoPolicyIT extends GravitinoOptimizerEnvIT {

  private GravitinoPolicyProvider policyProvider;

  @BeforeAll
  void init() {
    this.policyProvider = new GravitinoPolicyProvider();
    policyProvider.initialize(optimizerEnv);
  }

  @Test
  void testGravitinoPolicyProviderGetPolicy() {
    createPolicy("test_policy", ImmutableMap.of("rule1", "value1"), "test");

    RecommenderPolicy policy = policyProvider.getPolicy("test_policy");
    Assertions.assertNotNull(policy);
    Assertions.assertEquals("test", policy.policyType());
    Assertions.assertEquals("template-name", policy.jobTemplateName().orElse(null));
    Assertions.assertEquals(ImmutableMap.of("rule1", "value1"), policy.content().rules());
  }

  @Test
  void testGravitinoPolicyProviderGetTablePolicy() {
    String tableName = "test_get_table_policy";
    createTable(tableName);
    createPolicy("policy1", ImmutableMap.of("rule1", "value1"), "test");
    createPolicy("policy2", ImmutableMap.of("rule2", "value2"), "test");
    associatePoliciesToTable("policy1", tableName);
    associatePoliciesToTable("policy2", tableName);

    List<RecommenderPolicy> policies =
        policyProvider.getTablePolicy(NameIdentifier.of(TEST_SCHEMA, tableName));
    Assertions.assertNotNull(policies);
    Assertions.assertEquals(2, policies.size());

    policies.stream()
        .forEach(
            policy -> {
              if (policy.name().equals("policy1")) {
                Assertions.assertEquals("test", policy.policyType());
                Assertions.assertEquals("template-name", policy.jobTemplateName().orElse(null));
                Assertions.assertEquals(
                    ImmutableMap.of("rule1", "value1"), policy.content().rules());
              } else if (policy.name().equals("policy2")) {
                Assertions.assertEquals("test", policy.policyType());
                Assertions.assertEquals("template-name", policy.jobTemplateName().orElse(null));
                Assertions.assertEquals(
                    ImmutableMap.of("rule2", "value2"), policy.content().rules());
              } else {
                Assertions.fail("Unexpected policy name: " + policy.name());
              }
            });
  }
}
