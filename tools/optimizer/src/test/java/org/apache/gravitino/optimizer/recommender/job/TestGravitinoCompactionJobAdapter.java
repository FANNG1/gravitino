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

import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.optimizer.api.common.RecommenderPolicy;
import org.apache.gravitino.optimizer.recommender.actor.compaction.CompactionJobContext;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.rel.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestGravitinoCompactionJobAdapter {

  @Test
  public void testJobTemplateName() {
    GravitinoCompactionJobAdapter jobAdapter = new GravitinoCompactionJobAdapter();
    jobAdapter.initialize(mockCompactionJobContext());
    Assertions.assertEquals("compaction-job-template", jobAdapter.jobTemplateName());
    Assertions.assertEquals(
        Map.of(
            "table", "db.table",
            "where", "",
            "options", "map('target_file_size_bytes', '1073741824')"),
        jobAdapter.jobConfig());
  }

  private CompactionJobContext mockCompactionJobContext() {
    RecommenderPolicy policy = Mockito.mock(RecommenderPolicy.class);
    PolicyContent policyContent = Mockito.mock(PolicyContent.class);
    Mockito.when(policy.content()).thenReturn(policyContent);
    Mockito.when(policy.jobTemplateName()).thenReturn(Optional.of("compaction-job-template"));
    Table table = Mockito.mock(Table.class);
    Map<String, Object> jobConfig = Map.of("target_file_size_bytes", "1073741824");
    return new CompactionJobContext(NameIdentifier.of("db", "table"), jobConfig, policy, table);
  }
}
