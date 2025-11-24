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

package org.apache.gravitino.optimizer.monitor.evaluator;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.optimizer.api.common.MetricsPoint;
import org.apache.gravitino.optimizer.api.monitor.MetricsEvaluator;
import org.apache.gravitino.optimizer.monitor.job.JobProviderForTest;

public class MetricsEvaluatorForTest implements MetricsEvaluator {

  public static final String NAME = "test-metrics-evaluator";

  public Map<String, List<MetricsPoint>> tableBeforeMetrics;
  public Map<String, List<MetricsPoint>> tableAfterMetrics;

  public Map<String, List<MetricsPoint>> jobBeforeMetrics1;
  public Map<String, List<MetricsPoint>> jobAfterMetrics1;
  public Map<String, List<MetricsPoint>> jobBeforeMetrics2;
  public Map<String, List<MetricsPoint>> jobAfterMetrics2;

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public boolean evaluateTableMetrics(
      NameIdentifier tableIdentifier,
      Map<String, List<MetricsPoint>> beforeMetrics,
      Map<String, List<MetricsPoint>> afterMetrics) {
    this.tableBeforeMetrics = beforeMetrics;
    this.tableAfterMetrics = afterMetrics;
    return true;
  }

  @Override
  public boolean evaluateJobMetrics(
      NameIdentifier jobIdentifier,
      Map<String, List<MetricsPoint>> beforeMetrics,
      Map<String, List<MetricsPoint>> afterMetrics) {
    if (jobIdentifier.equals(JobProviderForTest.job1)) {
      this.jobBeforeMetrics1 = beforeMetrics;
      this.jobAfterMetrics1 = afterMetrics;
      return true;
    }
    if (jobIdentifier.equals(JobProviderForTest.job2)) {
      this.jobBeforeMetrics2 = beforeMetrics;
      this.jobAfterMetrics2 = afterMetrics;
      return true;
    }
    Preconditions.checkArgument(false, "Job %s is not supported", jobIdentifier);
    return false;
  }
}
