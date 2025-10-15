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

package org.apache.gravitino.maintenance.optimizer.monitor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.maintenance.optimizer.api.monitor.EvaluationResult;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MonitorCallback;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;

public class TestMonitorCallback implements MonitorCallback {

  public static final String NAME = "test-callback";

  private static final List<EvaluationResult> RESULTS = new CopyOnWriteArrayList<>();
  private static volatile CountDownLatch latch = new CountDownLatch(0);

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {}

  @Override
  public void onEvaluation(EvaluationResult result) {
    RESULTS.add(result);
    latch.countDown();
  }

  @Override
  public void close() throws Exception {}

  static void reset(int expected) {
    RESULTS.clear();
    latch = new CountDownLatch(expected);
  }

  static boolean await(long timeout, TimeUnit unit) throws InterruptedException {
    return latch.await(timeout, unit);
  }

  static List<EvaluationResult> results() {
    return new ArrayList<>(RESULTS);
  }
}
