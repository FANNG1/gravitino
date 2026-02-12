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

package org.apache.gravitino.maintenance.optimizer.common;

import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;

/**
 * Immutable container for runtime optimizer context passed into pluggable components. Today it only
 * exposes {@link OptimizerConfig}, but it is intended to carry additional shared resources (client
 * handles, metrics) as the optimizer matures.
 */
public class OptimizerEnv {
  private OptimizerConfig config;
  private OptimizerContent content;

  private OptimizerEnv() {}

  public OptimizerEnv(OptimizerConfig config) {
    this.config = config;
  }

  private static class InstanceHolder {
    private static final OptimizerEnv INSTANCE = new OptimizerEnv();
  }

  public static OptimizerEnv getInstance() {
    return InstanceHolder.INSTANCE;
  }

  public void initialize(OptimizerConfig config) {
    initialize(config, null);
  }

  public void initialize(OptimizerConfig config, OptimizerContent content) {
    this.config = config;
    this.content = content;
  }

  public OptimizerConfig config() {
    return config;
  }

  public OptimizerContent content() {
    return content;
  }

  public void setContent(OptimizerContent content) {
    this.content = content;
  }
}
