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

package org.apache.gravitino.maintenance.optimizer.common.util;

import java.io.File;
import java.util.Properties;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;

public class EnvUtils {
  public static final String CONF_FILE = "gravitino-optimizer.conf";

  public static OptimizerEnv getInitializedEnv(String confPath) {
    OptimizerEnv optimizerEnv = OptimizerEnv.getInstance();
    OptimizerConfig optimizerConfig = loadConfig(confPath);
    optimizerEnv.initialize(optimizerConfig);
    return optimizerEnv;
  }

  private static OptimizerConfig loadConfig(String confPath) {
    OptimizerConfig optimizerConfig = new OptimizerConfig();
    try {
      if (confPath.isEmpty()) {
        optimizerConfig.loadFromFile(CONF_FILE);
      } else {
        Properties properties = optimizerConfig.loadPropertiesFromFile(new File(confPath));
        optimizerConfig.loadFromProperties(properties);
      }
    } catch (Exception exception) {
      throw new IllegalArgumentException("Failed to load conf from file " + confPath, exception);
    }
    return optimizerConfig;
  }
}
