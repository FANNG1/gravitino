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

package org.apache.gravitino.optimizer;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.gravitino.optimizer.common.OptimizerEnv;
import org.apache.gravitino.optimizer.common.StartMode;
import org.apache.gravitino.optimizer.common.util.EnvUtils;
import org.apache.gravitino.optimizer.monitor.MonitorCmd;
import org.apache.gravitino.optimizer.recommender.RecommenderCmd;
import org.apache.gravitino.optimizer.updater.UpdaterCmd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OptimizerCmd {
  private static final Logger LOG = LoggerFactory.getLogger(OptimizerCmd.class);

  public static void main(String[] args) {
    Options options = new Options();
    options.addOption(
        Option.builder("type")
            .hasArg()
            .required(true)
            .desc("Optimizer type: recommender, update_stats, update_metrics, monitor_metrics")
            .build());

    options.addOption(
        Option.builder("mode").hasArg().required(false).desc("Run mode: cli or server").build());

    options.addOption(
        Option.builder("conf-path")
            .hasArg()
            .required(false)
            .desc("Optimizer configuration path")
            .build());

    CommandLineParser parser = new DefaultParser();
    try {
      var cmd = parser.parse(options, args);

      String modeStr = cmd.getOptionValue("mode", StartMode.CLI.name());
      StartMode mode = StartMode.fromString(modeStr);
      Preconditions.checkArgument(mode == StartMode.CLI, "Only CLI mode is supported currently.");

      String confPath = cmd.getOptionValue("conf-path", "conf/optimizer.conf");
      OptimizerEnv optimizerEnv = EnvUtils.getInitializedEnv(confPath);

      String typeStr = cmd.getOptionValue("type");
      OptimizerType type = OptimizerType.valueOf(typeStr.toUpperCase());
      switch (type) {
        case RECOMMENDER:
          LOG.info("Running Recommender");
          RecommenderCmd.runCli(optimizerEnv, args);
          break;
        case UPDATE_STATS:
          UpdaterCmd.runCli(optimizerEnv, args);
          LOG.info("Running Update Stats");
          break;
        case UPDATE_METRICS:
          UpdaterCmd.runCli(optimizerEnv, args);
          LOG.info("Running Update Metrics");
          break;
        case MONITOR_METRICS:
          MonitorCmd.runCli(optimizerEnv, args);
          LOG.info("Running Monitor Metrics Optimizer");
          break;
        default:
          throw new IllegalArgumentException("Unsupported optimizer type: " + typeStr);
      }
    } catch (Exception e) {
      LOG.error("Error parsing command line arguments: " + e.getMessage());
    }
  }

  public enum OptimizerType {
    RECOMMENDER,
    UPDATE_STATS,
    UPDATE_METRICS,
    MONITOR_METRICS
  }
}
