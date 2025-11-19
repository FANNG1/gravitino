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

package org.apache.gravitino.optimizer.monitor;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.optimizer.common.OptimizerEnv;
import org.apache.gravitino.optimizer.common.StartMode;
import org.apache.gravitino.optimizer.common.util.EnvUtils;
import org.apache.gravitino.optimizer.recommender.RecommenderCmd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitorCmd {
  private static final Logger LOG = LoggerFactory.getLogger(RecommenderCmd.class);

  /**
   * Runs the monitor with the given arguments.
   *
   * @param args The command-line arguments. Expected format: [0] - table identifier (e.g.,
   *     "db.table") [1] - Optimize Action time (in epoch seconds) [2] - Range hours (in hours) [3]
   *     - Optional policy type (e.g., "compaction")
   *     <p>For example: ./monitorCli db.table 1760693151 24 compaction will get the table metrics
   *     and job metrics in the range of [action time - range hours*3600, action time + range
   *     hours*3600], and then compare the metrics before and after the action time.
   */
  public static void main(String[] args) {
    Options options = new Options();
    options.addOption(
        Option.builder("mode").hasArg().required(false).desc("Run mode: cli or server").build());

    options.addOption(
        Option.builder("conf-path")
            .hasArg()
            .required(false)
            .desc("Optimizer configuration path")
            .build());

    options.addOption(
        Option.builder("identifiers")
            .hasArg()
            .required(true)
            .valueSeparator(',')
            .desc("Comma separated identifier list")
            .build());

    options.addOption(
        Option.builder("policy-type").hasArg().required(false).desc("Policy type").build());

    options.addOption(
        Option.builder("action-time")
            .hasArg()
            .required(true)
            .desc("Optimize Action time (in epoch seconds)")
            .build());

    options.addOption(
        Option.builder("range-seconds")
            .hasArg()
            .required(true)
            .desc("Range seconds (in seconds)")
            .build());

    CommandLineParser parser = new DefaultParser();
    StartMode mode;
    String[] identifiers;
    String policyType;
    String confPath;
    long actionTime;
    long rangeSeconds;

    try {
      CommandLine cmd = parser.parse(options, args);

      String modeStr = cmd.getOptionValue("mode", "cli");
      mode = StartMode.fromString(modeStr);
      confPath = cmd.getOptionValue("conf-path", "conf/optimizer.conf");

      actionTime = Long.parseLong(cmd.getOptionValue("action-time"));
      rangeSeconds = Long.parseLong(cmd.getOptionValue("range-seconds", "172800"));
      identifiers = cmd.getOptionValues("identifiers");
      policyType = cmd.getOptionValue("policy-type");
    } catch (Exception e) {
      LOG.error("Parse command Error: ", e);
      new HelpFormatter().printHelp("cli-app", options);
      return;
    }
    Preconditions.checkArgument(mode == StartMode.CLI, "Only CLI mode is supported currently.");

    List<NameIdentifier> nameIdentifiers =
        Arrays.stream(identifiers).map(NameIdentifier::parse).toList();
    OptimizerEnv optimizerEnv = EnvUtils.getInitializedEnv(confPath);
    Monitor monitor = new Monitor(optimizerEnv);
    monitor.run(nameIdentifiers, actionTime, rangeSeconds, Optional.ofNullable(policyType));
  }
}
