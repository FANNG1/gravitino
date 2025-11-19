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

package org.apache.gravitino.optimizer.recommender;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecommenderCmd {
  private static final Logger LOG = LoggerFactory.getLogger(RecommenderCmd.class);

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
        Option.builder("policy-type").hasArg().required(true).desc("Policy type").build());

    CommandLineParser parser = new DefaultParser();
    StartMode mode;
    String[] identifiers;
    String policyType;
    String confPath;

    try {
      CommandLine cmd = parser.parse(options, args);

      String modeStr = cmd.getOptionValue("mode", StartMode.CLI.name());
      mode = StartMode.fromString(modeStr);
      confPath = cmd.getOptionValue("conf-path", "conf/optimizer.conf");

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
    Recommender recommender = new Recommender(optimizerEnv);
    recommender.recommendForPolicyType(nameIdentifiers, policyType);
  }
}
