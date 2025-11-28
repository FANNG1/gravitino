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
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.optimizer.common.OptimizerContent;
import org.apache.gravitino.optimizer.common.OptimizerEnv;
import org.apache.gravitino.optimizer.common.StartMode;
import org.apache.gravitino.optimizer.common.StatsComputerContent;
import org.apache.gravitino.optimizer.common.util.EnvUtils;
import org.apache.gravitino.optimizer.monitor.Monitor;
import org.apache.gravitino.optimizer.recommender.Recommender;
import org.apache.gravitino.optimizer.updater.UpdateType;
import org.apache.gravitino.optimizer.updater.Updater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OptimizerCmd {
  private static final Logger LOG = LoggerFactory.getLogger(OptimizerCmd.class);

  public static void main(String[] args) {
    Options options = new Options();
    options.addOption(
        Option.builder()
            .longOpt("type")
            .hasArg()
            .required(true)
            .desc("Optimizer type: " + OptimizerType.allValues())
            .build());

    options.addOption(
        Option.builder()
            .longOpt("mode")
            .hasArg()
            .required(false)
            .desc("Run mode: cli or server")
            .build());

    options.addOption(
        Option.builder()
            .longOpt("conf-path")
            .hasArg()
            .required(false)
            .desc("Optimizer configuration path")
            .build());

    options.addOption(
        Option.builder()
            .longOpt("identifiers")
            .hasArgs()
            .required(false)
            .valueSeparator(',')
            .desc("Comma separated identifier list")
            .build());

    options.addOption(
        Option.builder()
            .longOpt("all-identifiers")
            .required(false)
            .desc("Compute for all identifiers (mutually exclusive with --identifiers)")
            .build());

    options.addOption(
        Option.builder()
            .longOpt("policy-type")
            .hasArg()
            .required(false)
            .desc("Policy type")
            .build());

    options.addOption(
        Option.builder()
            .longOpt("computer-name")
            .hasArg()
            .required(false)
            .desc("The stats computer name to compute stats or metrics")
            .build());

    options.addOption(
        Option.builder()
            .longOpt("action-time")
            .hasArg()
            .required(false)
            .desc("Optimize Action time (in epoch seconds)")
            .build());

    options.addOption(
        Option.builder()
            .longOpt("range-seconds")
            .hasArg()
            .required(false)
            .desc("Range seconds (in seconds)")
            .build());

    options.addOption(
        Option.builder()
            .longOpt("stats-payload")
            .hasArg()
            .required(false)
            .desc("Inline stats payload for CliStatsComputer")
            .build());

    options.addOption(
        Option.builder()
            .longOpt("stats-file")
            .hasArg()
            .required(false)
            .desc("Path to stats payload file for CliStatsComputer")
            .build());

    String computerName;
    String confPath;
    String[] identifiers;
    boolean allIdentifiers;
    String policyType;
    OptimizerType optimizerType;
    String statsPayload;
    String statsFile;

    CommandLineParser parser = new DefaultParser();
    try {
      var cmd = parser.parse(options, args);

      String modeStr = cmd.getOptionValue("mode", StartMode.CLI.name());
      StartMode mode = StartMode.fromString(modeStr);
      Preconditions.checkArgument(mode == StartMode.CLI, "Only CLI mode is supported currently.");

      statsPayload = cmd.getOptionValue("stats-payload");
      statsFile = cmd.getOptionValue("stats-file");
      confPath = cmd.getOptionValue("conf-path", Paths.get("conf", EnvUtils.CONF_FILE).toString());

      computerName = cmd.getOptionValue("computer-name");
      identifiers = cmd.getOptionValues("identifiers");
      allIdentifiers = cmd.hasOption("all-identifiers");
      policyType = cmd.getOptionValue("policy-type");
      String actionTime = cmd.getOptionValue("action-time");
      long defaultRangeSeconds = 24 * 3600;
      String rangeSeconds = cmd.getOptionValue("range-seconds", Long.toString(defaultRangeSeconds));

      String typeStr = cmd.getOptionValue("type");
      checkRequiredOption("type", typeStr);
      optimizerType = OptimizerType.valueOf(typeStr.toUpperCase(Locale.ROOT));
      checkMutualExclusion(allIdentifiers, identifiers);

      OptimizerEnv optimizerEnv = EnvUtils.getInitializedEnv(confPath);
      switch (optimizerType) {
        case RECOMMEND_POLICY_TYPE:
          Preconditions.checkArgument(
              !allIdentifiers, "--all-identifiers is not supported for recommend policy type");
          checkRequiredOption("identifiers", identifiers);
          checkRequiredOption("policy-type", policyType);

          runWithoutException(
              () -> {
                List<NameIdentifier> nameIdentifiers =
                    Arrays.stream(identifiers).map(NameIdentifier::parse).toList();
                Recommender recommender = new Recommender(optimizerEnv);
                recommender.recommendForPolicyType(nameIdentifiers, policyType);
              });
          break;
        case UPDATE_STATS:
          checkRequiredOption("computer-name", computerName);

          runWithoutException(
              () -> {
                OptimizerContent optimizerContent =
                    buildStatsComputerContent(statsPayload, statsFile);
                optimizerEnv.setContent(optimizerContent);
                Updater updater = new Updater(optimizerEnv);
                if (allIdentifiers) {
                  updater.updateAll(computerName, UpdateType.STATS);
                } else {
                  checkRequiredOption("identifiers", identifiers);
                  List<NameIdentifier> nameIdentifiers =
                      Arrays.stream(identifiers).map(NameIdentifier::parse).toList();
                  updater.update(computerName, nameIdentifiers, UpdateType.STATS);
                }
              });
          break;
        case UPDATE_METRICS:
          checkRequiredOption("computer-name", computerName);

          runWithoutException(
              () -> {
                OptimizerContent optimizerContent =
                    buildStatsComputerContent(statsPayload, statsFile);
                optimizerEnv.setContent(optimizerContent);
                Updater updater = new Updater(optimizerEnv);
                if (allIdentifiers) {
                  updater.updateAll(computerName, UpdateType.METRICS);
                } else {
                  checkRequiredOption("identifiers", identifiers);
                  List<NameIdentifier> nameIdentifiers =
                      Arrays.stream(identifiers).map(NameIdentifier::parse).toList();
                  updater.update(computerName, nameIdentifiers, UpdateType.METRICS);
                }
              });
          break;
        case MONITOR_METRICS:
          Preconditions.checkArgument(
              !allIdentifiers, "--all-identifiers is not supported for monitor metrics");
          checkRequiredOption("identifiers", identifiers);
          checkRequiredOption("action-time", actionTime);
          Long actionTimeLong = Long.parseLong(actionTime);
          Long rangeSecondsLong = Long.parseLong(rangeSeconds);

          runWithoutException(
              () -> {
                List<NameIdentifier> nameIdentifiers =
                    Arrays.stream(identifiers).map(NameIdentifier::parse).toList();
                Monitor monitor = new Monitor(optimizerEnv);
                monitor.run(nameIdentifiers, actionTimeLong, rangeSecondsLong, Optional.empty());
              });
          break;
        default:
          String error = String.format("Unsupported optimizer type: %s.", optimizerType);
          throw new IllegalArgumentException(error);
      }
    } catch (Exception e) {
      System.err.println(e.getMessage());
      new HelpFormatter().printHelp("gravitino-optimizer", options);
      LOG.error("Error parsing command line arguments: ", e);
    }
  }

  private static void checkRequiredOption(String optionName, String optionValue) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(optionValue), String.format("Option %s is required.", optionName));
  }

  private static void checkRequiredOption(String optionName, String[] optionValue) {
    Preconditions.checkArgument(
        optionValue != null, String.format("Option %s is required.", optionName));
  }

  private static void checkMutualExclusion(boolean allIdentifiers, String[] identifiers) {
    Preconditions.checkArgument(
        !(allIdentifiers && identifiers != null),
        "--all-identifiers and --identifiers cannot be used together");
  }

  private static OptimizerContent buildStatsComputerContent(
      String statsPayload, String statsFilePath) {
    boolean hasPayload = StringUtils.isNotBlank(statsPayload);
    boolean hasFile = StringUtils.isNotBlank(statsFilePath);

    Preconditions.checkArgument(
        !(hasPayload && hasFile), "--stats-payload and --stats-file cannot be used together");

    return new StatsComputerContent(
        hasFile ? statsFilePath : null, hasPayload ? statsPayload : null);
  }

  private static void runWithoutException(Runnable runnable) {
    try {
      runnable.run();
    } catch (Exception e) {
      System.err.println(e.getMessage());
      LOG.error("Error running optimizer: ", e);
    }
  }

  enum OptimizerType {
    RECOMMEND_POLICY_TYPE,
    UPDATE_STATS,
    UPDATE_METRICS,
    MONITOR_METRICS;

    public static String allValues() {
      return Arrays.stream(values())
          .map(Enum::name)
          .map(String::toLowerCase)
          .collect(Collectors.joining(","));
    }
  }
}
