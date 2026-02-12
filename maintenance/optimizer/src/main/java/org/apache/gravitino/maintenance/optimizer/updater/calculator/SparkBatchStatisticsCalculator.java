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

package org.apache.gravitino.maintenance.optimizer.updater.calculator;

import com.google.common.base.Splitter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.TableStatisticsBundle;
import org.apache.gravitino.maintenance.optimizer.api.updater.SupportsCalculateTableStatistics;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.recommender.statistics.FileStatisticsReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkBatchStatisticsCalculator implements SupportsCalculateTableStatistics {

  private static final Logger LOG = LoggerFactory.getLogger(SparkBatchStatisticsCalculator.class);

  public static final String NAME = "spark-batch-stats-calculator";

  public static final String SCRIPT_PATH_CONFIG =
      OptimizerConfig.OPTIMIZER_PREFIX + "updater." + NAME + ".script-path";

  public static final String OUTPUT_PATH_CONFIG =
      OptimizerConfig.OPTIMIZER_PREFIX + "updater." + NAME + ".output-path";

  public static final String SPARK_SUBMIT_CONFIG =
      OptimizerConfig.OPTIMIZER_PREFIX + "updater." + NAME + ".spark-submit";

  public static final String SPARK_ARGS_CONFIG =
      OptimizerConfig.OPTIMIZER_PREFIX + "updater." + NAME + ".spark-args";

  private Path scriptPath;
  private Path outputPath;
  private String sparkSubmit;
  private List<String> sparkArgs = Collections.emptyList();
  private FileStatisticsReader fileStatisticsReader;

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    String scriptPathStr = optimizerEnv.config().getRawString(SCRIPT_PATH_CONFIG);
    String outputPathStr = optimizerEnv.config().getRawString(OUTPUT_PATH_CONFIG);
    String submit =
        optimizerEnv
            .config()
            .getRawString(
                SPARK_SUBMIT_CONFIG,
                defaultSparkSubmitFromEnv().orElse("spark-submit")); // fallback if env missing
    String argsStr = optimizerEnv.config().getRawString(SPARK_ARGS_CONFIG, "");
    if (StringUtils.isBlank(scriptPathStr)) {
      throw new IllegalArgumentException(SCRIPT_PATH_CONFIG + " must be provided");
    }
    if (StringUtils.isBlank(outputPathStr)) {
      throw new IllegalArgumentException(OUTPUT_PATH_CONFIG + " must be provided");
    }
    this.scriptPath = Path.of(scriptPathStr);
    this.outputPath = Path.of(outputPathStr);
    this.sparkSubmit = submit;
    if (StringUtils.isNotBlank(argsStr)) {
      sparkArgs = Splitter.onPattern("\\s+").omitEmptyStrings().trimResults().splitToList(argsStr);
    }
    String defaultCatalog =
        optimizerEnv.config().get(OptimizerConfig.GRAVITINO_DEFAULT_CATALOG_CONFIG);
    this.fileStatisticsReader = new FileStatisticsReader(outputPath, defaultCatalog);
  }

  private java.util.Optional<String> defaultSparkSubmitFromEnv() {
    String sparkHome = System.getenv("SPARK_HOME");
    if (StringUtils.isBlank(sparkHome)) {
      return java.util.Optional.empty();
    }
    Path sparkSubmitPath = Path.of(sparkHome, "bin", "spark-submit");
    if (Files.isExecutable(sparkSubmitPath)) {
      return java.util.Optional.of(sparkSubmitPath.toString());
    }
    return java.util.Optional.empty();
  }

  @Override
  public TableStatisticsBundle calculateTableStatistics(NameIdentifier tableIdentifier) {
    ensureOutputParentExists();
    deleteExistingOutput();

    List<String> command = buildCommand(tableIdentifier);
    LOG.info("Running spark statistics script: {}", command);
    int exitCode = runCommand(command);
    if (exitCode != 0) {
      throw new RuntimeException("Spark statistics script failed with exit code " + exitCode);
    }
    List<StatisticEntry<?>> tableStatistics =
        fileStatisticsReader.readTableStatistics(tableIdentifier);
    Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics =
        fileStatisticsReader.readPartitionStatistics(tableIdentifier);
    return new TableStatisticsBundle(tableStatistics, partitionStatistics);
  }

  private List<String> buildCommand(NameIdentifier tableIdentifier) {
    List<String> cmd = new ArrayList<>();
    cmd.add(sparkSubmit);
    cmd.addAll(sparkArgs);
    cmd.add(scriptPath.toString());
    cmd.add("--table");
    cmd.add(tableIdentifier.toString());
    cmd.add("--output");
    cmd.add(outputPath.toString());
    return cmd;
  }

  private int runCommand(List<String> command) {
    ProcessBuilder builder = new ProcessBuilder(command);
    builder.redirectErrorStream(true);
    try {
      Process process = builder.start();
      try (BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
        String stdout = reader.lines().collect(Collectors.joining(System.lineSeparator()));
        if (StringUtils.isNotBlank(stdout)) {
          LOG.info("Spark statistics script output:\n{}", stdout);
        }
      }
      return process.waitFor();
    } catch (IOException | InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Failed to run spark statistics script", e);
    }
  }

  private void ensureOutputParentExists() {
    if (outputPath.getParent() != null) {
      try {
        Files.createDirectories(outputPath.getParent());
      } catch (IOException e) {
        throw new RuntimeException("Failed to create output directory: " + outputPath, e);
      }
    }
  }

  private void deleteExistingOutput() {
    try {
      Files.deleteIfExists(outputPath);
    } catch (IOException e) {
      throw new RuntimeException("Failed to clear existing output file: " + outputPath, e);
    }
  }
}
