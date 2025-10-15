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

package org.apache.gravitino.maintenance.optimizer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerContent;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.StartMode;
import org.apache.gravitino.maintenance.optimizer.common.StatisticsComputerContent;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.common.util.EnvUtils;
import org.apache.gravitino.maintenance.optimizer.common.util.IdentifierUtils;
import org.apache.gravitino.maintenance.optimizer.monitor.Monitor;
import org.apache.gravitino.maintenance.optimizer.monitor.service.MonitorServiceServer;
import org.apache.gravitino.maintenance.optimizer.monitor.service.MonitorSubmitRequest;
import org.apache.gravitino.maintenance.optimizer.recommender.Recommender;
import org.apache.gravitino.maintenance.optimizer.recommender.util.PartitionUtils;
import org.apache.gravitino.maintenance.optimizer.tool.MetricsLister;
import org.apache.gravitino.maintenance.optimizer.tool.TableRegister;
import org.apache.gravitino.maintenance.optimizer.updater.UpdateType;
import org.apache.gravitino.maintenance.optimizer.updater.Updater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OptimizerCmd {
  private static final Logger LOG = LoggerFactory.getLogger(OptimizerCmd.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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
            .longOpt("identifier")
            .hasArg()
            .required(false)
            .desc("Single identifier")
            .build());

    options.addOption(
        Option.builder()
            .longOpt("strategy-type")
            .hasArg()
            .required(false)
            .desc("Strategy type")
            .build());

    options.addOption(
        Option.builder()
            .longOpt("computer-name")
            .hasArg()
            .required(false)
            .desc("The statistics computer name to compute statistics or metrics")
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
            .longOpt("action-time-seconds")
            .hasArg()
            .required(false)
            .desc("Action time (in epoch seconds)")
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
            .longOpt("statistics-payload")
            .hasArg()
            .required(false)
            .desc("Inline statistics payload for CliStatisticsComputer")
            .build());

    options.addOption(
        Option.builder()
            .longOpt("file-path")
            .hasArg()
            .required(false)
            .desc(
                "Path to the input file (statistics payload file or table registration JSON per line)")
            .build());

    options.addOption(
        Option.builder()
            .longOpt("partition-path")
            .hasArg()
            .required(false)
            .desc(
                "Partition path for monitor_metrics (format: {\"col1\":\"val1\"} or col1=val1/col2=val2)")
            .build());

    options.addOption(
        Option.builder()
            .longOpt("monitor-id")
            .hasArg()
            .required(false)
            .desc("Monitor identifier")
            .build());

    options.addOption(
        Option.builder()
            .longOpt("monitor-service-url")
            .hasArg()
            .required(false)
            .desc("Monitor service base URL (default: http://localhost:<port>)")
            .build());

    options.addOption(
        Option.builder().longOpt("verbose").required(false).desc("Verbose output").build());
    String computerName;
    String confPath;
    String[] identifiers;
    String identifier;
    boolean allIdentifiers;
    String monitorId;
    String strategyType;
    OptimizerType optimizerType;
    String statisticsPayload;
    String filePath;
    String partitionPathStr;
    boolean verbose;
    String actionTimeSecondsStr;
    String rangeSecondsRaw;
    String monitorServiceUrl;

    CommandLineParser parser = new DefaultParser();
    try {
      var cmd = parser.parse(options, args);

      String modeStr = cmd.getOptionValue("mode", StartMode.CLI.name());
      StartMode mode = StartMode.fromString(modeStr);
      Preconditions.checkArgument(mode == StartMode.CLI, "Only CLI mode is supported currently.");

      statisticsPayload = cmd.getOptionValue("statistics-payload");
      filePath = cmd.getOptionValue("file-path");
      confPath = cmd.getOptionValue("conf-path", Paths.get("conf", EnvUtils.CONF_FILE).toString());
      partitionPathStr = cmd.getOptionValue("partition-path");
      verbose = cmd.hasOption("verbose");
      monitorId = cmd.getOptionValue("monitor-id");
      identifier = cmd.getOptionValue("identifier");
      monitorServiceUrl = cmd.getOptionValue("monitor-service-url");

      computerName = cmd.getOptionValue("computer-name");
      identifiers = cmd.getOptionValues("identifiers");
      strategyType = cmd.getOptionValue("strategy-type");
      actionTimeSecondsStr = cmd.getOptionValue("action-time-seconds");
      String actionTime = cmd.getOptionValue("action-time");
      long defaultRangeSeconds = 24 * 3600;
      rangeSecondsRaw = cmd.getOptionValue("range-seconds");
      String rangeSeconds = cmd.getOptionValue("range-seconds", Long.toString(defaultRangeSeconds));

      String typeStr = cmd.getOptionValue("type");
      checkRequiredOption("type", typeStr);
      optimizerType = OptimizerType.fromString(typeStr);
      allIdentifiers = identifiers == null || identifiers.length == 0;

      OptimizerEnv optimizerEnv = EnvUtils.getInitializedEnv(confPath);
      String defaultCatalogName =
          optimizerEnv.config().get(OptimizerConfig.GRAVITINO_DEFAULT_CATALOG_CONFIG);
      switch (optimizerType) {
        case RECOMMEND_STRATEGY_TYPE:
          checkRequiredOption("identifiers", identifiers);
          checkRequiredOption("strategy-type", strategyType);

          runWithoutException(
              () -> {
                List<NameIdentifier> nameIdentifiers =
                    parseAndNormalizeIdentifiers(identifiers, defaultCatalogName);
                runWithSummary(
                    "recommend_strategy_type",
                    () -> {
                      Recommender recommender = new Recommender(optimizerEnv);
                      recommender.recommendForStrategyType(nameIdentifiers, strategyType);
                    },
                    buildSummaryDetails(
                        nameIdentifiers, "strategyType=" + strategyType, Optional.empty()));
              });
          break;
        case UPDATE_STATISTICS:
          checkRequiredOption("computer-name", computerName);

          runWithoutException(
              () -> {
                OptimizerContent optimizerContent =
                    buildStatisticsComputerContent(statisticsPayload, filePath);
                optimizerEnv.setContent(optimizerContent);
                Updater updater = new Updater(optimizerEnv);
                if (allIdentifiers) {
                  runWithSummary(
                      "update_statistics",
                      () -> updater.updateAll(computerName, UpdateType.STATISTICS),
                      buildSummaryDetails(
                          null,
                          "computerName=" + computerName,
                          Optional.of("allIdentifiers=true")));
                } else {
                  checkRequiredOption("identifiers", identifiers);
                  List<NameIdentifier> nameIdentifiers =
                      parseAndNormalizeIdentifiers(identifiers, defaultCatalogName);
                  runWithSummary(
                      "update_statistics",
                      () -> updater.update(computerName, nameIdentifiers, UpdateType.STATISTICS),
                      buildSummaryDetails(
                          nameIdentifiers, "computerName=" + computerName, Optional.empty()));
                }
              });
          break;
        case APPEND_METRICS:
          checkRequiredOption("computer-name", computerName);

          runWithoutException(
              () -> {
                OptimizerContent optimizerContent =
                    buildStatisticsComputerContent(statisticsPayload, filePath);
                optimizerEnv.setContent(optimizerContent);
                Updater updater = new Updater(optimizerEnv);
                if (allIdentifiers) {
                  runWithSummary(
                      "append_metrics",
                      () -> updater.updateAll(computerName, UpdateType.METRICS),
                      buildSummaryDetails(
                          null,
                          "computerName=" + computerName,
                          Optional.of("allIdentifiers=true")));
                } else {
                  checkRequiredOption("identifiers", identifiers);
                  boolean jobMetricsPayload =
                      StringUtils.isNotBlank(statisticsPayload)
                          && statisticsPayload.startsWith("job:");
                  List<NameIdentifier> nameIdentifiers =
                      jobMetricsPayload
                          ? parseIdentifiers(identifiers)
                          : parseAndNormalizeIdentifiers(identifiers, defaultCatalogName);
                  runWithSummary(
                      "append_metrics",
                      () -> updater.update(computerName, nameIdentifiers, UpdateType.METRICS),
                      buildSummaryDetails(
                          nameIdentifiers, "computerName=" + computerName, Optional.empty()));
                }
              });
          break;
        case MONITOR_METRICS:
          checkRequiredOption("identifiers", identifiers);
          String monitorActionTime = StringUtils.defaultIfBlank(actionTimeSecondsStr, actionTime);
          checkRequiredOption("action-time", monitorActionTime);
          Optional<PartitionPath> monitorPartitionPath = parsePartitionPath(partitionPathStr);
          Preconditions.checkArgument(
              monitorPartitionPath.isEmpty() || identifiers.length == 1,
              "--partition-path requires exactly one identifier");
          Long actionTimeLong = Long.parseLong(monitorActionTime);
          Long rangeSecondsLong = Long.parseLong(rangeSeconds);

          runWithoutException(
              () -> {
                List<NameIdentifier> nameIdentifiers =
                    parseAndNormalizeIdentifiers(identifiers, defaultCatalogName);
                runWithSummary(
                    "monitor_metrics",
                    () -> {
                      Monitor monitor = new Monitor(optimizerEnv);
                      monitor.run(
                          nameIdentifiers,
                          actionTimeLong,
                          rangeSecondsLong,
                          Optional.empty(),
                          monitorPartitionPath);
                    },
                    buildSummaryDetails(
                        nameIdentifiers,
                        "actionTime=" + actionTimeLong + ",rangeSeconds=" + rangeSecondsLong,
                        monitorPartitionPath.map(path -> "partitionPath=" + path.toString())));
              });
          break;
        case SUBMIT_MONITOR:
          checkRequiredOption("identifier", identifier);
          runWithoutException(
              () -> {
                String resolvedMonitorServiceUrl =
                    resolveMonitorServiceUrl(optimizerEnv, monitorServiceUrl);
                long submitRangeSeconds = resolveRangeSeconds(rangeSecondsRaw, 2 * 24 * 3600L);
                long submitActionTimeSeconds =
                    resolveActionTimeSeconds(
                        StringUtils.defaultIfBlank(actionTimeSecondsStr, actionTime));
                validateRangeSeconds(submitRangeSeconds);
                validateActionTimeSeconds(submitActionTimeSeconds);
                validatePartitionPath(partitionPathStr);
                Map<String, Object> response =
                    submitMonitor(
                        resolvedMonitorServiceUrl,
                        identifier,
                        partitionPathStr,
                        submitActionTimeSeconds,
                        submitRangeSeconds);
                String submittedMonitorId = String.valueOf(response.get("monitorId"));
                if (verbose) {
                  printMonitorSubmitVerbose(
                      submittedMonitorId,
                      identifier,
                      partitionPathStr,
                      submitActionTimeSeconds,
                      submitRangeSeconds);
                } else {
                  System.out.println(submittedMonitorId);
                }
              });
          break;
        case CANCEL_MONITOR:
          checkRequiredOption("monitor-id", monitorId);
          runWithoutException(
              () -> {
                String resolvedMonitorServiceUrl =
                    resolveMonitorServiceUrl(optimizerEnv, monitorServiceUrl);
                Map<String, Object> response = cancelMonitor(resolvedMonitorServiceUrl, monitorId);
                if (verbose) {
                  printMonitorResponseVerbose(response);
                } else {
                  printMonitorStateLine(response);
                }
              });
          break;
        case GET_MONITOR:
          checkRequiredOption("monitor-id", monitorId);
          runWithoutException(
              () -> {
                String resolvedMonitorServiceUrl =
                    resolveMonitorServiceUrl(optimizerEnv, monitorServiceUrl);
                Map<String, Object> response = getMonitor(resolvedMonitorServiceUrl, monitorId);
                if (verbose) {
                  printMonitorResponseVerbose(response);
                } else {
                  printMonitorStatusLine(response);
                }
              });
          break;
        case LIST_MONITORS:
          runWithoutException(
              () -> {
                String resolvedMonitorServiceUrl =
                    resolveMonitorServiceUrl(optimizerEnv, monitorServiceUrl);
                List<Map<String, Object>> response = listMonitors(resolvedMonitorServiceUrl);
                printMonitorList(response);
              });
          break;
        case RUN_MONITOR_SERVICE:
          runWithoutException(() -> runMonitorService(optimizerEnv));
          break;
        case REGISTER_TABLES:
          checkRequiredOption("file-path", filePath);
          runWithoutException(
              () -> {
                TableRegister tableRegister = new TableRegister(optimizerEnv);
                tableRegister.registerFromFile(filePath);
              });
          break;
        case LIST_TABLE_METRICS:
          checkRequiredOption("identifiers", identifiers);
          Optional<PartitionPath> partitionPath = parsePartitionPath(partitionPathStr);

          runWithoutException(
              () -> {
                List<NameIdentifier> nameIdentifiers =
                    parseAndNormalizeIdentifiers(identifiers, defaultCatalogName);
                MetricsLister lister = new MetricsLister(optimizerEnv);
                lister.listTableMetrics(
                    nameIdentifiers, partitionPath, /*startTimeSeconds=*/ 0L, Long.MAX_VALUE);
              });
          break;
        case LIST_JOB_METRICS:
          checkRequiredOption("identifiers", identifiers);

          runWithoutException(
              () -> {
                List<NameIdentifier> nameIdentifiers = parseIdentifiers(identifiers);
                MetricsLister lister = new MetricsLister(optimizerEnv);
                lister.listJobMetrics(nameIdentifiers, /*startTimeSeconds=*/ 0L, Long.MAX_VALUE);
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

  private static List<NameIdentifier> parseAndNormalizeIdentifiers(
      String[] identifiers, String defaultCatalogName) {
    Preconditions.checkArgument(identifiers != null, "identifiers must not be null");
    return Arrays.stream(identifiers)
        .map(NameIdentifier::parse)
        .map(identifier -> IdentifierUtils.normalizeTableIdentifier(identifier, defaultCatalogName))
        .toList();
  }

  static List<NameIdentifier> parseIdentifiers(String[] identifiers) {
    Preconditions.checkArgument(identifiers != null, "identifiers must not be null");
    return Arrays.stream(identifiers).map(NameIdentifier::parse).toList();
  }

  private static OptimizerContent buildStatisticsComputerContent(
      String statisticsPayload, String filePath) {
    boolean hasPayload = StringUtils.isNotBlank(statisticsPayload);
    boolean hasFile = StringUtils.isNotBlank(filePath);

    Preconditions.checkArgument(
        !(hasPayload && hasFile), "--statistics-payload and --file-path cannot be used together");

    return new StatisticsComputerContent(
        hasFile ? filePath : null, hasPayload ? statisticsPayload : null);
  }

  private static void runWithoutException(Runnable runnable) {
    try {
      runnable.run();
    } catch (Exception e) {
      System.err.println(e.getMessage());
      LOG.error("Error running optimizer: ", e);
    }
  }

  private static void runWithSummary(String action, Runnable runnable, String details) {
    long startTimeMs = System.currentTimeMillis();
    try {
      runnable.run();
      long elapsedMs = System.currentTimeMillis() - startTimeMs;
      System.out.printf("OK: %s (%s) in %d ms%n", action, details, elapsedMs);
    } catch (Exception e) {
      long elapsedMs = System.currentTimeMillis() - startTimeMs;
      System.err.printf(
          "FAILED: %s (%s) after %d ms. %s%n",
          action, details, elapsedMs, String.valueOf(e.getMessage()));
      LOG.error("Error running optimizer: ", e);
    }
  }

  private static String buildSummaryDetails(
      List<NameIdentifier> identifiers, String requiredDetails, Optional<String> extraDetails) {
    String identifierDetails =
        identifiers == null
            ? "identifiers=all"
            : "identifiers="
                + identifiers.stream()
                    .map(NameIdentifier::toString)
                    .collect(Collectors.joining(","));
    String extra = extraDetails.map(value -> "," + value).orElse("");
    return identifierDetails + "," + requiredDetails + extra;
  }

  static Optional<PartitionPath> parsePartitionPath(String partitionPathStr) {
    if (StringUtils.isBlank(partitionPathStr)) {
      return Optional.empty();
    }
    String trimmed = partitionPathStr.trim();
    if (trimmed.startsWith("{")) {
      return Optional.of(parseJsonPartitionPath(trimmed));
    }
    return Optional.of(PartitionUtils.parseLegacyPartitionPath(trimmed));
  }

  private static PartitionPath parseJsonPartitionPath(String partitionPathJson) {
    try {
      var node = OBJECT_MAPPER.readTree(partitionPathJson);
      Preconditions.checkArgument(
          node != null && node.isObject(), "--partition-path must be a JSON object");
      List<PartitionEntry> entries = new java.util.ArrayList<>();
      node.fields()
          .forEachRemaining(
              entry -> {
                var valueNode = entry.getValue();
                Preconditions.checkArgument(
                    valueNode != null && valueNode.isTextual(),
                    "--partition-path values must be strings");
                entries.add(new PartitionEntryImpl(entry.getKey(), valueNode.asText()));
              });
      Preconditions.checkArgument(!entries.isEmpty(), "--partition-path cannot be empty");
      return PartitionPath.of(entries);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid JSON for --partition-path", e);
    }
  }

  private static String buildMonitorServiceUrl(OptimizerEnv optimizerEnv) {
    int port = optimizerEnv.config().get(OptimizerConfig.MONITOR_SERVICE_PORT_CONFIG);
    return "http://localhost:" + port;
  }

  private static String resolveMonitorServiceUrl(
      OptimizerEnv optimizerEnv, String monitorServiceUrl) {
    if (StringUtils.isNotBlank(monitorServiceUrl)) {
      return StringUtils.removeEnd(monitorServiceUrl, "/");
    }
    return buildMonitorServiceUrl(optimizerEnv);
  }

  private static long resolveRangeSeconds(String rangeSeconds, long defaultValue) {
    if (StringUtils.isBlank(rangeSeconds)) {
      return defaultValue;
    }
    return Long.parseLong(rangeSeconds);
  }

  private static long resolveActionTimeSeconds(String actionTimeSeconds) {
    if (StringUtils.isBlank(actionTimeSeconds)) {
      return Instant.now().getEpochSecond();
    }
    return Long.parseLong(actionTimeSeconds);
  }

  private static void validateRangeSeconds(long rangeSeconds) {
    Preconditions.checkArgument(rangeSeconds >= 0, "rangeSeconds must be >= 0");
  }

  private static void validateActionTimeSeconds(long actionTimeSeconds) {
    Preconditions.checkArgument(actionTimeSeconds > 0, "actionTimeSeconds must be > 0");
  }

  private static void validatePartitionPath(String partitionPathStr) {
    if (StringUtils.isBlank(partitionPathStr)) {
      return;
    }
    parsePartitionPath(partitionPathStr);
  }

  private static void runMonitorService(OptimizerEnv optimizerEnv) {
    MonitorServiceServer server = new MonitorServiceServer(optimizerEnv);
    CountDownLatch shutdownLatch = new CountDownLatch(1);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    server.stop();
                  } catch (Exception e) {
                    LOG.warn("Failed to stop monitor service", e);
                  } finally {
                    shutdownLatch.countDown();
                  }
                },
                "monitor-service-shutdown"));
    try {
      server.start();
    } catch (Exception e) {
      throw new RuntimeException("Failed to start monitor service", e);
    }
    System.out.printf("Monitor service started on port %d%n", server.localPort());
    try {
      shutdownLatch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static Map<String, Object> submitMonitor(
      String monitorServiceUrl,
      String identifier,
      String partitionPath,
      long actionTimeSeconds,
      long rangeSeconds) {
    MonitorSubmitRequest request = new MonitorSubmitRequest();
    request.identifier = identifier;
    request.partitionPath = partitionPath;
    request.actionTimeSeconds = actionTimeSeconds;
    request.rangeSeconds = rangeSeconds;
    String payload = writeJson(request);
    String response = executeRequest("POST", monitorServiceUrl + "/v1/monitor", payload);
    return readJsonMap(response);
  }

  private static Map<String, Object> cancelMonitor(String monitorServiceUrl, String monitorId) {
    String response =
        executeRequest("POST", monitorServiceUrl + "/v1/monitor/" + monitorId + "/cancel", null);
    return readJsonMap(response);
  }

  private static Map<String, Object> getMonitor(String monitorServiceUrl, String monitorId) {
    String response = executeRequest("GET", monitorServiceUrl + "/v1/monitor/" + monitorId, null);
    return readJsonMap(response);
  }

  private static List<Map<String, Object>> listMonitors(String monitorServiceUrl) {
    String response = executeRequest("GET", monitorServiceUrl + "/v1/monitor", null);
    return readJsonList(response);
  }

  private static String executeRequest(String method, String url, String payloadJson) {
    try {
      HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
      connection.setRequestMethod(method);
      connection.setConnectTimeout(5000);
      connection.setReadTimeout(30000);
      if (payloadJson != null) {
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/json");
        try (OutputStream output = connection.getOutputStream()) {
          output.write(payloadJson.getBytes(StandardCharsets.UTF_8));
        }
      }
      int code = connection.getResponseCode();
      InputStream inputStream =
          code >= 200 && code < 300 ? connection.getInputStream() : connection.getErrorStream();
      String responseBody = readResponseBody(inputStream);
      if (code < 200 || code >= 300) {
        throw new IllegalArgumentException(
            String.format("Request failed (%d): %s", code, responseBody));
      }
      return responseBody;
    } catch (IOException e) {
      throw new IllegalArgumentException("Request failed: " + e.getMessage(), e);
    }
  }

  private static String readResponseBody(InputStream inputStream) throws IOException {
    if (inputStream == null) {
      return "";
    }
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
      StringBuilder builder = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        builder.append(line);
      }
      return builder.toString();
    }
  }

  private static String writeJson(Object payload) {
    try {
      return OBJECT_MAPPER.writeValueAsString(payload);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to serialize payload: " + e.getMessage(), e);
    }
  }

  private static Map<String, Object> readJsonMap(String payload) {
    try {
      return OBJECT_MAPPER.readValue(payload, Map.class);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to parse response: " + e.getMessage(), e);
    }
  }

  private static List<Map<String, Object>> readJsonList(String payload) {
    try {
      return OBJECT_MAPPER.readValue(payload, List.class);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to parse response: " + e.getMessage(), e);
    }
  }

  private static void printMonitorSubmitVerbose(
      String monitorId,
      String identifier,
      String partitionPath,
      long actionTimeSeconds,
      long rangeSeconds) {
    System.out.printf("monitorId=%s%n", monitorId);
    System.out.printf("identifier=%s%n", identifier);
    if (StringUtils.isNotBlank(partitionPath)) {
      System.out.printf("partitionPath=%s%n", partitionPath);
    }
    System.out.printf("actionTimeSeconds=%d%n", actionTimeSeconds);
    System.out.printf("rangeSeconds=%d%n", rangeSeconds);
  }

  private static void printMonitorStatusLine(Map<String, Object> response) {
    String monitorId = String.valueOf(response.get("monitorId"));
    Map<String, Object> detail = getMonitorDetailInfo(response);
    String identifier = String.valueOf(detail.get("tableIdentifier"));
    String partitionPath = String.valueOf(detail.get("partitionPath"));
    String actionTimeSeconds = String.valueOf(detail.get("actionTimeSeconds"));
    String rangeSeconds = String.valueOf(detail.get("rangeSeconds"));
    String state = String.valueOf(detail.get("state"));
    String partitionPart =
        StringUtils.isBlank(partitionPath) || "null".equals(partitionPath)
            ? ""
            : ", partitionPath=" + partitionPath;
    System.out.printf(
        "monitorId=%s, identifier=%s%s, actionTimeSeconds=%s, rangeSeconds=%s, state=%s%n",
        monitorId, identifier, partitionPart, actionTimeSeconds, rangeSeconds, state);
  }

  private static void printMonitorStateLine(Map<String, Object> response) {
    String monitorId = String.valueOf(response.get("monitorId"));
    Map<String, Object> detail = getMonitorDetailInfo(response);
    String state = String.valueOf(detail.get("state"));
    System.out.printf("monitorId=%s, state=%s%n", monitorId, state);
  }

  private static void printMonitorResponseVerbose(Map<String, Object> response) {
    String monitorId = String.valueOf(response.get("monitorId"));
    Map<String, Object> detail = getMonitorDetailInfo(response);
    System.out.printf("monitorId=%s%n", monitorId);
    System.out.printf("identifier=%s%n", String.valueOf(detail.get("tableIdentifier")));
    String partitionPath = String.valueOf(detail.get("partitionPath"));
    if (StringUtils.isNotBlank(partitionPath) && !"null".equals(partitionPath)) {
      System.out.printf("partitionPath=%s%n", partitionPath);
    }
    System.out.printf("actionTimeSeconds=%s%n", String.valueOf(detail.get("actionTimeSeconds")));
    System.out.printf("rangeSeconds=%s%n", String.valueOf(detail.get("rangeSeconds")));
    System.out.printf("state=%s%n", String.valueOf(detail.get("state")));
    System.out.printf("beforeMetrics=%s%n", String.valueOf(detail.get("beforeMetrics")));
    System.out.printf("afterMetrics=%s%n", String.valueOf(detail.get("afterMetrics")));
    Object jobs = response.get("jobMonitorDetailInfoList");
    if (!(jobs instanceof List)) {
      System.out.println("jobs=none");
      return;
    }
    List<?> jobList = (List<?>) jobs;
    if (jobList.isEmpty()) {
      System.out.println("jobs=none");
      return;
    }
    System.out.println("jobs:");
    for (Object job : jobList) {
      Map<?, ?> jobInfo = (Map<?, ?>) job;
      System.out.printf(
          "  - jobIdentifier=%s, state=%s%n",
          String.valueOf(jobInfo.get("jobIdentifier")), String.valueOf(jobInfo.get("state")));
      System.out.printf("    beforeMetrics=%s%n", String.valueOf(jobInfo.get("beforeMetrics")));
      System.out.printf("    afterMetrics=%s%n", String.valueOf(jobInfo.get("afterMetrics")));
    }
  }

  private static Map<String, Object> getMonitorDetailInfo(Map<String, Object> response) {
    Object detail = response.get("tableMonitorDetailInfo");
    if (detail instanceof Map) {
      return (Map<String, Object>) detail;
    }
    throw new IllegalArgumentException("Missing tableMonitorDetailInfo in response");
  }

  private static void printMonitorList(List<Map<String, Object>> response) {
    String[] headers = {
      "monitorId", "identifier", "partitionPath", "actionTimeSeconds", "rangeSeconds"
    };
    int[] widths = Arrays.stream(headers).mapToInt(String::length).toArray();

    String[][] rows = new String[response.size()][headers.length];
    for (int i = 0; i < response.size(); i++) {
      Map<String, Object> item = response.get(i);
      String monitorId = String.valueOf(item.get("monitorId"));
      Map<?, ?> basicInfo = (Map<?, ?>) item.get("monitorBasicInfo");
      String identifier = basicInfo == null ? "" : String.valueOf(basicInfo.get("identifier"));
      String partitionPath =
          basicInfo == null ? "" : String.valueOf(basicInfo.get("partitionPath"));
      String actionTimeSeconds =
          basicInfo == null ? "" : String.valueOf(basicInfo.get("actionTimeSeconds"));
      String rangeSeconds = basicInfo == null ? "" : String.valueOf(basicInfo.get("rangeSeconds"));
      rows[i][0] = monitorId;
      rows[i][1] = identifier;
      rows[i][2] =
          StringUtils.isBlank(partitionPath) || "null".equals(partitionPath) ? "" : partitionPath;
      rows[i][3] = actionTimeSeconds;
      rows[i][4] = rangeSeconds;
      for (int c = 0; c < headers.length; c++) {
        widths[c] = Math.max(widths[c], rows[i][c].length());
      }
    }

    String headerLine = formatRow(headers, widths);
    System.out.println(headerLine);
    System.out.println(formatSeparator(widths));
    for (String[] row : rows) {
      System.out.println(formatRow(row, widths));
    }
  }

  private static String formatRow(String[] values, int[] widths) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < values.length; i++) {
      if (i > 0) {
        builder.append(" | ");
      }
      builder.append(padRight(values[i], widths[i]));
    }
    return builder.toString();
  }

  private static String formatSeparator(int[] widths) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < widths.length; i++) {
      if (i > 0) {
        builder.append("-+-");
      }
      builder.append("-".repeat(widths[i]));
    }
    return builder.toString();
  }

  private static String padRight(String value, int width) {
    if (value == null) {
      value = "";
    }
    if (value.length() >= width) {
      return value;
    }
    return value + " ".repeat(width - value.length());
  }

  enum OptimizerType {
    RECOMMEND_STRATEGY_TYPE,
    UPDATE_STATISTICS,
    APPEND_METRICS,
    MONITOR_METRICS,
    SUBMIT_MONITOR,
    CANCEL_MONITOR,
    GET_MONITOR,
    LIST_MONITORS,
    RUN_MONITOR_SERVICE,
    REGISTER_TABLES,
    LIST_TABLE_METRICS,
    LIST_JOB_METRICS;

    public static String allValues() {
      return Arrays.stream(values())
          .map(Enum::name)
          .map(name -> name.toLowerCase(Locale.ROOT))
          .collect(Collectors.joining(","));
    }

    public static OptimizerType fromString(String rawValue) {
      String upperValue = rawValue.replace('-', '_').toUpperCase(Locale.ROOT);
      return OptimizerType.valueOf(upperValue);
    }
  }
}
