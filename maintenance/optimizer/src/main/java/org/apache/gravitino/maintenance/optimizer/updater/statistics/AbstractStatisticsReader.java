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

package org.apache.gravitino.maintenance.optimizer.updater.statistics;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.maintenance.optimizer.updater.StatisticEntryImpl;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * partition schema: { "identifier": "schema.table", "stats-type": "partition", "partition-path":
 * {"p1": "v1", "p2": "v2"}, "stats1":"100"}
 *
 * <p>table schema: { "identifier": "schema.table", "stats-type": "table", "stats1":"100"}
 *
 * <p>job schema: { "identifier": "schema.job", "stats-type": "job", "stats1":"100"}
 *
 * <p>For Iceberg we don't have restrict on partition name, but prefer to use the name in Iceberg
 * transform name.
 *
 * <ul>
 *   <li>identity(col) -> col
 *   <li>year(col) -> col_year
 *   <li>month(col) -> col_month
 *   <li>day(col) / days(col) -> col_day
 *   <li>hour(col) / hours(col) -> col_hour
 *   <li>bucket(N, col) -> col_bucket_N
 *   <li>truncate(W, col) -> col_trunc
 * </ul>
 */
abstract class AbstractStatisticsReader implements StatisticsReader {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractStatisticsReader.class);

  // support table and partition statistics
  static final String STATISTICS_TYPE_FIELD = "stats-type";
  static final String IDENTIFIER_FIELD = "identifier";
  static final String PARTITION_PATH_FIELD = "partition-path";
  static final String TABLE_STATISTICS_TYPE = "table";
  static final String PARTITION_STATISTICS_TYPE = "partition";
  static final String JOB_STATISTICS_TYPE = "job";

  private final String defaultCatalogName;

  AbstractStatisticsReader(String defaultCatalogName) {
    this.defaultCatalogName = defaultCatalogName;
  }

  @Override
  public List<StatisticEntry<?>> readTableStatistics(NameIdentifier tableIdentifier) {
    Map<NameIdentifier, Map<String, StatisticValue<?>>> aggregated =
        aggregateStatistics(tableIdentifier);
    Map<String, StatisticValue<?>> mergedStatistics = aggregated.get(tableIdentifier);
    if (mergedStatistics == null || mergedStatistics.isEmpty()) {
      return ImmutableList.of();
    }

    List<StatisticEntry<?>> statistics = new ArrayList<>();
    mergedStatistics.forEach(
        (name, value) -> statistics.add(new StatisticEntryImpl<>(name, value)));
    return ImmutableList.copyOf(statistics);
  }

  @Override
  public Map<NameIdentifier, List<StatisticEntry<?>>> readAllTableStatistics() {
    Map<NameIdentifier, Map<String, StatisticValue<?>>> aggregated = aggregateStatistics(null);
    Map<NameIdentifier, List<StatisticEntry<?>>> result = new LinkedHashMap<>();
    aggregated.forEach(
        (identifier, statsByName) -> {
          List<StatisticEntry<?>> statistics = new ArrayList<>();
          statsByName.forEach(
              (name, value) -> statistics.add(new StatisticEntryImpl<>(name, value)));
          if (!statistics.isEmpty()) {
            result.put(identifier, ImmutableList.copyOf(statistics));
          } else {
            result.put(identifier, ImmutableList.of());
          }
        });

    return result;
  }

  @Override
  public List<StatisticEntry<?>> readJobStatistics(NameIdentifier jobIdentifier) {
    Map<NameIdentifier, Map<String, StatisticValue<?>>> aggregated =
        aggregateJobStatistics(jobIdentifier);
    Map<String, StatisticValue<?>> mergedStatistics = aggregated.get(jobIdentifier);
    if (mergedStatistics == null || mergedStatistics.isEmpty()) {
      return ImmutableList.of();
    }

    List<StatisticEntry<?>> statistics = new ArrayList<>();
    mergedStatistics.forEach(
        (name, value) -> statistics.add(new StatisticEntryImpl<>(name, value)));
    return ImmutableList.copyOf(statistics);
  }

  @Override
  public Map<NameIdentifier, List<StatisticEntry<?>>> readAllJobStatistics() {
    Map<NameIdentifier, Map<String, StatisticValue<?>>> aggregated = aggregateJobStatistics(null);
    Map<NameIdentifier, List<StatisticEntry<?>>> result = new LinkedHashMap<>();
    aggregated.forEach(
        (identifier, statsByName) -> {
          List<StatisticEntry<?>> statistics = new ArrayList<>();
          statsByName.forEach(
              (name, value) -> statistics.add(new StatisticEntryImpl<>(name, value)));
          if (!statistics.isEmpty()) {
            result.put(identifier, ImmutableList.copyOf(statistics));
          } else {
            result.put(identifier, ImmutableList.of());
          }
        });

    return result;
  }

  @Override
  public Map<PartitionPath, List<StatisticEntry<?>>> readPartitionStatistics(
      NameIdentifier tableIdentifier) {
    Map<NameIdentifier, Map<PartitionPath, Map<String, StatisticValue<?>>>> aggregated =
        aggregatePartitionStatistics(tableIdentifier);
    Map<PartitionPath, Map<String, StatisticValue<?>>> partitionStatistics =
        aggregated.get(tableIdentifier);
    if (partitionStatistics == null || partitionStatistics.isEmpty()) {
      return Map.of();
    }

    Map<PartitionPath, List<StatisticEntry<?>>> result = new LinkedHashMap<>();
    partitionStatistics.forEach(
        (partitionPath, statisticsByName) -> {
          List<StatisticEntry<?>> statistics = new ArrayList<>();
          statisticsByName.forEach(
              (name, value) -> statistics.add(new StatisticEntryImpl<>(name, value)));
          result.put(partitionPath, ImmutableList.copyOf(statistics));
        });
    return result;
  }

  @Override
  public Map<NameIdentifier, Map<PartitionPath, List<StatisticEntry<?>>>>
      readAllPartitionStatistics() {
    Map<NameIdentifier, Map<PartitionPath, Map<String, StatisticValue<?>>>> aggregated =
        aggregatePartitionStatistics(null);
    Map<NameIdentifier, Map<PartitionPath, List<StatisticEntry<?>>>> result = new LinkedHashMap<>();
    aggregated.forEach(
        (identifier, statisticsByPartition) -> {
          Map<PartitionPath, List<StatisticEntry<?>>> perPartition = new LinkedHashMap<>();
          statisticsByPartition.forEach(
              (partitionPath, statisticsByName) -> {
                List<StatisticEntry<?>> statistics = new ArrayList<>();
                statisticsByName.forEach(
                    (name, value) -> statistics.add(new StatisticEntryImpl<>(name, value)));
                perPartition.put(partitionPath, ImmutableList.copyOf(statistics));
              });
          result.put(identifier, perPartition);
        });
    return result;
  }

  private Map<NameIdentifier, Map<String, StatisticValue<?>>> aggregateStatistics(
      NameIdentifier targetIdentifier) {
    Map<NameIdentifier, Map<String, StatisticValue<?>>> aggregated = new LinkedHashMap<>();

    try (BufferedReader reader = openReader()) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (StringUtils.isBlank(line)) {
          continue;
        }

        JsonNode node = parseJson(line);
        if (node == null || !isTableStatistics(node)) {
          continue;
        }

        NameIdentifier identifier = parseIdentifier(node.get(IDENTIFIER_FIELD), true);
        if (identifier == null) {
          continue;
        }
        if (targetIdentifier != null && !targetIdentifier.equals(identifier)) {
          continue;
        }

        Map<String, StatisticValue<?>> statisticsByName =
            aggregated.computeIfAbsent(identifier, k -> new LinkedHashMap<>());
        populateStatistics(node, statisticsByName);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read statistics", e);
    }

    return aggregated;
  }

  private Map<NameIdentifier, Map<String, StatisticValue<?>>> aggregateJobStatistics(
      NameIdentifier targetIdentifier) {
    Map<NameIdentifier, Map<String, StatisticValue<?>>> aggregated = new LinkedHashMap<>();

    try (BufferedReader reader = openReader()) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (StringUtils.isBlank(line)) {
          continue;
        }

        JsonNode node = parseJson(line);
        if (node == null || !isJobStatistics(node)) {
          continue;
        }

        NameIdentifier identifier = parseIdentifier(node.get(IDENTIFIER_FIELD), false);
        if (identifier == null) {
          continue;
        }
        if (targetIdentifier != null && !targetIdentifier.equals(identifier)) {
          continue;
        }

        Map<String, StatisticValue<?>> statisticsByName =
            aggregated.computeIfAbsent(identifier, k -> new LinkedHashMap<>());
        populateStatistics(node, statisticsByName);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read statistics", e);
    }

    return aggregated;
  }

  private Map<NameIdentifier, Map<PartitionPath, Map<String, StatisticValue<?>>>>
      aggregatePartitionStatistics(NameIdentifier targetIdentifier) {
    Map<NameIdentifier, Map<PartitionPath, Map<String, StatisticValue<?>>>> aggregated =
        new LinkedHashMap<>();

    try (BufferedReader reader = openReader()) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (StringUtils.isBlank(line)) {
          continue;
        }

        JsonNode node = parseJson(line);
        if (node == null || !isPartitionStatistics(node)) {
          continue;
        }

        NameIdentifier identifier = parseIdentifier(node.get(IDENTIFIER_FIELD), true);
        if (identifier == null) {
          continue;
        }
        if (targetIdentifier != null && !targetIdentifier.equals(identifier)) {
          continue;
        }

        Optional<PartitionPath> partitionPathOpt =
            parsePartitionPath(node.get(PARTITION_PATH_FIELD));
        if (partitionPathOpt.isEmpty()) {
          continue;
        }
        PartitionPath partitionPath = partitionPathOpt.get();

        Map<PartitionPath, Map<String, StatisticValue<?>>> partitionStatisticsByName =
            aggregated.computeIfAbsent(identifier, k -> new LinkedHashMap<>());
        Map<String, StatisticValue<?>> statisticsByName =
            partitionStatisticsByName.computeIfAbsent(partitionPath, k -> new LinkedHashMap<>());
        populateStatistics(node, statisticsByName);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read statistics", e);
    }

    return aggregated;
  }

  protected abstract BufferedReader openReader() throws IOException;

  private JsonNode parseJson(String line) {
    try {
      return JsonUtils.anyFieldMapper().readTree(line);
    } catch (IOException e) {
      LOG.warn("Skip malformed statistics line: {}", line, e);
      return null;
    }
  }

  private void populateStatistics(JsonNode node, Map<String, StatisticValue<?>> statisticsByName) {
    Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> entry = fields.next();
      String fieldName = entry.getKey();
      if (IDENTIFIER_FIELD.equals(fieldName)
          || STATISTICS_TYPE_FIELD.equals(fieldName)
          || PARTITION_PATH_FIELD.equals(fieldName)) {
        continue;
      }

      StatisticValue<?> value = parseStatisticValue(fieldName, entry.getValue());
      if (value != null) {
        statisticsByName.put(fieldName, value);
      }
    }
  }

  private boolean isTableStatistics(JsonNode node) {
    if (node == null) {
      return false;
    }
    JsonNode statsType = node.get(STATISTICS_TYPE_FIELD);
    return statsType != null
        && statsType.isTextual()
        && TABLE_STATISTICS_TYPE.equalsIgnoreCase(statsType.asText());
  }

  private boolean isPartitionStatistics(JsonNode node) {
    if (node == null) {
      return false;
    }
    JsonNode statsType = node.get(STATISTICS_TYPE_FIELD);
    return statsType != null
        && statsType.isTextual()
        && PARTITION_STATISTICS_TYPE.equalsIgnoreCase(statsType.asText());
  }

  private boolean isJobStatistics(JsonNode node) {
    if (node == null) {
      return false;
    }
    JsonNode statsType = node.get(STATISTICS_TYPE_FIELD);
    return statsType != null
        && statsType.isTextual()
        && JOB_STATISTICS_TYPE.equalsIgnoreCase(statsType.asText());
  }

  private NameIdentifier parseIdentifier(JsonNode identifierNode, boolean applyDefaultCatalog) {
    if (identifierNode == null || identifierNode.isNull() || !identifierNode.isTextual()) {
      return null;
    }
    try {
      NameIdentifier parsed = NameIdentifier.parse(identifierNode.asText());
      int levels = parsed.namespace().levels().length;
      if (levels == 0) {
        return parsed;
      } else if (levels == 1) {
        if (applyDefaultCatalog && StringUtils.isNotBlank(defaultCatalogName)) {
          return NameIdentifier.of(
              defaultCatalogName, parsed.namespace().levels()[0], parsed.name());
        }
        return parsed;
      } else if (levels == 2) {
        return parsed;
      } else {
        return null;
      }
    } catch (Exception e) {
      LOG.warn("Skip line with invalid identifier: {}", identifierNode.asText());
      return null;
    }
  }

  private Optional<PartitionPath> parsePartitionPath(JsonNode partitionPathNode) {
    if (partitionPathNode == null || !partitionPathNode.isObject()) {
      return Optional.empty();
    }

    List<PartitionEntry> entries = new ArrayList<>();
    Iterator<Map.Entry<String, JsonNode>> iterator = partitionPathNode.fields();
    while (iterator.hasNext()) {
      Map.Entry<String, JsonNode> entry = iterator.next();
      JsonNode valueNode = entry.getValue();
      if (valueNode == null || valueNode.isNull() || !valueNode.isTextual()) {
        return Optional.empty();
      }
      entries.add(new PartitionEntryImpl(entry.getKey(), valueNode.asText()));
    }

    if (entries.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(PartitionPath.of(entries));
  }

  /**
   * Parse metric values as numeric statistics only.
   *
   * <p>Non-numeric textual values are skipped and logged.
   */
  private StatisticValue<?> parseStatisticValue(String fieldName, JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }

    if (node.isNumber()) {
      return node.isIntegralNumber()
          ? StatisticValues.longValue(node.longValue())
          : StatisticValues.doubleValue(node.doubleValue());
    }

    if (node.isTextual()) {
      String text = node.asText();
      if (StringUtils.isBlank(text)) {
        return null;
      }

      try {
        long longValue = Long.parseLong(text);
        return StatisticValues.longValue(longValue);
      } catch (NumberFormatException e) {
        // Ignore and try parsing as double
      }

      try {
        double doubleValue = Double.parseDouble(text);
        return StatisticValues.doubleValue(doubleValue);
      } catch (NumberFormatException e) {
        LOG.warn(
            "Skip non-numeric textual statistic value for field '{}': {}",
            fieldName,
            text);
        return null;
      }
    }

    return null;
  }
}
