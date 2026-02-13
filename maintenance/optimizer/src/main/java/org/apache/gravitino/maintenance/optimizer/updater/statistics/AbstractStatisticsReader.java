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
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.TableStatisticsBundle;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.StatisticEntryImpl;
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
  public TableStatisticsBundle readTableStatistics(NameIdentifier tableIdentifier) {
    return aggregateTableStatisticsBundle(tableIdentifier);
  }

  @Override
  public Map<NameIdentifier, TableStatisticsBundle> readBulkTableStatistics() {
    return aggregateAllTableStatisticsBundles();
  }

  @Override
  public List<StatisticEntry<?>> readJobStatistics(NameIdentifier jobIdentifier) {
    return toStatisticEntries(aggregateJobStatistics(jobIdentifier).get(jobIdentifier));
  }

  @Override
  public Map<NameIdentifier, List<StatisticEntry<?>>> readBulkJobStatistics() {
    return toIdentifierStatisticEntries(aggregateJobStatistics(null));
  }

  private List<StatisticEntry<?>> toStatisticEntries(Map<String, StatisticValue<?>> statsByName) {
    if (statsByName == null || statsByName.isEmpty()) {
      return ImmutableList.of();
    }

    List<StatisticEntry<?>> statistics = new ArrayList<>();
    statsByName.forEach((name, value) -> statistics.add(new StatisticEntryImpl<>(name, value)));
    return ImmutableList.copyOf(statistics);
  }

  private Map<PartitionPath, List<StatisticEntry<?>>> toPartitionStatisticEntries(
      Map<PartitionPath, Map<String, StatisticValue<?>>> statsByPartition) {
    if (statsByPartition == null || statsByPartition.isEmpty()) {
      return Map.of();
    }

    Map<PartitionPath, List<StatisticEntry<?>>> result = new LinkedHashMap<>();
    statsByPartition.forEach(
        (partitionPath, statsByName) -> result.put(partitionPath, toStatisticEntries(statsByName)));
    return result;
  }

  private Map<NameIdentifier, List<StatisticEntry<?>>> toIdentifierStatisticEntries(
      Map<NameIdentifier, Map<String, StatisticValue<?>>> statsByIdentifier) {
    Map<NameIdentifier, List<StatisticEntry<?>>> result = new LinkedHashMap<>();
    if (statsByIdentifier == null || statsByIdentifier.isEmpty()) {
      return result;
    }

    statsByIdentifier.forEach(
        (identifier, statsByName) -> result.put(identifier, toStatisticEntries(statsByName)));
    return result;
  }

  private Map<NameIdentifier, Map<String, StatisticValue<?>>> aggregateStatistics(
      NameIdentifier targetIdentifier) {
    Map<NameIdentifier, Map<String, StatisticValue<?>>> aggregated = new LinkedHashMap<>();
    visitParsedNodes(
        new StatisticsNodeVisitor() {
          @Override
          public void onTable(JsonNode node) {
            NameIdentifier identifier = parseIdentifier(node.get(IDENTIFIER_FIELD), true);
            if (identifier == null) {
              return;
            }
            if (targetIdentifier != null && !targetIdentifier.equals(identifier)) {
              return;
            }

            Map<String, StatisticValue<?>> statisticsByName =
                aggregated.computeIfAbsent(identifier, k -> new LinkedHashMap<>());
            populateStatistics(node, statisticsByName);
          }
        });

    return aggregated;
  }

  private TableStatisticsBundle aggregateTableStatisticsBundle(NameIdentifier targetIdentifier) {
    if (targetIdentifier == null) {
      return new TableStatisticsBundle(ImmutableList.of(), Map.of());
    }

    Map<String, StatisticValue<?>> tableStatistics = new LinkedHashMap<>();
    Map<PartitionPath, Map<String, StatisticValue<?>>> partitionStatistics = new LinkedHashMap<>();
    visitParsedNodes(
        new StatisticsNodeVisitor() {
          @Override
          public void onTable(JsonNode node) {
            NameIdentifier identifier = parseIdentifier(node.get(IDENTIFIER_FIELD), true);
            if (targetIdentifier.equals(identifier)) {
              populateStatistics(node, tableStatistics);
            }
          }

          @Override
          public void onPartition(JsonNode node) {
            NameIdentifier identifier = parseIdentifier(node.get(IDENTIFIER_FIELD), true);
            if (!targetIdentifier.equals(identifier)) {
              return;
            }

            Optional<PartitionPath> partitionPathOpt =
                parsePartitionPath(node.get(PARTITION_PATH_FIELD));
            if (partitionPathOpt.isEmpty()) {
              return;
            }

            Map<String, StatisticValue<?>> partitionStatsByName =
                partitionStatistics.computeIfAbsent(
                    partitionPathOpt.get(), k -> new LinkedHashMap<>());
            populateStatistics(node, partitionStatsByName);
          }
        });

    return new TableStatisticsBundle(
        toStatisticEntries(tableStatistics), toPartitionStatisticEntries(partitionStatistics));
  }

  private Map<NameIdentifier, TableStatisticsBundle> aggregateAllTableStatisticsBundles() {
    Map<NameIdentifier, Map<String, StatisticValue<?>>> tableStatisticsByIdentifier =
        new LinkedHashMap<>();
    Map<NameIdentifier, Map<PartitionPath, Map<String, StatisticValue<?>>>>
        partitionStatisticsByIdentifier = new LinkedHashMap<>();
    visitParsedNodes(
        new StatisticsNodeVisitor() {
          @Override
          public void onTable(JsonNode node) {
            NameIdentifier identifier = parseIdentifier(node.get(IDENTIFIER_FIELD), true);
            if (identifier == null) {
              return;
            }
            Map<String, StatisticValue<?>> tableStats =
                tableStatisticsByIdentifier.computeIfAbsent(identifier, k -> new LinkedHashMap<>());
            populateStatistics(node, tableStats);
          }

          @Override
          public void onPartition(JsonNode node) {
            NameIdentifier identifier = parseIdentifier(node.get(IDENTIFIER_FIELD), true);
            if (identifier == null) {
              return;
            }

            Optional<PartitionPath> partitionPathOpt =
                parsePartitionPath(node.get(PARTITION_PATH_FIELD));
            if (partitionPathOpt.isEmpty()) {
              return;
            }

            Map<PartitionPath, Map<String, StatisticValue<?>>> partitionStatsByPath =
                partitionStatisticsByIdentifier.computeIfAbsent(
                    identifier, k -> new LinkedHashMap<>());
            Map<String, StatisticValue<?>> partitionStatsByName =
                partitionStatsByPath.computeIfAbsent(
                    partitionPathOpt.get(), k -> new LinkedHashMap<>());
            populateStatistics(node, partitionStatsByName);
          }
        });

    Map<NameIdentifier, TableStatisticsBundle> bundles = new LinkedHashMap<>();
    tableStatisticsByIdentifier.forEach(
        (identifier, tableStats) ->
            bundles.put(
                identifier,
                new TableStatisticsBundle(
                    toStatisticEntries(tableStats),
                    toPartitionStatisticEntries(partitionStatisticsByIdentifier.get(identifier)))));
    partitionStatisticsByIdentifier.forEach(
        (identifier, partitionStats) ->
            bundles.putIfAbsent(
                identifier,
                new TableStatisticsBundle(
                    ImmutableList.of(), toPartitionStatisticEntries(partitionStats))));
    return bundles;
  }

  private Map<NameIdentifier, Map<String, StatisticValue<?>>> aggregateJobStatistics(
      NameIdentifier targetIdentifier) {
    Map<NameIdentifier, Map<String, StatisticValue<?>>> aggregated = new LinkedHashMap<>();
    visitParsedNodes(
        new StatisticsNodeVisitor() {
          @Override
          public void onJob(JsonNode node) {
            NameIdentifier identifier = parseIdentifier(node.get(IDENTIFIER_FIELD), false);
            if (identifier == null) {
              return;
            }
            if (targetIdentifier != null && !targetIdentifier.equals(identifier)) {
              return;
            }

            Map<String, StatisticValue<?>> statisticsByName =
                aggregated.computeIfAbsent(identifier, k -> new LinkedHashMap<>());
            populateStatistics(node, statisticsByName);
          }
        });

    return aggregated;
  }

  protected abstract BufferedReader openReader() throws IOException;

  private void forEachParsedNode(Consumer<JsonNode> nodeConsumer) {
    try (BufferedReader reader = openReader()) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (StringUtils.isBlank(line)) {
          continue;
        }

        JsonNode node = parseJson(line);
        if (node == null) {
          continue;
        }
        nodeConsumer.accept(node);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read statistics", e);
    }
  }

  private void visitParsedNodes(StatisticsNodeVisitor visitor) {
    forEachParsedNode(node -> dispatchNodeByStatisticsType(node, visitor));
  }

  private void dispatchNodeByStatisticsType(JsonNode node, StatisticsNodeVisitor visitor) {
    JsonNode statsType = node.get(STATISTICS_TYPE_FIELD);
    if (statsType == null || !statsType.isTextual()) {
      return;
    }

    String normalizedType = statsType.asText().toLowerCase(Locale.ROOT);
    if (TABLE_STATISTICS_TYPE.equals(normalizedType)) {
      visitor.onTable(node);
    } else if (PARTITION_STATISTICS_TYPE.equals(normalizedType)) {
      visitor.onPartition(node);
    } else if (JOB_STATISTICS_TYPE.equals(normalizedType)) {
      visitor.onJob(node);
    }
  }

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

  private interface StatisticsNodeVisitor {
    default void onTable(JsonNode node) {}

    default void onPartition(JsonNode node) {}

    default void onJob(JsonNode node) {}
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
        LOG.warn("Skip non-numeric textual statistic value for field '{}': {}", fieldName, text);
        return null;
      }
    }

    return null;
  }
}
