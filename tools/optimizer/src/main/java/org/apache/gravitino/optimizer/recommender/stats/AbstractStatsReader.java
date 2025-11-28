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

package org.apache.gravitino.optimizer.recommender.stats;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.optimizer.updater.StatisticEntryImpl;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractStatsReader implements StatsReader {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractStatsReader.class);

  static final String STATS_TYPE_FIELD = "stats-type";
  static final String IDENTIFIER_FIELD = "identifier";
  static final String TABLE_STATS_TYPE = "table";

  private final String defaultCatalogName;

  AbstractStatsReader(String defaultCatalogName) {
    this.defaultCatalogName = defaultCatalogName;
  }

  @Override
  public List<StatisticEntry<?>> readTableStats(NameIdentifier tableIdentifier) {
    Map<NameIdentifier, Map<String, StatisticValue<?>>> aggregated =
        aggregateStats(tableIdentifier);
    Map<String, StatisticValue<?>> mergedStats = aggregated.get(tableIdentifier);
    if (mergedStats == null || mergedStats.isEmpty()) {
      return ImmutableList.of();
    }

    List<StatisticEntry<?>> statistics = new ArrayList<>();
    mergedStats.forEach((name, value) -> statistics.add(new StatisticEntryImpl<>(name, value)));
    return ImmutableList.copyOf(statistics);
  }

  @Override
  public Map<NameIdentifier, List<StatisticEntry<?>>> readAllTableStats() {
    Map<NameIdentifier, Map<String, StatisticValue<?>>> aggregated = aggregateStats(null);
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

  private Map<NameIdentifier, Map<String, StatisticValue<?>>> aggregateStats(
      NameIdentifier targetIdentifier) {
    Map<NameIdentifier, Map<String, StatisticValue<?>>> aggregated = new LinkedHashMap<>();

    try (BufferedReader reader = openReader()) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (StringUtils.isBlank(line)) {
          continue;
        }

        JsonNode node = parseJson(line);
        if (node == null || !isTableStats(node)) {
          continue;
        }

        NameIdentifier identifier = parseIdentifier(node.get(IDENTIFIER_FIELD));
        if (identifier == null) {
          continue;
        }
        if (targetIdentifier != null && !targetIdentifier.equals(identifier)) {
          continue;
        }

        Map<String, StatisticValue<?>> statsByName =
            aggregated.computeIfAbsent(identifier, k -> new LinkedHashMap<>());
        populateStats(node, statsByName);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read stats", e);
    }

    return aggregated;
  }

  protected abstract BufferedReader openReader() throws IOException;

  private JsonNode parseJson(String line) {
    try {
      return JsonUtils.anyFieldMapper().readTree(line);
    } catch (IOException e) {
      LOG.warn("Skip malformed stats line: {}", line, e);
      return null;
    }
  }

  private void populateStats(JsonNode node, Map<String, StatisticValue<?>> statsByName) {
    Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> entry = fields.next();
      String fieldName = entry.getKey();
      if (IDENTIFIER_FIELD.equals(fieldName) || STATS_TYPE_FIELD.equals(fieldName)) {
        continue;
      }

      StatisticValue<?> value = parseStatisticValue(entry.getValue());
      if (value != null) {
        statsByName.put(fieldName, value);
      }
    }
  }

  private boolean isTableStats(JsonNode node) {
    if (node == null) {
      return false;
    }
    JsonNode statsType = node.get(STATS_TYPE_FIELD);
    return statsType != null
        && statsType.isTextual()
        && TABLE_STATS_TYPE.equalsIgnoreCase(statsType.asText());
  }

  private NameIdentifier parseIdentifier(JsonNode identifierNode) {
    if (identifierNode == null || identifierNode.isNull() || !identifierNode.isTextual()) {
      return null;
    }
    try {
      NameIdentifier parsed = NameIdentifier.parse(identifierNode.asText());
      int levels = parsed.namespace().levels().length;
      if (levels == 1 && StringUtils.isNotBlank(defaultCatalogName)) {
        return NameIdentifier.of(defaultCatalogName, parsed.namespace().levels()[0], parsed.name());
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

  private StatisticValue<?> parseStatisticValue(JsonNode node) {
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
        return null;
      }
    }

    return null;
  }
}
