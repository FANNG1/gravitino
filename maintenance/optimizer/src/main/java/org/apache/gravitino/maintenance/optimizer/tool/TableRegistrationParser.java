/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.maintenance.optimizer.tool;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergTable;
import org.apache.gravitino.catalog.lakehouse.iceberg.converter.ConvertUtil;
import org.apache.gravitino.catalog.lakehouse.iceberg.converter.FromIcebergPartitionSpec;
import org.apache.gravitino.catalog.lakehouse.iceberg.converter.FromIcebergSortOrder;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;

/**
 * Parses JSON-per-line input rows into Gravitino-ready table registration requests.
 *
 * <p>Each line must contain:
 *
 * <ul>
 *   <li>{@code identifier}: catalog.schema.table or schema.table (catalog falls back to default)
 *   <li>{@code tableMetadata}: Iceberg TableMetadata JSON object OR
 *   <li>{@code schema}, {@code partition_specs}, {@code sort_orders}, {@code default_spec_id},
 *       {@code default_sort_order_id}: Iceberg schema/spec/sort JSON fragments
 * </ul>
 */
public class TableRegistrationParser {
  private static final String IDENTIFIER_KEY = "identifier";
  private static final String TABLE_METADATA_KEY = "tableMetadata";
  private static final String TABLE_METADATA_SNAKE_KEY = "table_metadata";
  private static final String SCHEMA_KEY = "schema";
  private static final String PARTITION_SPECS_KEY = "partition_specs";
  private static final String PARTITION_SPEC_KEY = "partitionSpec";
  private static final String PARTITION_SPECS_CAMEL_KEY = "partitionSpecs";
  private static final String SORT_ORDERS_KEY = "sort_orders";
  private static final String SORT_ORDERS_CAMEL_KEY = "sortOrders";
  private static final String DEFAULT_SPEC_ID_KEY = "defaultSpecId";
  private static final String DEFAULT_SPEC_ID_SNAKE_KEY = "default_spec_id";
  private static final String DEFAULT_SORT_ORDER_ID_KEY = "defaultSortOrderId";
  private static final String DEFAULT_SORT_ORDER_ID_SNAKE_KEY = "default_sort_order_id";

  private final ObjectMapper mapper = new ObjectMapper();
  private final String defaultCatalogName;

  public TableRegistrationParser(String defaultCatalogName) {
    this.defaultCatalogName = defaultCatalogName;
  }

  public TableRegistrationRequest parse(String jsonLine) throws IOException {
    JsonNode root = mapper.readTree(jsonLine);
    String identifierText = requiredText(root, IDENTIFIER_KEY);

    NameIdentifier parsedIdentifier;
    try {
      parsedIdentifier = NameIdentifier.parse(identifierText);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format("Invalid identifier '%s': %s", identifierText, e.getMessage()), e);
    }

    IcebergTable icebergTable = parseIcebergTable(root, parsedIdentifier);
    IdentifierParts identifierParts = normalizeIdentifier(parsedIdentifier);

    return new TableRegistrationRequest(
        identifierText, identifierParts.catalogName, identifierParts.tableIdentifier, icebergTable);
  }

  private IcebergTable parseIcebergTable(JsonNode root, NameIdentifier identifier) {
    JsonNode tableMetadataNode =
        firstNonNull(root.get(TABLE_METADATA_KEY), root.get(TABLE_METADATA_SNAKE_KEY));
    if (tableMetadataNode != null) {
      TableMetadata tableMetadata = TableMetadataParser.fromJson(tableMetadataNode);
      return IcebergTable.fromIcebergTable(tableMetadata, identifier.name());
    }

    return parseFromSchemaAndSpecs(root, identifier);
  }

  private IcebergTable parseFromSchemaAndSpecs(JsonNode root, NameIdentifier identifier) {
    JsonNode schemaNode = firstNonNull(root.get(SCHEMA_KEY));
    Preconditions.checkArgument(schemaNode != null, "Missing schema field");
    Schema schema = SchemaParser.fromJson(schemaNode);

    List<PartitionSpec> partitionSpecs = parsePartitionSpecs(root, schema);
    int defaultSpecId = requiredInt(root, DEFAULT_SPEC_ID_KEY, DEFAULT_SPEC_ID_SNAKE_KEY);
    PartitionSpec defaultSpec =
        partitionSpecs.stream()
            .filter(spec -> spec.specId() == defaultSpecId)
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "Default partition spec %d not found in partition_specs",
                            defaultSpecId)));

    List<SortOrder> sortOrders = parseSortOrders(root, schema);
    int defaultSortOrderId =
        requiredInt(root, DEFAULT_SORT_ORDER_ID_KEY, DEFAULT_SORT_ORDER_ID_SNAKE_KEY);
    SortOrder defaultSortOrder =
        selectDefaultSortOrder(sortOrders, defaultSortOrderId)
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "Default sort order %d not found in sort_orders",
                            defaultSortOrderId)));

    Transform[] partitions = FromIcebergPartitionSpec.fromPartitionSpec(defaultSpec, schema);
    org.apache.gravitino.rel.expressions.sorts.SortOrder[] gravitinoSortOrders =
        FromIcebergSortOrder.fromSortOrder(defaultSortOrder);

    return IcebergTable.builder()
        .withName(identifier.name())
        .withColumns(
            schema.columns().stream()
                .map(ConvertUtil::fromNestedField)
                .toArray(org.apache.gravitino.catalog.lakehouse.iceberg.IcebergColumn[]::new))
        .withAuditInfo(AuditInfo.EMPTY)
        .withPartitioning(partitions)
        .withSortOrders(gravitinoSortOrders)
        .withDistribution(Distributions.NONE)
        .build();
  }

  private List<PartitionSpec> parsePartitionSpecs(JsonNode root, Schema schema) {
    JsonNode partitionSpecsNode =
        firstNonNull(
            root.get(PARTITION_SPECS_KEY),
            root.get(PARTITION_SPEC_KEY),
            root.get(PARTITION_SPECS_CAMEL_KEY));
    Preconditions.checkArgument(
        partitionSpecsNode != null, "Missing partition_specs or partitionSpec field");

    List<PartitionSpec> specs = new ArrayList<>();
    if (partitionSpecsNode.isArray()) {
      partitionSpecsNode.forEach(node -> specs.add(PartitionSpecParser.fromJson(schema, node)));
    } else {
      specs.add(PartitionSpecParser.fromJson(schema, partitionSpecsNode));
    }

    Preconditions.checkArgument(!specs.isEmpty(), "partition_specs cannot be empty");
    return specs;
  }

  private List<SortOrder> parseSortOrders(JsonNode root, Schema schema) {
    JsonNode sortOrdersNode =
        firstNonNull(root.get(SORT_ORDERS_KEY), root.get(SORT_ORDERS_CAMEL_KEY));
    Preconditions.checkArgument(sortOrdersNode != null, "Missing sort_orders field");

    List<SortOrder> sortOrders = new ArrayList<>();
    if (sortOrdersNode.isArray()) {
      sortOrdersNode.forEach(node -> sortOrders.add(SortOrderParser.fromJson(schema, node)));
    } else {
      sortOrders.add(SortOrderParser.fromJson(schema, sortOrdersNode));
    }

    return sortOrders;
  }

  private Optional<SortOrder> selectDefaultSortOrder(
      List<SortOrder> sortOrders, int defaultSortOrderId) {
    Optional<SortOrder> matched =
        sortOrders.stream().filter(order -> order.orderId() == defaultSortOrderId).findFirst();
    if (matched.isPresent()) {
      return matched;
    }

    if (sortOrders.isEmpty() && defaultSortOrderId == SortOrder.unsorted().orderId()) {
      return Optional.of(SortOrder.unsorted());
    }
    return Optional.empty();
  }

  private IdentifierParts normalizeIdentifier(NameIdentifier identifier) {
    Preconditions.checkArgument(identifier.namespace() != null, "Identifier must include a schema");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(defaultCatalogName), "Default catalog is required");
    String[] levels = identifier.namespace().levels();
    Preconditions.checkArgument(
        levels.length >= 1 && levels.length <= 2,
        "Identifier must be schema.table or catalog.schema.table");

    String schemaName = levels.length == 1 ? levels[0] : levels[1];
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, identifier.name());
    return new IdentifierParts(defaultCatalogName, tableIdentifier);
  }

  private JsonNode firstNonNull(JsonNode... nodes) {
    for (JsonNode node : nodes) {
      if (node != null) {
        return node;
      }
    }
    return null;
  }

  private String requiredText(JsonNode node, String fieldName) {
    String text = textOrNull(node.get(fieldName));
    Preconditions.checkArgument(
        StringUtils.isNotBlank(text), "Missing required field '%s'", fieldName);
    return text;
  }

  private String textOrNull(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    return node.isTextual() ? node.asText() : null;
  }

  private int requiredInt(JsonNode root, String camelCaseKey, String snakeCaseKey) {
    JsonNode valueNode = firstNonNull(root.get(camelCaseKey), root.get(snakeCaseKey));
    Preconditions.checkArgument(
        valueNode != null, "Missing required field '%s' or '%s'", camelCaseKey, snakeCaseKey);
    return parseInt(valueNode, camelCaseKey);
  }

  private int parseInt(JsonNode node, String fieldName) {
    if (node.isInt()) {
      return node.intValue();
    }
    if (node.isTextual()) {
      try {
        return Integer.parseInt(node.textValue());
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            String.format(Locale.ROOT, "Invalid integer for field '%s'", fieldName), e);
      }
    }
    throw new IllegalArgumentException(
        String.format(Locale.ROOT, "Field '%s' must be an integer", fieldName));
  }

  public static class TableRegistrationRequest {
    private final String originalIdentifier;
    private final String catalogName;
    private final NameIdentifier tableIdentifier;
    private final IcebergTable icebergTable;

    public TableRegistrationRequest(
        String originalIdentifier,
        String catalogName,
        NameIdentifier tableIdentifier,
        IcebergTable icebergTable) {
      this.originalIdentifier = originalIdentifier;
      this.catalogName = catalogName;
      this.tableIdentifier = tableIdentifier;
      this.icebergTable = icebergTable;
    }

    public String originalIdentifier() {
      return originalIdentifier;
    }

    public String catalogName() {
      return catalogName;
    }

    public NameIdentifier tableIdentifier() {
      return tableIdentifier;
    }

    public IcebergTable icebergTable() {
      return icebergTable;
    }
  }

  private static class IdentifierParts {
    private final String catalogName;
    private final NameIdentifier tableIdentifier;

    IdentifierParts(String catalogName, NameIdentifier tableIdentifier) {
      this.catalogName = catalogName;
      this.tableIdentifier = tableIdentifier;
    }
  }
}
