/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the
 * Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License.  You may obtain a copy
 * of the License at
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.tool.TableRegistrationParser.TableRegistrationRequest;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.sorts.NullOrdering;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.Test;

class TestTableRegistrationParser {

  @Test
  void parseWithCatalogAndPartitions() throws Exception {
    TableRegistrationParser parser = new TableRegistrationParser("default_catalog");

    TableRegistrationRequest request = parser.parse(sampleLineWithCatalog());

    assertEquals("default_catalog", request.catalogName());
    assertEquals(NameIdentifier.of("test_schema", "test_table"), request.tableIdentifier());

    Column[] columns = request.icebergTable().columns();
    assertEquals(4, columns.length);
    assertEquals(Types.LongType.get(), columns[0].dataType());
    assertTrue(columns[1].dataType() instanceof Types.TimestampType);
    assertTrue(((Types.TimestampType) columns[1].dataType()).hasTimeZone());
    assertEquals(Types.StringType.get(), columns[2].dataType());

    Transform[] partitions = request.icebergTable().partitioning();
    assertEquals(2, partitions.length);
    assertTrue(partitions[0] instanceof Transforms.IdentityTransform);
    assertTrue(partitions[1] instanceof Transforms.BucketTransform);
  }

  @Test
  void parseUsesDefaultCatalogWhenMissing() throws Exception {
    TableRegistrationParser parser = new TableRegistrationParser("iceberg");

    TableRegistrationRequest request = parser.parse(sampleLineWithoutCatalog());

    assertEquals("iceberg", request.catalogName());
    assertEquals(NameIdentifier.of("test_schema", "test_table"), request.tableIdentifier());
    assertNotNull(request.icebergTable().columns());
  }

  @Test
  void parseSchemaAndPartitionSpecsWithoutTableMetadata() throws Exception {
    TableRegistrationParser parser = new TableRegistrationParser("iceberg");
    Schema schema =
        new Schema(
            NestedField.optional(1, "id", org.apache.iceberg.types.Types.LongType.get()),
            NestedField.optional(2, "ts", org.apache.iceberg.types.Types.TimestampType.withZone()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).withSpecId(10).identity("id").build();
    SortOrder sortOrder =
        SortOrder.builderFor(schema).withOrderId(5).asc("ts", NullOrder.NULLS_LAST).build();

    String line =
        "{"
            + "\"identifier\":\"test_schema.test_table\","
            + "\"schema\":"
            + SchemaParser.toJson(schema)
            + ","
            + "\"partition_specs\":["
            + PartitionSpecParser.toJson(spec)
            + "],"
            + "\"sort_orders\":["
            + SortOrderParser.toJson(sortOrder)
            + "],"
            + "\"default_spec_id\":10,"
            + "\"default_sort_order_id\":5"
            + "}";

    TableRegistrationRequest request = parser.parse(line);

    assertEquals("iceberg", request.catalogName());
    assertEquals(NameIdentifier.of("test_schema", "test_table"), request.tableIdentifier());
    Column[] columns = request.icebergTable().columns();
    assertEquals(2, columns.length);
    assertEquals(Types.LongType.get(), columns[0].dataType());

    Transform[] partitions = request.icebergTable().partitioning();
    assertEquals(1, partitions.length);
    assertTrue(partitions[0] instanceof Transforms.IdentityTransform);

    org.apache.gravitino.rel.expressions.sorts.SortOrder[] sortOrders =
        request.icebergTable().sortOrder();
    assertEquals(1, sortOrders.length);
    assertEquals(SortDirection.ASCENDING, sortOrders[0].direction());
  }

  @Test
  void parseDocExampleJsonLines() throws Exception {
    TableRegistrationParser parser = new TableRegistrationParser("generic");

    String table1Line =
        "{\"identifier\":\"generic.db.table1\","
            + "\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"},{\"id\":2,\"name\":\"ts\",\"required\":false,\"type\":\"timestamp\",\"original-type\":\"timestamp with time zone\"},{\"id\":3,\"name\":\"country\",\"required\":false,\"type\":\"string\"}]},"
            + "\"partition_specs\":[{\"spec-id\":0,\"fields\":[{\"source-id\":3,\"field-id\":1000,\"name\":\"country\",\"transform\":\"identity\"}]}],"
            + "\"sort_orders\":[{\"order-id\":1,\"fields\":[{\"transform\":\"identity\",\"source-id\":2,\"direction\":\"asc\",\"null-order\":\"nulls-last\"}]}],"
            + "\"default_spec_id\":0,"
            + "\"default_sort_order_id\":1}";

    String table2Line =
        "{\"identifier\":\"db.table2\","
            + "\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"},{\"id\":2,\"name\":\"bucket_col\",\"required\":false,\"type\":\"string\"}]},"
            + "\"partition_specs\":[{\"spec-id\":0,\"fields\":[{\"source-id\":2,\"field-id\":2000,\"name\":\"bucket_col_bucket_8\",\"transform\":\"bucket[8]\"}]}],"
            + "\"sort_orders\":[{\"order-id\":1,\"fields\":[{\"transform\":\"identity\",\"source-id\":2,\"direction\":\"asc\",\"null-order\":\"nulls-first\"}]}],"
            + "\"default_spec_id\":0,"
            + "\"default_sort_order_id\":1}";

    TableRegistrationRequest table1 = parser.parse(table1Line);
    TableRegistrationRequest table2 = parser.parse(table2Line);

    assertEquals("generic", table1.catalogName());
    assertEquals(NameIdentifier.of("db", "table1"), table1.tableIdentifier());
    assertEquals(3, table1.icebergTable().columns().length);
    assertEquals(1, table1.icebergTable().partitioning().length);
    assertEquals(1, table1.icebergTable().sortOrder().length);
    assertEquals(SortDirection.ASCENDING, table1.icebergTable().sortOrder()[0].direction());
    assertEquals(NullOrdering.NULLS_LAST, table1.icebergTable().sortOrder()[0].nullOrdering());

    assertEquals("generic", table2.catalogName());
    assertEquals(NameIdentifier.of("db", "table2"), table2.tableIdentifier());
    assertEquals(2, table2.icebergTable().columns().length);
    assertEquals(1, table2.icebergTable().partitioning().length);
    assertEquals(1, table2.icebergTable().sortOrder().length);
    assertEquals(SortDirection.ASCENDING, table2.icebergTable().sortOrder()[0].direction());
    assertEquals(NullOrdering.NULLS_FIRST, table2.icebergTable().sortOrder()[0].nullOrdering());
  }

  @Test
  void tableMetadataTakesPrecedenceOverSchemaFragments() throws Exception {
    TableRegistrationParser parser = new TableRegistrationParser("default_catalog");

    Schema fallbackSchema =
        new Schema(
            NestedField.optional(
                1, "only_in_schema", org.apache.iceberg.types.Types.StringType.get()));
    PartitionSpec fallbackSpec =
        PartitionSpec.builderFor(fallbackSchema).identity("only_in_schema").build();
    SortOrder fallbackSort = SortOrder.unsorted();

    String line =
        "{"
            + "\"identifier\":\"iceberg.test_schema.test_table\","
            + "\"tableMetadata\":"
            + TableMetadataParser.toJson(sampleMetadata())
            + ","
            + "\"schema\":"
            + SchemaParser.toJson(fallbackSchema)
            + ","
            + "\"partition_specs\":["
            + PartitionSpecParser.toJson(fallbackSpec)
            + "],"
            + "\"sort_orders\":["
            + SortOrderParser.toJson(fallbackSort)
            + "],"
            + "\"default_spec_id\":0,"
            + "\"default_sort_order_id\":0"
            + "}";

    TableRegistrationRequest request = parser.parse(line);

    assertEquals("default_catalog", request.catalogName());
    // columns should come from tableMetadata, not fallback schema
    assertEquals(4, request.icebergTable().columns().length);
    assertTrue(
        StringUtils.containsIgnoreCase(request.icebergTable().properties().get("location"), "tmp"));
  }

  @Test
  void unsupportedTransformThrows() {
    TableRegistrationParser parser = new TableRegistrationParser("iceberg");
    Schema schema =
        new Schema(NestedField.optional(1, "id", org.apache.iceberg.types.Types.LongType.get()));
    String line =
        "{"
            + "\"identifier\":\"iceberg.test_schema.test_table\","
            + "\"schema\":"
            + SchemaParser.toJson(schema)
            + ","
            + "\"partition_specs\":[{\"spec-id\":0,\"fields\":[{\"name\":\"id\",\"transform\":\"unknown\",\"source-id\":1,\"field-id\":1000}]}],"
            + "\"sort_orders\":[],"
            + "\"default_spec_id\":0,"
            + "\"default_sort_order_id\":0"
            + "}";

    assertThrows(UnsupportedOperationException.class, () -> parser.parse(line));
  }

  private String sampleLineWithCatalog() {
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            sampleSchema(),
            PartitionSpec.builderFor(sampleSchema())
                .identity("user_id")
                .bucket("payload", 2)
                .build(),
            SortOrder.unsorted(),
            "file:///tmp/location",
            Map.of("provider", "iceberg"));
    return "{"
        + "\"identifier\":\"iceberg.test_schema.test_table\","
        + "\"tableMetadata\":"
        + TableMetadataParser.toJson(metadata)
        + "}";
  }

  private String sampleLineWithoutCatalog() {
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            new Schema(
                org.apache.iceberg.types.Types.NestedField.optional(
                    1, "id", org.apache.iceberg.types.Types.LongType.get())),
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            "file:///tmp/location2",
            Map.of("provider", "iceberg"));
    return "{"
        + "\"identifier\":\"test_schema.test_table\","
        + "\"tableMetadata\":"
        + TableMetadataParser.toJson(metadata)
        + "}";
  }

  private TableMetadata sampleMetadata() {
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            new Schema(
                org.apache.iceberg.types.Types.NestedField.optional(
                    1, "id", org.apache.iceberg.types.Types.LongType.get()),
                org.apache.iceberg.types.Types.NestedField.optional(
                    2, "ts", org.apache.iceberg.types.Types.TimestampType.withZone()),
                org.apache.iceberg.types.Types.NestedField.optional(
                    3, "user_id", org.apache.iceberg.types.Types.StringType.get()),
                org.apache.iceberg.types.Types.NestedField.optional(
                    4, "payload", org.apache.iceberg.types.Types.StringType.get())),
            PartitionSpec.builderFor(
                    new Schema(
                        org.apache.iceberg.types.Types.NestedField.optional(
                            1, "id", org.apache.iceberg.types.Types.LongType.get()),
                        org.apache.iceberg.types.Types.NestedField.optional(
                            3, "user_id", org.apache.iceberg.types.Types.StringType.get()),
                        org.apache.iceberg.types.Types.NestedField.optional(
                            4, "payload", org.apache.iceberg.types.Types.StringType.get())))
                .identity("user_id")
                .bucket("payload", 2)
                .build(),
            SortOrder.unsorted(),
            "file:///tmp/location",
            Map.of("provider", "iceberg"));
    return metadata;
  }

  private Schema sampleSchema() {
    return new Schema(
        org.apache.iceberg.types.Types.NestedField.optional(
            1, "id", org.apache.iceberg.types.Types.LongType.get()),
        org.apache.iceberg.types.Types.NestedField.optional(
            2, "ts", org.apache.iceberg.types.Types.TimestampType.withZone()),
        org.apache.iceberg.types.Types.NestedField.optional(
            3, "user_id", org.apache.iceberg.types.Types.StringType.get()),
        org.apache.iceberg.types.Types.NestedField.optional(
            4, "payload", org.apache.iceberg.types.Types.StringType.get()));
  }
}
