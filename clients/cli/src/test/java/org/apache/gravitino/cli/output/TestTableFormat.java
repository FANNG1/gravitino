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

package org.apache.gravitino.cli.output;

import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;
import static org.apache.gravitino.rel.expressions.NamedReference.field;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Metalake;
import org.apache.gravitino.Schema;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.outputs.Column;
import org.apache.gravitino.cli.outputs.TableFormat;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.tag.Tag;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

public class TestTableFormat {
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;

  @BeforeEach
  void setUp() {
    System.setOut(new PrintStream(outContent));
    System.setErr(new PrintStream(errContent));
  }

  @AfterEach
  public void restoreStreams() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  void testCreateDefaultTableFormat() {
    CommandContext mockContext = getMockContext();

    Column columnA = new Column(mockContext, "metalake");
    Column columnB = new Column(mockContext, "comment");

    columnA.addCell("cell1").addCell("cell2").addCell("cell3");
    columnB.addCell("cell4").addCell("cell5").addCell("cell6");

    TableFormat<String> tableFormat =
        new TableFormat<String>(mockContext) {
          @Override
          public String getOutput(String entity) {
            return null;
          }
        };

    String outputString = tableFormat.getTableFormat(columnA, columnB).trim();
    Assertions.assertEquals(
        "+----------+---------+\n"
            + "| Metalake | Comment |\n"
            + "+----------+---------+\n"
            + "| cell1    | cell4   |\n"
            + "| cell2    | cell5   |\n"
            + "| cell3    | cell6   |\n"
            + "+----------+---------+",
        outputString);
  }

  @Test
  void testTitleWithLeftAlign() {
    CommandContext mockContext = getMockContext();

    Column columnA =
        new Column(
            mockContext, "metalake", Column.HorizontalAlign.LEFT, Column.HorizontalAlign.CENTER);
    Column columnB =
        new Column(
            mockContext, "comment", Column.HorizontalAlign.LEFT, Column.HorizontalAlign.CENTER);

    columnA.addCell("cell1").addCell("cell2").addCell("cell3").addCell("very long cell");
    columnB.addCell("cell4").addCell("cell5").addCell("cell6").addCell("very long cell");

    TableFormat<String> tableFormat =
        new TableFormat<String>(mockContext) {
          @Override
          public String getOutput(String entity) {
            return null;
          }
        };

    String outputString = tableFormat.getTableFormat(columnA, columnB).trim();
    Assertions.assertEquals(
        "+----------------+----------------+\n"
            + "| Metalake       | Comment        |\n"
            + "+----------------+----------------+\n"
            + "|     cell1      |     cell4      |\n"
            + "|     cell2      |     cell5      |\n"
            + "|     cell3      |     cell6      |\n"
            + "| very long cell | very long cell |\n"
            + "+----------------+----------------+",
        outputString);
  }

  @Test
  void testTitleWithRightAlign() {
    CommandContext mockContext = getMockContext();

    Column columnA =
        new Column(
            mockContext, "metalake", Column.HorizontalAlign.RIGHT, Column.HorizontalAlign.CENTER);
    Column columnB =
        new Column(
            mockContext, "comment", Column.HorizontalAlign.RIGHT, Column.HorizontalAlign.CENTER);

    columnA.addCell("cell1").addCell("cell2").addCell("cell3").addCell("very long cell");
    columnB.addCell("cell4").addCell("cell5").addCell("cell6").addCell("very long cell");

    TableFormat<String> tableFormat =
        new TableFormat<String>(mockContext) {
          @Override
          public String getOutput(String entity) {
            return null;
          }
        };

    String outputString = tableFormat.getTableFormat(columnA, columnB).trim();
    Assertions.assertEquals(
        "+----------------+----------------+\n"
            + "|       Metalake |        Comment |\n"
            + "+----------------+----------------+\n"
            + "|     cell1      |     cell4      |\n"
            + "|     cell2      |     cell5      |\n"
            + "|     cell3      |     cell6      |\n"
            + "| very long cell | very long cell |\n"
            + "+----------------+----------------+",
        outputString);
  }

  @Test
  void testDataWithCenterAlign() {
    CommandContext mockContext = getMockContext();

    Column columnA =
        new Column(
            mockContext, "metalake", Column.HorizontalAlign.CENTER, Column.HorizontalAlign.CENTER);
    Column columnB =
        new Column(
            mockContext, "comment", Column.HorizontalAlign.CENTER, Column.HorizontalAlign.CENTER);

    columnA.addCell("cell1").addCell("cell2").addCell("cell3").addCell("very long cell");
    columnB.addCell("cell4").addCell("cell5").addCell("cell6").addCell("very long cell");

    TableFormat<String> tableFormat =
        new TableFormat<String>(mockContext) {
          @Override
          public String getOutput(String entity) {
            return null;
          }
        };

    String outputString = tableFormat.getTableFormat(columnA, columnB).trim();
    Assertions.assertEquals(
        "+----------------+----------------+\n"
            + "|    Metalake    |    Comment     |\n"
            + "+----------------+----------------+\n"
            + "|     cell1      |     cell4      |\n"
            + "|     cell2      |     cell5      |\n"
            + "|     cell3      |     cell6      |\n"
            + "| very long cell | very long cell |\n"
            + "+----------------+----------------+",
        outputString);
  }

  @Test
  void testDataWithRightAlign() {
    CommandContext mockContext = getMockContext();

    Column columnA =
        new Column(
            mockContext, "metalake", Column.HorizontalAlign.CENTER, Column.HorizontalAlign.RIGHT);
    Column columnB =
        new Column(
            mockContext, "comment", Column.HorizontalAlign.CENTER, Column.HorizontalAlign.RIGHT);

    columnA.addCell("cell1").addCell("cell2").addCell("cell3").addCell("very long cell");
    columnB.addCell("cell4").addCell("cell5").addCell("cell6").addCell("very long cell");

    TableFormat<String> tableFormat =
        new TableFormat<String>(mockContext) {
          @Override
          public String getOutput(String entity) {
            return null;
          }
        };

    String outputString = tableFormat.getTableFormat(columnA, columnB).trim();
    Assertions.assertEquals(
        "+----------------+----------------+\n"
            + "|    Metalake    |    Comment     |\n"
            + "+----------------+----------------+\n"
            + "|          cell1 |          cell4 |\n"
            + "|          cell2 |          cell5 |\n"
            + "|          cell3 |          cell6 |\n"
            + "| very long cell | very long cell |\n"
            + "+----------------+----------------+",
        outputString);
  }

  @Test
  void testMetalakeDetailsWithTableFormat() {
    CommandContext mockContext = getMockContext();

    Metalake mockMetalake = getMockMetalake();
    TableFormat.output(mockMetalake, mockContext);

    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+---------------+-------------------------+\n"
            + "|   Metalake    |         Comment         |\n"
            + "+---------------+-------------------------+\n"
            + "| demo_metalake | This is a demo metalake |\n"
            + "+---------------+-------------------------+",
        output);
  }

  @Test
  void testListMetalakeWithTableFormat() {
    CommandContext mockContext = getMockContext();
    Metalake mockMetalake1 = getMockMetalake("metalake1", "This is a metalake");
    Metalake mockMetalake2 = getMockMetalake("metalake2", "This is another metalake");

    TableFormat.output(new Metalake[] {mockMetalake1, mockMetalake2}, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+-----------+\n"
            + "| Metalake  |\n"
            + "+-----------+\n"
            + "| metalake1 |\n"
            + "| metalake2 |\n"
            + "+-----------+",
        output);
  }

  @Test
  void testCatalogDetailsWithTableFormat() {
    CommandContext mockContext = getMockContext();
    Catalog mockCatalog = getMockCatalog();

    TableFormat.output(mockCatalog, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+--------------+------------+---------------+------------------------+\n"
            + "|   Catalog    |    Type    |   Provider    |        Comment         |\n"
            + "+--------------+------------+---------------+------------------------+\n"
            + "| demo_catalog | RELATIONAL | demo_provider | This is a demo catalog |\n"
            + "+--------------+------------+---------------+------------------------+",
        output);
  }

  @Test
  void testListCatalogWithTableFormat() {
    CommandContext mockContext = getMockContext();
    Catalog mockCatalog1 =
        getMockCatalog(
            "catalog1", Catalog.Type.RELATIONAL, "demo_provider", "This is a demo catalog");
    Catalog mockCatalog2 =
        getMockCatalog(
            "catalog2", Catalog.Type.RELATIONAL, "demo_provider", "This is another demo catalog");

    TableFormat.output(new Catalog[] {mockCatalog1, mockCatalog2}, mockContext);

    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+----------+\n"
            + "| Catalog  |\n"
            + "+----------+\n"
            + "| catalog1 |\n"
            + "| catalog2 |\n"
            + "+----------+",
        output);
  }

  @Test
  void testSchemaDetailsWithTableFormat() {
    CommandContext mockContext = getMockContext();
    Schema mockSchema = getMockSchema();

    TableFormat.output(mockSchema, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+-------------+-----------------------+\n"
            + "|   Schema    |        Comment        |\n"
            + "+-------------+-----------------------+\n"
            + "| demo_schema | This is a demo schema |\n"
            + "+-------------+-----------------------+",
        output);
  }

  @Test
  void testListSchemaWithTableFormat() {
    CommandContext mockContext = getMockContext();
    Schema mockSchema1 = getMockSchema("demo_schema1", "This is a demo schema");
    Schema mockSchema2 = getMockSchema("demo_schema2", "This is another demo schema");

    TableFormat.output(new Schema[] {mockSchema1, mockSchema2}, mockContext);

    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+--------------+\n"
            + "|    Schema    |\n"
            + "+--------------+\n"
            + "| demo_schema1 |\n"
            + "| demo_schema2 |\n"
            + "+--------------+",
        output);
  }

  @Test
  void testTableDetailsWithTableFormat() {
    CommandContext mockContext = getMockContext();
    Table mockTable = getMockTable();

    TableFormat.output(mockTable, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+------+---------+---------+---------------+----------+-------------------------+\n"
            + "| Name |  Type   | Default | AutoIncrement | Nullable |         Comment         |\n"
            + "+------+---------+---------+---------------+----------+-------------------------+\n"
            + "| id   | integer |         | true          | false    | This is a int column    |\n"
            + "| name | string  |         |               | true     | This is a string column |\n"
            + "+------+---------+---------+---------------+----------+-------------------------+",
        output);
  }

  @Test
  void testListTableWithTableFormat() {
    CommandContext mockContext = getMockContext();

    Table mockTable1 = getMockTable("table1", "This is a demo table");
    Table mockTable2 = getMockTable("table2", "This is another demo table");
    TableFormat.output(new Table[] {mockTable1, mockTable2}, mockContext);

    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+--------+\n"
            + "| Table  |\n"
            + "+--------+\n"
            + "| table1 |\n"
            + "| table2 |\n"
            + "+--------+",
        output);
  }

  @Test
  void testAuditWithTableFormat() {
    CommandContext mockContext = getMockContext();
    Audit mockAudit = mock(Audit.class);
    when(mockAudit.creator()).thenReturn("demo_user");
    when(mockAudit.createTime()).thenReturn(Instant.ofEpochMilli(1611111111111L));
    when(mockAudit.lastModifier()).thenReturn("demo_user");
    when(mockAudit.lastModifiedTime()).thenReturn(Instant.ofEpochMilli(1611111111111L));

    TableFormat.output(mockAudit, mockContext);

    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+-----------+--------------------------+-----------+--------------------------+\n"
            + "|  Creator  |       Creation at        | Modifier  |       Modified at        |\n"
            + "+-----------+--------------------------+-----------+--------------------------+\n"
            + "| demo_user | 2021-01-20T02:51:51.111Z | demo_user | 2021-01-20T02:51:51.111Z |\n"
            + "+-----------+--------------------------+-----------+--------------------------+",
        output);
  }

  @Test
  void testAuditWithTableFormatWithNullValues() {
    CommandContext mockContext = getMockContext();
    Audit mockAudit = mock(Audit.class);
    when(mockAudit.creator()).thenReturn("demo_user");
    when(mockAudit.createTime()).thenReturn(null);
    when(mockAudit.lastModifier()).thenReturn(null);
    when(mockAudit.lastModifiedTime()).thenReturn(null);

    TableFormat.output(mockAudit, mockContext);

    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+-----------+-------------+----------+-------------+\n"
            + "|  Creator  | Creation at | Modifier | Modified at |\n"
            + "+-----------+-------------+----------+-------------+\n"
            + "| demo_user | N/A         | N/A      | N/A         |\n"
            + "+-----------+-------------+----------+-------------+",
        output);
  }

  @Test
  void testListColumnWithTableFormat() {
    CommandContext mockContext = getMockContext();
    org.apache.gravitino.rel.Column mockColumn1 =
        getMockColumn(
            "column1",
            Types.IntegerType.get(),
            "This is a int column",
            false,
            true,
            new Literal<Integer>() {
              @Override
              public Integer value() {
                return 4;
              }

              @Override
              public Type dataType() {
                return null;
              }
            });
    org.apache.gravitino.rel.Column mockColumn2 =
        getMockColumn(
            "column2",
            Types.StringType.get(),
            "This is a string column",
            true,
            false,
            new Literal<String>() {
              @Override
              public String value() {
                return "default value";
              }

              @Override
              public Type dataType() {
                return null;
              }
            });
    org.apache.gravitino.rel.Column mockColumn3 =
        getMockColumn(
            "column2",
            Types.StringType.get(),
            "This is a string column",
            true,
            false,
            new Literal<String>() {
              @Override
              public String value() {
                return "";
              }

              @Override
              public Type dataType() {
                return null;
              }
            });

    org.apache.gravitino.rel.Column mockColumn4 =
        getMockColumn(
            "column2",
            Types.StringType.get(),
            "This is a string column",
            true,
            false,
            FunctionExpression.of("current_timestamp"));

    org.apache.gravitino.rel.Column mockColumn5 =
        getMockColumn(
            "column2",
            Types.StringType.get(),
            "This is a string column",
            true,
            false,
            FunctionExpression.of("date", new Expression[] {field("b")}));

    TableFormat.output(
        new org.apache.gravitino.rel.Column[] {
          mockColumn1, mockColumn2, mockColumn3, mockColumn4, mockColumn5
        },
        mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+---------+---------+---------------------+---------------+----------+-------------------------+\n"
            + "|  Name   |  Type   |       Default       | AutoIncrement | Nullable |         Comment         |\n"
            + "+---------+---------+---------------------+---------------+----------+-------------------------+\n"
            + "| column1 | integer | 4                   | true          | false    | This is a int column    |\n"
            + "| column2 | string  | default value       |               | true     | This is a string column |\n"
            + "| column2 | string  | ''                  |               | true     | This is a string column |\n"
            + "| column2 | string  | current_timestamp() |               | true     | This is a string column |\n"
            + "| column2 | string  | date([b])           |               | true     | This is a string column |\n"
            + "+---------+---------+---------------------+---------------+----------+-------------------------+",
        output);
  }

  @Test
  void testListColumnWithTableFormatAndEmptyDefaultValues() {
    CommandContext mockContext = getMockContext();
    org.apache.gravitino.rel.Column mockColumn1 =
        getMockColumn(
            "column1",
            Types.IntegerType.get(),
            "This is a int column",
            false,
            true,
            DEFAULT_VALUE_NOT_SET);
    org.apache.gravitino.rel.Column mockColumn2 =
        getMockColumn(
            "column2",
            Types.StringType.get(),
            "This is a string column",
            true,
            false,
            DEFAULT_VALUE_NOT_SET);
    org.apache.gravitino.rel.Column mockColumn3 =
        getMockColumn(
            "column3",
            Types.BooleanType.get(),
            "this is a boolean column",
            true,
            false,
            DEFAULT_VALUE_NOT_SET);

    TableFormat.output(
        new org.apache.gravitino.rel.Column[] {mockColumn1, mockColumn2, mockColumn3}, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+---------+---------+---------+---------------+----------+--------------------------+\n"
            + "|  Name   |  Type   | Default | AutoIncrement | Nullable |         Comment          |\n"
            + "+---------+---------+---------+---------------+----------+--------------------------+\n"
            + "| column1 | integer |         | true          | false    | This is a int column     |\n"
            + "| column2 | string  |         |               | true     | This is a string column  |\n"
            + "| column3 | boolean |         |               | true     | this is a boolean column |\n"
            + "+---------+---------+---------+---------------+----------+--------------------------+",
        output);
  }

  @Test
  void testListModelWithTableFormat() {
    CommandContext mockContext = getMockContext();
    Model model1 = getMockModel("model1", "This is a demo model", 1);
    Model model2 = getMockModel("model2", "This is another demo model", 2);
    Model model3 = getMockModel("model3", "This is a third demo model", 3);

    TableFormat.output(new Model[] {model1, model2, model3}, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+--------+\n"
            + "|  Name  |\n"
            + "+--------+\n"
            + "| model1 |\n"
            + "| model2 |\n"
            + "| model3 |\n"
            + "+--------+",
        output);
  }

  @Test
  void testModelDetailsWithTableFormat() {
    CommandContext mockContext = getMockContext();
    Model mockModel = getMockModel("demo_model", "This is a demo model", 1);

    TableFormat.output(mockModel, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+------------+----------------------+----------------+\n"
            + "|    Name    |       Comment        | Latest version |\n"
            + "+------------+----------------------+----------------+\n"
            + "| demo_model | This is a demo model | 1              |\n"
            + "+------------+----------------------+----------------+",
        output);
  }

  @Test
  void testListUserWithTableFormat() {
    CommandContext mockContext = getMockContext();
    User user1 = getMockUser("user1", Arrays.asList("role1", "role2"));
    User user2 = getMockUser("user2", Arrays.asList("role3", "role4"));
    User user3 = getMockUser("user3", Arrays.asList("role5"));

    TableFormat.output(new User[] {user1, user2, user3}, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+-------+\n"
            + "| Name  |\n"
            + "+-------+\n"
            + "| user1 |\n"
            + "| user2 |\n"
            + "| user3 |\n"
            + "+-------+",
        output);
  }

  @Test
  void testUserDetailsWithTableFormat() {
    CommandContext mockContext = getMockContext();
    User mockUser = getMockUser("demo_user", Arrays.asList("role1", "role2"));

    TableFormat.output(mockUser, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+-----------+--------------+\n"
            + "|   Name    |    Roles     |\n"
            + "+-----------+--------------+\n"
            + "| demo_user | role1, role2 |\n"
            + "+-----------+--------------+",
        output);
  }

  @Test
  void testListGroupWithTableFormat() {
    CommandContext mockContext = getMockContext();
    Group group1 = getMockGroup("group1", Arrays.asList("role1", "role2"));
    Group group2 = getMockGroup("group2", Arrays.asList("role3", "role4"));
    Group group3 = getMockGroup("group3", Arrays.asList("role5"));

    TableFormat.output(new Group[] {group1, group2, group3}, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+--------+\n"
            + "|  Name  |\n"
            + "+--------+\n"
            + "| group1 |\n"
            + "| group2 |\n"
            + "| group3 |\n"
            + "+--------+",
        output);
  }

  @Test
  void testGroupDetailsWithTableFormat() {
    CommandContext mockContext = getMockContext();
    Group mockGroup = getMockGroup("demo_group", Arrays.asList("role1", "role2"));

    TableFormat.output(mockGroup, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+------------+--------------+\n"
            + "|    Name    |    Roles     |\n"
            + "+------------+--------------+\n"
            + "| demo_group | role1, role2 |\n"
            + "+------------+--------------+",
        output);
  }

  @Test
  void testTagDetailsWithTableFormat() {
    CommandContext mockContext = getMockContext();
    Tag mockTag = getMockTag("tag1", "comment for tag1");

    TableFormat.output(mockTag, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+------+------------------+\n"
            + "| Name |     Comment      |\n"
            + "+------+------------------+\n"
            + "| tag1 | comment for tag1 |\n"
            + "+------+------------------+",
        output);
  }

  @Test
  void testTagDetailsWithTableFormatWithNullValues() {
    CommandContext mockContext = getMockContext();
    Tag mockTag = mock(Tag.class);
    when(mockTag.name()).thenReturn("tag1");
    when(mockTag.comment()).thenReturn(null);

    TableFormat.output(mockTag, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+------+---------+\n"
            + "| Name | Comment |\n"
            + "+------+---------+\n"
            + "| tag1 | N/A     |\n"
            + "+------+---------+",
        output);
  }

  @Test
  void testListAllTagsWithTableFormat() {
    CommandContext mockContext = getMockContext();

    Tag mockTag1 = getMockTag("tag1", "comment for tag1");
    Tag mockTag2 = getMockTag("tag2", "comment for tag2");
    Tag mockTag3 = getMockTag("tag3", "comment for tag3");

    TableFormat.output(new Tag[] {mockTag1, mockTag2, mockTag3}, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+------+\n"
            + "| Name |\n"
            + "+------+\n"
            + "| tag1 |\n"
            + "| tag2 |\n"
            + "| tag3 |\n"
            + "+------+",
        output);
  }

  @Test
  void testListTagPropertiesWithTableFormat() {
    CommandContext mockContext = getMockContext();

    Tag mockTag1 = getMockTag("tag1", "comment for tag1");

    TableFormat.output(mockTag1.properties(), mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+-----+-------+\n"
            + "| Key | Value |\n"
            + "+-----+-------+\n"
            + "| k1  | v1    |\n"
            + "| k2  | v2    |\n"
            + "+-----+-------+",
        output);
  }

  @Test
  void testListTagPropertiesWithTableFormatWithEmptyMap() {
    CommandContext mockContext = getMockContext();

    Tag mockTag1 = getMockTag("tag1", "comment for tag1", Collections.emptyMap());

    TableFormat.output(mockTag1.properties(), mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+-----+-------+\n"
            + "| Key | Value |\n"
            + "+-----+-------+\n"
            + "| N/A | N/A   |\n"
            + "+-----+-------+",
        output);
  }

  @Test
  void testOutputWithUnsupportType() {
    CommandContext mockContext = getMockContext();
    Object mockObject = new Object();

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          TableFormat.output(mockObject, mockContext);
        });
  }

  private CommandContext getMockContext() {
    CommandContext mockContext = mock(CommandContext.class);

    return mockContext;
  }

  private Metalake getMockMetalake() {
    return getMockMetalake("demo_metalake", "This is a demo metalake");
  }

  private Metalake getMockMetalake(String name, String comment) {
    Metalake mockMetalake = mock(Metalake.class);
    when(mockMetalake.name()).thenReturn(name);
    when(mockMetalake.comment()).thenReturn(comment);

    return mockMetalake;
  }

  private Catalog getMockCatalog() {
    return getMockCatalog(
        "demo_catalog", Catalog.Type.RELATIONAL, "demo_provider", "This is a demo catalog");
  }

  private Catalog getMockCatalog(String name, Catalog.Type type, String provider, String comment) {
    Catalog mockCatalog = mock(Catalog.class);
    when(mockCatalog.name()).thenReturn(name);
    when(mockCatalog.type()).thenReturn(type);
    when(mockCatalog.provider()).thenReturn(provider);
    when(mockCatalog.comment()).thenReturn(comment);

    return mockCatalog;
  }

  private Schema getMockSchema() {
    return getMockSchema("demo_schema", "This is a demo schema");
  }

  private Schema getMockSchema(String name, String comment) {
    Schema mockSchema = mock(Schema.class);
    when(mockSchema.name()).thenReturn(name);
    when(mockSchema.comment()).thenReturn(comment);

    return mockSchema;
  }

  private Table getMockTable() {
    return getMockTable("demo_table", "This is a demo table");
  }

  private Table getMockTable(String name, String comment) {
    Table mockTable = mock(Table.class);
    org.apache.gravitino.rel.Column mockColumnInt =
        getMockColumn(
            "id",
            Types.IntegerType.get(),
            "This is a int column",
            false,
            true,
            DEFAULT_VALUE_NOT_SET);
    org.apache.gravitino.rel.Column mockColumnString =
        getMockColumn(
            "name",
            Types.StringType.get(),
            "This is a string column",
            true,
            false,
            DEFAULT_VALUE_NOT_SET);

    when(mockTable.name()).thenReturn(name);
    when(mockTable.comment()).thenReturn(comment);
    when(mockTable.columns())
        .thenReturn(new org.apache.gravitino.rel.Column[] {mockColumnInt, mockColumnString});

    return mockTable;
  }

  private org.apache.gravitino.rel.Column getMockColumn(
      String name,
      Type dataType,
      String comment,
      boolean nullable,
      boolean autoIncrement,
      Expression defaultValue) {

    org.apache.gravitino.rel.Column mockColumn = mock(org.apache.gravitino.rel.Column.class);
    when(mockColumn.name()).thenReturn(name);
    when(mockColumn.dataType()).thenReturn(dataType);
    when(mockColumn.comment()).thenReturn(comment);
    when(mockColumn.nullable()).thenReturn(nullable);
    when(mockColumn.defaultValue()).thenReturn(defaultValue);
    when(mockColumn.autoIncrement()).thenReturn(autoIncrement);

    return mockColumn;
  }

  private Model getMockModel(String name, String comment, int lastVersion) {
    Model mockModel = mock(Model.class);
    when(mockModel.name()).thenReturn(name);
    when(mockModel.comment()).thenReturn(comment);
    when(mockModel.latestVersion()).thenReturn(lastVersion);

    return mockModel;
  }

  private User getMockUser(String name, List<String> roles) {
    User mockUser = mock(User.class);
    when(mockUser.name()).thenReturn(name);
    when(mockUser.roles()).thenReturn(roles);

    return mockUser;
  }

  private Group getMockGroup(String name, List<String> roles) {
    Group mockGroup = mock(Group.class);
    when(mockGroup.name()).thenReturn(name);
    when(mockGroup.roles()).thenReturn(roles);

    return mockGroup;
  }

  private Tag getMockTag(String name, String comment) {
    return getMockTag(name, comment, ImmutableMap.of("k1", "v1", "k2", "v2"));
  }

  private Tag getMockTag(String name, String comment, Map<String, String> properties) {
    Tag mockTag = mock(Tag.class);
    when(mockTag.name()).thenReturn(name);
    when(mockTag.comment()).thenReturn(comment);
    when(mockTag.properties()).thenReturn(properties);

    return mockTag;
  }
}
