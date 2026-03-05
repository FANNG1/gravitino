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

package org.apache.gravitino.maintenance.optimizer.integration.test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.recommender.strategy.GravitinoStrategy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.BeforeAll;

// Set up the Gravitino server, metalake, Iceberg catalogs
public class GravitinoOptimizerEnvIT extends BaseIT {

  protected static final String METALAKE_NAME = "test_metalake";
  protected static final String GRAVITINO_CATALOG_NAME = "iceberg";
  protected static final String TEST_SCHEMA = "test_schema";
  private static ContainerSuite containerSuite = ContainerSuite.getInstance();
  protected Catalog catalogClient;
  protected GravitinoMetalake metalakeClient;
  protected OptimizerEnv optimizerEnv;

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    containerSuite.startPostgreSQLContainer(TestDatabaseName.PG_TEST_PARTITION_STATS);
    super.startIntegrationTest();
    initMetalakeAndCatalog();
    this.optimizerEnv = initOptimizerEnv();
  }

  protected void createTable(String tableName) {
    catalogClient
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(TEST_SCHEMA, tableName),
            new Column[] {Column.of("col_1", Types.IntegerType.get())},
            "comment",
            ImmutableMap.of("format", "iceberg"));
  }

  protected NameIdentifier getTableIdentifier(String tableName) {
    return NameIdentifier.of(GRAVITINO_CATALOG_NAME, TEST_SCHEMA, tableName);
  }

  protected void createPartitionTable(String tableName) {
    catalogClient
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(TEST_SCHEMA, tableName),
            new Column[] {
              Column.of("col1", Types.IntegerType.get(), "col1"),
              Column.of("col2", Types.IntegerType.get(), "col2"),
              Column.of("col3", Types.IntegerType.get(), "col3")
            },
            "comment",
            ImmutableMap.of("format", "iceberg"),
            new Transform[] {
              Transforms.identity("col1"), Transforms.bucket(8, new String[] {"col2"})
            });
  }

  protected void createPolicy(String policyName, Map<String, Object> rules, String policyType) {
    PolicyContent content =
        PolicyContents.custom(
            rules,
            ImmutableSet.of(MetadataObject.Type.TABLE),
            Map.of(
                GravitinoStrategy.STRATEGY_TYPE_KEY,
                policyType,
                GravitinoStrategy.JOB_TEMPLATE_NAME_KEY,
                "template-name"));
    metalakeClient.createPolicy(policyName, "custom", "comment", true, content);
  }

  protected void associatePoliciesToTable(String policyName, String tableName) {
    Table table =
        catalogClient.asTableCatalog().loadTable(NameIdentifier.of(TEST_SCHEMA, tableName));
    table.supportsPolicies().associatePolicies(new String[] {policyName}, new String[] {});
  }

  protected void associatePoliciesToSchema(String policyName, String schemaName) {
    Schema schema = catalogClient.asSchemas().loadSchema(schemaName);
    schema.supportsPolicies().associatePolicies(new String[] {policyName}, new String[] {});
  }

  protected Map<String, String> getSpecifyConfigs() {
    return Map.of();
  }

  protected OptimizerEnv initOptimizerEnv() {
    Map<String, String> configs = new HashMap<>();

    configs.putAll(getGravitinoConfigs());
    configs.putAll(getSpecifyConfigs());

    OptimizerConfig config = new OptimizerConfig(configs);
    return new OptimizerEnv(config);
  }

  private Map<String, String> getGravitinoConfigs() {
    int gravitinoPort = getGravitinoServerPort();
    String uri = String.format("http://127.0.0.1:%d", gravitinoPort);
    return ImmutableMap.of(
        OptimizerConfig.GRAVITINO_URI,
        uri,
        OptimizerConfig.GRAVITINO_METALAKE,
        METALAKE_NAME,
        OptimizerConfig.GRAVITINO_DEFAULT_CATALOG,
        GRAVITINO_CATALOG_NAME);
  }

  private void initMetalakeAndCatalog() {
    this.metalakeClient = client.createMetalake(METALAKE_NAME, "", new HashMap<>());

    this.catalogClient = createGenericCatalog();

    if (!catalogClient.asSchemas().schemaExists(TEST_SCHEMA)) {
      catalogClient.asSchemas().createSchema(TEST_SCHEMA, "comment", ImmutableMap.of());
    }
  }

  private Catalog createGenericCatalog() {
    return metalakeClient.createCatalog(
        GRAVITINO_CATALOG_NAME,
        Catalog.Type.RELATIONAL,
        "lakehouse-generic",
        "comment",
        ImmutableMap.of("location", "xx"));
  }

  private Catalog createGravitinoIcebergCatalog() {
    return metalakeClient.createCatalog(
        GRAVITINO_CATALOG_NAME,
        Catalog.Type.RELATIONAL,
        "lakehouse-iceberg",
        "comment",
        ImmutableMap.of(
            IcebergConstants.URI,
            getPGUri(),
            IcebergConstants.CATALOG_BACKEND,
            "jdbc",
            IcebergConstants.GRAVITINO_JDBC_DRIVER,
            "org.postgresql.Driver",
            IcebergConstants.GRAVITINO_JDBC_USER,
            getPGUser(),
            IcebergConstants.GRAVITINO_JDBC_PASSWORD,
            getPGPassword(),
            IcebergConstants.WAREHOUSE,
            "file:///tmp/"));
  }

  private String getPGUri() {
    return containerSuite
        .getPostgreSQLContainer()
        .getJdbcUrl(TestDatabaseName.PG_TEST_PARTITION_STATS);
  }

  private String getPGUser() {
    return containerSuite.getPostgreSQLContainer().getUsername();
  }

  private String getPGPassword() {
    return containerSuite.getPostgreSQLContainer().getPassword();
  }
}
