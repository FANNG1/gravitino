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

package org.apache.gravitino.maintenance.optimizer.recommender.table;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.maintenance.optimizer.api.recommender.TableMetadataProvider;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.common.util.IdentifierUtils;
import org.apache.gravitino.rel.Table;

public class GravitinoTableMetadataProvider implements TableMetadataProvider {
  public static final String NAME = "gravitino-table-metadata-provider";
  private GravitinoClient gravitinoClient;
  private String defaultCatalogName;

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    OptimizerConfig config = optimizerEnv.config();
    String uri = config.get(OptimizerConfig.GRAVITINO_URI_CONFIG);
    String metalake = config.get(OptimizerConfig.GRAVITINO_METALAKE_CONFIG);
    this.gravitinoClient = GravitinoClient.builder(uri).withMetalake(metalake).build();
    this.defaultCatalogName = config.get(OptimizerConfig.GRAVITINO_DEFAULT_CATALOG_CONFIG);
  }

  @Override
  public Table tableMetadata(NameIdentifier tableIdentifier) {
    return gravitinoClient
        .loadCatalog(
            IdentifierUtils.getCatalogNameFromTableIdentifier(tableIdentifier, defaultCatalogName))
        .asTableCatalog()
        .loadTable(IdentifierUtils.removeCatalogFromIdentifier(tableIdentifier));
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void close() throws Exception {
    if (gravitinoClient != null) {
      gravitinoClient.close();
    }
  }
}
