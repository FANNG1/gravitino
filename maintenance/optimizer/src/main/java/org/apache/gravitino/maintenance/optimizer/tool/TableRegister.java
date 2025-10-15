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

import com.google.common.base.Preconditions;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.tool.FileContentReader.ReaderSource;
import org.apache.gravitino.maintenance.optimizer.tool.TableRegistrationParser.TableRegistrationRequest;
import org.apache.gravitino.rel.TableCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableRegister {
  private static final Logger LOG = LoggerFactory.getLogger(TableRegister.class);

  private final GravitinoClient gravitinoClient;
  private final TableRegistrationParser parser;
  private final String s3Region;

  public TableRegister(OptimizerEnv optimizerEnv) {
    Preconditions.checkArgument(optimizerEnv != null, "optimizerEnv must not be null");
    OptimizerConfig config = optimizerEnv.config();
    String uri = config.get(OptimizerConfig.GRAVITINO_URI_CONFIG);
    String metalake = config.get(OptimizerConfig.GRAVITINO_METALAKE_CONFIG);
    String defaultCatalog = config.get(OptimizerConfig.GRAVITINO_DEFAULT_CATALOG_CONFIG);
    this.s3Region = config.get(OptimizerConfig.S3_REGION_CONFIG);
    this.gravitinoClient = GravitinoClient.builder(uri).withMetalake(metalake).build();
    this.parser = new TableRegistrationParser(defaultCatalog);
  }

  public void registerFromFile(String filePath) {
    try (FileContentReader reader = getFileContentReader(filePath)) {
      // list all files
      for (ReaderSource source : reader.listSources(filePath)) {
        try (ReaderSource autoCloseSource = source;
            BufferedReader bufferedReader = reader.getReader(autoCloseSource)) {
          LOG.info("Processing: {}", autoCloseSource.getPath());
          // read file content
          handleContent(bufferedReader);
        } catch (Exception e) {
          LOG.error("Failed to process source: {}", source.getPath(), e);
          System.out.printf("ERROR processing %s: %s%n", source.getPath(), e.getMessage());
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to register tables from file: {}", filePath, e);
      System.out.printf("ERROR processing %s: %s%n", filePath, e.getMessage());
    }
  }

  private FileContentReader getFileContentReader(String filePath) {
    return S3Utils.isS3Path(filePath) ? new S3FileReader(s3Region) : new LocalFileReader();
  }

  private void handleContent(BufferedReader reader) throws IOException {
    String line;
    int lineNumber = 0;
    while ((line = reader.readLine()) != null) {
      lineNumber++;
      if (line.trim().isEmpty()) {
        continue;
      }
      handleLine(line, lineNumber);
    }
  }

  private void handleLine(String line, int lineNumber) {
    String identifierForOutput = "line " + lineNumber;
    try {
      TableRegistrationRequest request = parser.parse(line);
      identifierForOutput = request.originalIdentifier();
      createTable(request);
      System.out.printf("OK %s%n", identifierForOutput);
    } catch (Exception e) {
      System.out.printf("ERROR %s: %s%n", identifierForOutput, e.getMessage());
      LOG.error("Failed to register table for {}", identifierForOutput, e);
    }
  }

  private void createTable(TableRegistrationRequest request) {
    Catalog catalog = gravitinoClient.loadCatalog(request.catalogName());
    TableCatalog tableCatalog = catalog.asTableCatalog();
    String schemaName = request.tableIdentifier().namespace().levels()[0];

    if (!catalog.asSchemas().schemaExists(schemaName)) {
      catalog.asSchemas().createSchema(schemaName, "", Collections.emptyMap());
    }

    if (tableCatalog.tableExists(request.tableIdentifier())) {
      tableCatalog.dropTable(request.tableIdentifier());
    }

    var icebergTable = request.icebergTable();
    Map<String, String> createProperties = new HashMap<>(icebergTable.properties());
    createProperties.put("format", "iceberg");
    tableCatalog.createTable(
        request.tableIdentifier(),
        icebergTable.columns(),
        icebergTable.comment(),
        createProperties,
        icebergTable.partitioning(),
        icebergTable.distribution(),
        icebergTable.sortOrder());
  }
}
