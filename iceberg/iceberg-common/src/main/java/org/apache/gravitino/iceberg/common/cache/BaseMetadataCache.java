/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.common.cache;

import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseMetadataCache implements MetadataCache {

  public static final Logger LOG = LoggerFactory.getLogger(BaseMetadataCache.class);

  protected abstract TableMetadata doGetTableMetadata(TableIdentifier tableIdentifier);

  private SupportsMetadataLocation supportsMetadataLocation;

  protected void initialize(SupportsMetadataLocation supportsMetadataLocation) {
    this.supportsMetadataLocation = supportsMetadataLocation;
  }

  @Override
  public TableMetadata getTableMetadata(TableIdentifier tableIdentifier) {
    TableMetadata tableMetadata = doGetTableMetadata(tableIdentifier);
    if (tableMetadata == null) {
      LOG.info("Table cache miss, table identifier: {}", tableIdentifier);
      return null;
    }
    String latestLocation = supportsMetadataLocation.metadataLocation(tableIdentifier);
    if (latestLocation == null) {
      LOG.info("Table metadata location is null, table identifier: {}", tableIdentifier);
      return null;
    }
    if (latestLocation.equals(tableMetadata.metadataFileLocation())) {
      LOG.info(
          "Table metadata location match, table identifier: {}, table metadata location: {}",
          tableIdentifier,
          tableMetadata.metadataFileLocation());
      return tableMetadata;
    }

    LOG.info(
        "Table metadata location not match, table identifier: {}, table metadata location: {}",
        tableIdentifier,
        tableMetadata.metadataFileLocation());
    invalidate(tableIdentifier);
    return null;
  }
}
