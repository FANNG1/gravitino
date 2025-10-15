package org.apache.gravitino.recommender.api;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.rel.Table;

// The table metadata provider to get the table metadata from Gravitino or external systems.
public interface TableMetadataProvider {
  Table getTableMetadata(NameIdentifier tableIdentifier);
}
