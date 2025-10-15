package org.apache.gravitino.recommender.impl;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.recommender.api.TableMetadataProvider;
import org.apache.gravitino.rel.Table;

public class GravitinoTableMetadataProvider implements TableMetadataProvider {

  GravitinoClient gravitinoClient;

  @Override
  public Table getTableMetadata(NameIdentifier tableIdentifier) {
    return gravitinoClient.loadCatalog("").asTableCatalog().loadTable(tableIdentifier);
  }
}
