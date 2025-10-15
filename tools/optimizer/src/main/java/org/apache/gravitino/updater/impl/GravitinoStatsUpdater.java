package org.apache.gravitino.updater.impl;

import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.stats.PartitionStatistics;
import org.apache.gravitino.stats.Statistic;
import org.apache.gravitino.updater.api.StatsUpdater;

public class GravitinoStatsUpdater implements StatsUpdater {

  GravitinoClient gravitinoClient;

  @Override
  public void updateTableStatistics(NameIdentifier tableIdentifier, List<Statistic> statistics) {
    // gravitinoClient.loadCatalog("").asTableCatalog().loadTable(tableIdentifier).supportsStatistics().updateStatistics(statistics);
  }

  @Override
  public void updatePartitionStatistics(
      NameIdentifier tableIdentifier, List<PartitionStatistics> partitionStatistics) {
    // gravitinoClient.loadCatalog("").asTableCatalog().loadTable(tableIdentifier).supportsPartitionStatistics().updatePartitionStatistics(partitionStatistics);
  }
}
