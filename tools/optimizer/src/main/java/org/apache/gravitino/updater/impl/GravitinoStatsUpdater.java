package org.apache.gravitino.updater.impl;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.stats.PartitionStatistics;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.updater.api.OStatistic;
import org.apache.gravitino.updater.api.StatsUpdater;

public class GravitinoStatsUpdater implements StatsUpdater {

  GravitinoClient gravitinoClient;

  @Override
  public void updateTableStatistics(NameIdentifier tableIdentifier, List<OStatistic> statistics) {
    Map<String, StatisticValue<?>> statsMap =
        statistics.stream().collect(Collectors.toMap(OStatistic::name, OStatistic::value));
    gravitinoClient
        .loadCatalog("")
        .asTableCatalog()
        .loadTable(tableIdentifier)
        .supportsStatistics()
        .updateStatistics(statsMap);
  }

  @Override
  public void updatePartitionStatistics(
      NameIdentifier tableIdentifier, List<PartitionStatistics> partitionStatistics) {
    // gravitinoClient.loadCatalog("").asTableCatalog().loadTable(tableIdentifier).supportsPartitionStatistics().updatePartitionStatistics(partitionStatistics);
  }
}
