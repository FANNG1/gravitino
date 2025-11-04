package org.apache.gravitino.updater.impl;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.stats.PartitionStatisticsUpdate;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.updater.api.PartitionStatistic;
import org.apache.gravitino.updater.api.SingleStatistic;
import org.apache.gravitino.updater.api.StatsUpdater;
import org.apache.gravitino.updater.impl.util.PartitionUtils;

// todo Support column stats updater
public class GravitinoStatsUpdater implements StatsUpdater {

  GravitinoClient gravitinoClient;

  @Override
  public void updateTableStatistics(
      NameIdentifier tableIdentifier, List<SingleStatistic<?>> statistics) {
    doUpdateTableStatistics(tableIdentifier, getTableStatisticsMap(statistics));
    doUpdatePartitionStatistics(tableIdentifier, getPartitionStatsUpdates(statistics));
  }

  private void doUpdateTableStatistics(
      NameIdentifier tableIdentifier, Map<String, StatisticValue<?>> tableStatsMap) {
    if (tableStatsMap.isEmpty()) {
      return;
    }
    gravitinoClient
        .loadCatalog("")
        .asTableCatalog()
        .loadTable(tableIdentifier)
        .supportsStatistics()
        .updateStatistics(tableStatsMap);
  }

  private Map<String, StatisticValue<?>> getTableStatisticsMap(
      List<SingleStatistic<?>> statistics) {
    return statistics.stream()
        .filter(statistic -> !(statistic instanceof PartitionStatistic))
        .collect(Collectors.toMap(SingleStatistic::name, SingleStatistic::value));
  }

  private List<PartitionStatisticsUpdate> getPartitionStatsUpdates(
      List<SingleStatistic<?>> partitionStatistics) {
    return partitionStatistics.stream()
        .filter(statistic -> statistic instanceof PartitionStatistic)
        .map(statistic -> (PartitionStatistic) statistic)
        .map(
            partitionStatistic ->
                new PartitionStatisticsUpdate() {
                  @Override
                  public String partitionName() {
                    return PartitionUtils.getGravitinoPartitionName(
                        partitionStatistic.partitions());
                  }

                  @Override
                  public Map<String, StatisticValue<?>> statistics() {
                    return Map.of(partitionStatistic.name(), partitionStatistic.value());
                  }
                })
        .collect(Collectors.toList());
  }

  private void doUpdatePartitionStatistics(
      NameIdentifier tableIdentifier, List<PartitionStatisticsUpdate> partitionStatisticsUpdates) {
    gravitinoClient
        .loadCatalog("")
        .asTableCatalog()
        .loadTable(tableIdentifier)
        .supportsPartitionStatistics()
        .updatePartitionStatistics(partitionStatisticsUpdates);
  }
}
