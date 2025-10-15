package org.apache.gravitino.recommender.impl;

import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.recommender.api.TableStatsProvider;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.stats.PartitionStatistics;
import org.apache.gravitino.stats.Statistic;

public class GravitinoTableStatsProvider implements TableStatsProvider {

  private GravitinoClient gravitinoClient;

  @Override
  public List<Statistic> getTableStats(NameIdentifier tableIdentifier) {
    Table t = gravitinoClient.loadCatalog("").asTableCatalog().loadTable(tableIdentifier);
    List<Statistic> statistics = t.supportsStatistics().listStatistics();
    return statistics;
  }

  @Override
  public List<PartitionStatistics> getPartitionStats(NameIdentifier table) {
    Table t = gravitinoClient.loadCatalog("").asTableCatalog().loadTable(table);
    List<PartitionStatistics> partitionStatistics =
        t.supportsPartitionStatistics().listPartitionStatistics(null);
    return partitionStatistics;
  }
}
