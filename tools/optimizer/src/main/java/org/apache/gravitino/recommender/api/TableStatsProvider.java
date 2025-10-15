package org.apache.gravitino.recommender.api;

import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.stats.PartitionStatistics;
import org.apache.gravitino.stats.Statistic;

// The table stats provider to get the table and partition stats from Gravitino or external systems.
public interface TableStatsProvider {
  List<Statistic> getTableStats(NameIdentifier tableIdentifier);

  List<PartitionStatistics> getPartitionStats(NameIdentifier tableIdentifier);
}
