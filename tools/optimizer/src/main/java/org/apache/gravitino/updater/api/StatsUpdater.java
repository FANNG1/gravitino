package org.apache.gravitino.updater.api;

import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.stats.PartitionStatistics;

// Update the statistics to Gravitino stats store or external systems.
public interface StatsUpdater {
  void updateTableStatistics(NameIdentifier tableIdentifier, List<OStatistic> statistics);

  void updatePartitionStatistics(
      NameIdentifier tableIdentifier, List<PartitionStatistics> partitionStatistics);
}
