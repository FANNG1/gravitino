package org.apache.gravitino.updater;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.stats.PartitionStatistics;
import org.apache.gravitino.stats.Statistic;
import org.apache.gravitino.updater.api.Computer;
import org.apache.gravitino.updater.api.MetricsComputer;
import org.apache.gravitino.updater.api.MetricsUpdater;
import org.apache.gravitino.updater.api.StatsComputer;
import org.apache.gravitino.updater.api.StatsUpdater;
import org.apache.gravitino.updater.api.SupportPartitionStats;
import org.apache.gravitino.updater.api.SupportTableStats;

public class Updater {
  private Map<String, StatsComputer> computers = new HashMap<>();
  private StatsUpdater statsUpdater;
  private MetricsUpdater metricsUpdater;

  void updateStats(StatsComputer statsComputer, NameIdentifier tableIdentifier) {
    if (statsComputer instanceof SupportTableStats) {
      SupportTableStats supportTableStats = ((SupportTableStats) statsComputer);
      List<Statistic> statistics = supportTableStats.computeTableStats(tableIdentifier);
      statsUpdater.updateTableStatistics(tableIdentifier, statistics);
    }

    if (statsComputer instanceof SupportPartitionStats) {
      SupportPartitionStats supportPartitionStats = ((SupportPartitionStats) statsComputer);
      List<PartitionStatistics> partitionStatistics =
          supportPartitionStats.computePartitionStats(tableIdentifier);
      statsUpdater.updatePartitionStatistics(tableIdentifier, partitionStatistics);
    }
  }

  void updateMetrics(MetricsComputer metricsComputer, NameIdentifier tableIdentifier) {
    metricsUpdater.updateMetrics(metricsComputer.computeMetrics(tableIdentifier));
  }

  void update(
      String statsComputerName, List<NameIdentifier> tableIdentifiers, UpdateType updateType) {
    Computer computer = getStatsComputer(statsComputerName);
    for (NameIdentifier table : tableIdentifiers) {
      if (updateType == UpdateType.STATS) {
        updateStats((StatsComputer) computer, table);
      } else if (updateType == UpdateType.METRICS) {
        updateMetrics((MetricsComputer) computer, table);
      }
    }
  }

  private StatsComputer getStatsComputer(String statsComputerName) {
    return computers.get(statsComputerName);
  }
}
