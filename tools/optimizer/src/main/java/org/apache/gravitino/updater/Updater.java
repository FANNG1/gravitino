package org.apache.gravitino.updater;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.common.BaseMetric;
import org.apache.gravitino.monitor.api.SingleMetric;
import org.apache.gravitino.updater.api.BaseStatistic;
import org.apache.gravitino.updater.api.MetricsUpdater;
import org.apache.gravitino.updater.api.StatsComputer;
import org.apache.gravitino.updater.api.StatsUpdater;
import org.apache.gravitino.updater.api.SupportJobStats;
import org.apache.gravitino.updater.api.SupportTableStats;

public class Updater {
  private Map<String, StatsComputer> computers = new HashMap<>();
  private StatsUpdater statsUpdater;
  private MetricsUpdater metricsUpdater;

  public void update(
      String statsComputerName, List<NameIdentifier> nameIdentifiers, UpdateType updateType) {
    StatsComputer computer = getStatsComputer(statsComputerName);
    for (NameIdentifier nameIdentifier : nameIdentifiers) {
      if (computer instanceof SupportTableStats) {
        SupportTableStats supportTableStats = ((SupportTableStats) computer);
        List<BaseStatistic<?>> statistics = supportTableStats.computeTableStats(nameIdentifier);
        updateTable(statistics, nameIdentifier, updateType);
      } else if (computer instanceof SupportJobStats) {
        Preconditions.checkState(
            updateType.equals(UpdateType.METRICS), "Job stats only support metrics update");
        SupportJobStats supportJobStats = ((SupportJobStats) computer);
        List<BaseStatistic<?>> statistics = supportJobStats.computeJobStats(nameIdentifier);
        updateJob(statistics, nameIdentifier);
      } else {
        throw new UnsupportedOperationException(
            String.format("Stats computer %s does not support %s", statsComputerName, updateType));
      }
    }
  }

  private void updateTable(
      List<BaseStatistic<?>> statistics, NameIdentifier tableIdentifier, UpdateType updateType) {
    switch (updateType) {
      case STATS:
        statsUpdater.updateTableStatistics(tableIdentifier, statistics);
        break;
      case METRICS:
        metricsUpdater.updateTableMetrics(tableIdentifier, toMetrics(statistics));
        break;
    }
  }

  private void updateJob(List<BaseStatistic<?>> statistics, NameIdentifier jobIdentifier) {
    metricsUpdater.updateJobMetrics(jobIdentifier, toMetrics(statistics));
  }

  private List<SingleMetric> toMetrics(List<BaseStatistic<?>> statistics) {
    return statistics.stream()
        .map(stat -> new BaseMetric(System.currentTimeMillis(), stat))
        .toList();
  }

  private StatsComputer getStatsComputer(String statsComputerName) {
    return computers.get(statsComputerName);
  }
}
