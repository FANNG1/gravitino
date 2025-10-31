package org.apache.gravitino.updater.api;

import java.util.List;
import org.apache.gravitino.NameIdentifier;

// Update the statistics to Gravitino stats store or external systems.
public interface StatsUpdater {
  void updateTableStatistics(NameIdentifier tableIdentifier, List<BaseStatistic<?>> statistics);
}
