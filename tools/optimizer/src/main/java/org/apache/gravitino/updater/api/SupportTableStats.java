package org.apache.gravitino.updater.api;

import java.util.List;
import org.apache.gravitino.NameIdentifier;

// The stats provider to compute table stats, the stats will be used to update Gravitino stats store
// or external systems.
public interface SupportTableStats extends StatsComputer {
  List<SingleStatistic<?>> computeTableStats(NameIdentifier tableIdentifier);
}
