package org.apache.gravitino.updater.api;

import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.stats.PartitionStatistics;

// The stats provider to compute partition stats, the stats will be used to update Gravitino stats
// store or exteranl systems.
public interface SupportPartitionStats {
  /**
   * Compute the partition statistics for the given table identifier.
   *
   * @param tableIdentifier The identifier of the table.
   * @return The partition statistics for the table.
   */
  List<PartitionStatistics> computePartitionStats(NameIdentifier tableIdentifier);
}
