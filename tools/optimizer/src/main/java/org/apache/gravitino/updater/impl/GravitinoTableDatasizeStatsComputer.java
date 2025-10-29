package org.apache.gravitino.updater.impl;

import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.monitor.api.Metrics;
import org.apache.gravitino.updater.api.MetricsComputer;
import org.apache.gravitino.updater.api.OStatistic;
import org.apache.gravitino.updater.api.SupportTableStats;

public class GravitinoTableDatasizeStatsComputer implements SupportTableStats, MetricsComputer {
  @Override
  public String name() {
    return "gravitino-table-datasize";
  }

  @Override
  public Metrics computeMetrics(NameIdentifier tableIdentifier) {
    return null;
  }

  @Override
  public List<OStatistic> computeTableStats(NameIdentifier tableIdentifier) {
    // generate deletefile_number, datafile_mse, total_file_number, etc from Iceberg file metadata
    // or Gravitino file stats.
    return new java.util.ArrayList<>();
  }
}
