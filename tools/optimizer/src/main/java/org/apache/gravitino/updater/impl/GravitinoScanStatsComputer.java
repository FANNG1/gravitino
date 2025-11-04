package org.apache.gravitino.updater.impl;

import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.updater.api.SingleStatistic;
import org.apache.gravitino.updater.api.SupportTableStats;

public class GravitinoScanStatsComputer implements SupportTableStats {
  @Override
  public String name() {
    return "gravitino-table-scan";
  }

  @Override
  public List<SingleStatistic<?>> computeTableStats(NameIdentifier tableIdentifier) {
    // table_scan_number, column_scan_number, low_filter_number from Iceberg Scan metrics store
    return new java.util.ArrayList<>();
  }
}
