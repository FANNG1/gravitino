package org.apache.gravitino.monitor.api;

import java.util.List;
import org.apache.gravitino.NameIdentifier;

/**
 * Provider interface for retrieving table-related metrics. Implementations of this interface should
 * provide specific logic to fetch metrics for a given table.
 */
public interface TableMetricsProvider {
  List<Metrics> metrics(NameIdentifier tableIdentifier, long startTime, long endTime);
}
