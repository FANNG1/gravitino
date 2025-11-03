package org.apache.gravitino.updater.api;

import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.monitor.api.SingleMetric;

/*
 * Job metrics:
 *    a. job_runtime_metrics, job_name:xx,  xx
 *    b. job_cost_metrics, job_name:xx, xx
 *    c. job_s3_request_metrics, job_name:xx xx
 * Table metrics:
 *    a. table_storage_metrics, table_name:xx, partition_name:xx xx
 *    b. table_file_count_metrics, table_name:xx, partition_name:xx xx
 *    c. table_delete_file_number_metrics, table_name:xx, partition_name:xx xx
 *    d. table_datafile_mse_metrics, table_name:xx, partition_name:xx xx
 */
public interface MetricsUpdater {
  void updateTableMetrics(NameIdentifier nameIdentifier, List<SingleMetric> metrics);

  void updateJobMetrics(NameIdentifier nameIdentifier, List<SingleMetric> metrics);
}
