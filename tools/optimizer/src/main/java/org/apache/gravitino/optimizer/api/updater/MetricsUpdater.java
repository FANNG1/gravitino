/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.optimizer.api.updater;

import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.optimizer.api.common.Provider;
import org.apache.gravitino.optimizer.api.common.SingleMetric;

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
public interface MetricsUpdater extends Provider {
  void updateTableMetrics(NameIdentifier nameIdentifier, List<SingleMetric> metrics);

  void updateJobMetrics(NameIdentifier nameIdentifier, List<SingleMetric> metrics);
}
