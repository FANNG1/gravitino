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

package org.apache.gravitino.maintenance.optimizer.monitor.service;

import java.util.List;

// detail monitor response
public class MonitorResponse {
  public final String monitorId;
  // it could be table monitor or partition monitor
  public final TableMonitorDetailInfo tableMonitorDetailInfo;
  public final List<JobMonitorDetailInfo> jobMonitorDetailInfoList;

  public MonitorResponse(
      String monitorId,
      TableMonitorDetailInfo tableMonitorDetailInfo,
      List<JobMonitorDetailInfo> jobMonitorDetailInfoList) {
    this.monitorId = monitorId;
    this.tableMonitorDetailInfo = tableMonitorDetailInfo;
    this.jobMonitorDetailInfoList = jobMonitorDetailInfoList;
  }
}
