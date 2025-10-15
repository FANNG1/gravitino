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

import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricSample;

final class MonitorState {
  private final String monitorId;
  private final MonitorDetailBundle detailBundle;
  private final long lastEvaluatedAtSeconds;

  private MonitorState(
      String monitorId,
      TableMonitorDetailInfo tableDetailInfo,
      List<JobMonitorDetailInfo> jobMonitorDetailInfoList,
      long lastEvaluatedAtSeconds) {
    this.monitorId = monitorId;
    this.detailBundle = new MonitorDetailBundle(tableDetailInfo, jobMonitorDetailInfoList);
    this.lastEvaluatedAtSeconds = lastEvaluatedAtSeconds;
  }

  static String computeMonitorId(String tableIdentifier, String partitionPath) {
    String normalizedPartition = partitionPath == null ? "" : partitionPath;
    String key = tableIdentifier + "#" + normalizedPartition;
    return Hashing.sha256().hashString(key, StandardCharsets.UTF_8).toString();
  }

  static MonitorState queued(
      String monitorId,
      String tableIdentifier,
      String partitionPath,
      long actionTimeSeconds,
      long rangeSeconds) {
    return new MonitorState(
        monitorId,
        buildDetailInfo(
            tableIdentifier,
            partitionPath,
            actionTimeSeconds,
            rangeSeconds,
            MonitorRequestState.QUEUED,
            null,
            null),
        Collections.emptyList(),
        0L);
  }

  MonitorState withState(MonitorRequestState state) {
    return new MonitorState(
        monitorId,
        buildDetailInfo(
            tableIdentifier(),
            partitionPath(),
            actionTimeSeconds(),
            rangeSeconds(),
            state,
            beforeMetrics(),
            afterMetrics()),
        detailBundle.jobMonitorDetailInfoList,
        lastEvaluatedAtSeconds);
  }

  MonitorState withRequest(long actionTimeSeconds, long rangeSeconds) {
    return new MonitorState(
        monitorId,
        buildDetailInfo(
            tableIdentifier(),
            partitionPath(),
            actionTimeSeconds,
            rangeSeconds,
            MonitorRequestState.QUEUED,
            null,
            null),
        Collections.emptyList(),
        0L);
  }

  MonitorState withEvaluationResults(
      MonitorRequestState state,
      Map<String, List<MetricSample>> beforeMetrics,
      Map<String, List<MetricSample>> afterMetrics,
      List<JobMonitorDetailInfo> jobMonitorDetailInfoList,
      long lastEvaluatedAtSeconds) {
    return new MonitorState(
        monitorId,
        buildDetailInfo(
            tableIdentifier(),
            partitionPath(),
            actionTimeSeconds(),
            rangeSeconds(),
            state,
            beforeMetrics,
            afterMetrics),
        jobMonitorDetailInfoList,
        lastEvaluatedAtSeconds);
  }

  MonitorResponse toResponse() {
    return new MonitorResponse(
        monitorId, detailBundle.tableDetailInfo, detailBundle.jobMonitorDetailInfoList);
  }

  MonitorListItem toListItem() {
    MonitorBasicInfo basicInfo = new MonitorBasicInfo();
    basicInfo.identifier = tableIdentifier();
    basicInfo.partitionPath = partitionPath();
    basicInfo.actionTimeSeconds = actionTimeSeconds();
    basicInfo.rangeSeconds = rangeSeconds();
    return new MonitorListItem(monitorId, basicInfo);
  }

  MonitorRequestState state() {
    return detailBundle.tableDetailInfo.state;
  }

  String monitorId() {
    return monitorId;
  }

  String tableIdentifier() {
    return detailBundle.tableDetailInfo.tableIdentifier;
  }

  String partitionPath() {
    if (detailBundle.tableDetailInfo instanceof PartitionMonitorDetailInfo) {
      return ((PartitionMonitorDetailInfo) detailBundle.tableDetailInfo).partitionPath;
    }
    return null;
  }

  long actionTimeSeconds() {
    return detailBundle.tableDetailInfo.actionTimeSeconds;
  }

  long rangeSeconds() {
    return detailBundle.tableDetailInfo.rangeSeconds;
  }

  long lastEvaluatedAtSeconds() {
    return lastEvaluatedAtSeconds;
  }

  private Map<String, List<MetricSample>> beforeMetrics() {
    return detailBundle.tableDetailInfo.beforeMetrics;
  }

  private Map<String, List<MetricSample>> afterMetrics() {
    return detailBundle.tableDetailInfo.afterMetrics;
  }

  private static TableMonitorDetailInfo buildDetailInfo(
      String tableIdentifier,
      String partitionPath,
      long actionTimeSeconds,
      long rangeSeconds,
      MonitorRequestState state,
      Map<String, List<MetricSample>> beforeMetrics,
      Map<String, List<MetricSample>> afterMetrics) {
    if (partitionPath == null || partitionPath.isEmpty()) {
      return new TableMonitorDetailInfo(
          tableIdentifier, actionTimeSeconds, rangeSeconds, state, beforeMetrics, afterMetrics);
    }
    return new PartitionMonitorDetailInfo(
        tableIdentifier,
        partitionPath,
        actionTimeSeconds,
        rangeSeconds,
        state,
        beforeMetrics,
        afterMetrics);
  }
}
