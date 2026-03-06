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

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.DataScope;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.monitor.EvaluationResult;
import org.apache.gravitino.maintenance.optimizer.monitor.Monitor;
import org.apache.gravitino.maintenance.optimizer.monitor.service.rest.MonitorServiceException;
import org.apache.gravitino.maintenance.optimizer.recommender.util.PartitionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitorStateManager {

  private static final Logger LOG = LoggerFactory.getLogger(MonitorStateManager.class);

  private final Monitor monitor;
  private final ExecutorService executor;
  private final MonitorStateStore monitorStateStore;

  public MonitorStateManager(
      Monitor monitor, ExecutorService executor, MonitorStateStore monitorStateStore) {
    this.monitor = monitor;
    this.executor = executor;
    this.monitorStateStore = monitorStateStore;
  }

  public MonitorSubmitResponse submit(MonitorSubmitRequest payload) {
    if (executor == null || executor.isShutdown()) {
      throw new MonitorServiceException(Response.Status.SERVICE_UNAVAILABLE, "Service not started");
    }

    MonitorRequest request = validateRequest(payload);
    String partitionPath = StringUtils.defaultIfBlank(payload.partitionPath, null);
    String monitorId = MonitorState.computeMonitorId(request.identifier.toString(), partitionPath);
    MonitorState state =
        monitorStateStore.compute(
            monitorId,
            existing ->
                existing == null
                    ? MonitorState.queued(
                        monitorId,
                        request.identifier.toString(),
                        partitionPath,
                        request.actionTimeSeconds,
                        request.rangeSeconds)
                    : existing.withRequest(request.actionTimeSeconds, request.rangeSeconds));
    executor.submit(() -> executeRequest(monitorId, request));
    return new MonitorSubmitResponse(state.toResponse().monitorId);
  }

  public Collection<MonitorListItem> listMonitors() {
    return monitorStateStore.listIds().stream()
        .map(monitorStateStore::get)
        .flatMap(java.util.Optional::stream)
        .map(MonitorState::toListItem)
        .collect(java.util.stream.Collectors.toList());
  }

  public void runScheduledChecks() {
    if (executor == null || executor.isShutdown()) {
      LOG.info("Skip scheduled checks: executor is not available or shut down");
      return;
    }
    List<String> monitorIds = monitorStateStore.listIds();
    LOG.info("Running scheduled checks for {} monitors", monitorIds.size());
    for (String monitorId : monitorIds) {
      MonitorState state = monitorStateStore.get(monitorId).orElse(null);
      if (state == null) {
        LOG.info("Skip scheduled check for monitor {}: state not found", monitorId);
        continue;
      }
      if (state.state() == MonitorRequestState.CANCELED
          || state.state() == MonitorRequestState.RUNNING) {
        LOG.info("Skip scheduled check for monitor {}: state={}", monitorId, state.state());
        continue;
      }
      if (isExpired(state)) {
        LOG.info(
            "Finalize expired monitor {}: actionTimeSeconds={}, rangeSeconds={}, state={}",
            monitorId,
            state.actionTimeSeconds(),
            state.rangeSeconds(),
            state.state());
        finalizeIfExpired(state);
        continue;
      }
      LOG.info(
          "Schedule monitor evaluation for {}: state={}, actionTimeSeconds={}, rangeSeconds={}",
          monitorId,
          state.state(),
          state.actionTimeSeconds(),
          state.rangeSeconds());
      MonitorRequest request = toRequest(state);
      executor.submit(() -> executeRequest(state, request));
    }
  }

  public MonitorResponse getStatus(String monitorId) {
    if (StringUtils.isBlank(monitorId)) {
      throw new MonitorServiceException(Response.Status.BAD_REQUEST, "monitorId is required");
    }

    MonitorState state = monitorStateStore.get(monitorId).orElse(null);
    if (state == null) {
      throw new MonitorServiceException(
          Response.Status.NOT_FOUND, "Unknown monitorId: " + monitorId);
    }
    return state.toResponse();
  }

  public MonitorResponse cancel(String monitorId) {
    if (StringUtils.isBlank(monitorId)) {
      throw new MonitorServiceException(Response.Status.BAD_REQUEST, "monitorId is required");
    }
    MonitorState state =
        monitorStateStore.compute(
            monitorId,
            existing -> {
              if (existing == null) {
                return null;
              }
              return existing.withState(MonitorRequestState.CANCELED);
            });
    if (state == null) {
      throw new MonitorServiceException(
          Response.Status.NOT_FOUND, "Unknown monitorId: " + monitorId);
    }
    return state.toResponse();
  }

  private void executeRequest(String monitorId, MonitorRequest request) {
    MonitorState current = monitorStateStore.get(monitorId).orElse(null);
    if (current != null) {
      executeRequest(current, request);
      return;
    }
    executeRequest(
        MonitorState.queued(
            monitorId,
            request.identifier.toString(),
            request.partitionPath == null ? null : request.partitionPath.toString(),
            request.actionTimeSeconds,
            request.rangeSeconds),
        request);
  }

  private void executeRequest(MonitorState current, MonitorRequest request) {
    LOG.info(
        "Execute monitor {}: identifier={}, partitionPath={}, state={}, actionTimeSeconds={}, rangeSeconds={}",
        current.monitorId(),
        current.tableIdentifier(),
        current.partitionPath(),
        current.state(),
        current.actionTimeSeconds(),
        current.rangeSeconds());
    if (current.state() == MonitorRequestState.CANCELED
        || current.state() == MonitorRequestState.RUNNING) {
      LOG.info("Skip monitor {}: state={}", current.monitorId(), current.state());
      return;
    }
    if (isExpired(current)) {
      LOG.info(
          "Skip monitor {}: expired at {}, state={}",
          current.monitorId(),
          current.actionTimeSeconds() + current.rangeSeconds(),
          current.state());
      finalizeIfExpired(current);
      return;
    }
    long lastEvaluatedAtSeconds = current.lastEvaluatedAtSeconds();
    long latestTimestamp =
        monitor.latestMetricTimestampSeconds(
            request.identifier,
            request.actionTimeSeconds,
            request.rangeSeconds,
            java.util.Optional.ofNullable(request.partitionPath));
    if (latestTimestamp <= lastEvaluatedAtSeconds) {
      LOG.info(
          "Skip monitor {}: no new metrics (latestTimestamp={}, lastEvaluatedAtSeconds={})",
          current.monitorId(),
          latestTimestamp,
          lastEvaluatedAtSeconds);
      return;
    }
    MonitorState running =
        monitorStateStore.compute(
            current.monitorId(),
            status -> {
              if (status == null) {
                return current.withState(MonitorRequestState.RUNNING);
              }
              if (status.state() == MonitorRequestState.CANCELED
                  || status.state() == MonitorRequestState.RUNNING) {
                return status;
              }
              return status.withState(MonitorRequestState.RUNNING);
            });
    if (running == null || running.state() != MonitorRequestState.RUNNING) {
      LOG.info(
          "Skip monitor {}: unable to transition to RUNNING (currentState={})",
          current.monitorId(),
          running == null ? null : running.state());
      return;
    }
    try {
      LOG.info(
          "Run monitor {}: identifier={}, partitionPath={}, actionTimeSeconds={}, rangeSeconds={}",
          running.monitorId(),
          request.identifier,
          request.partitionPath,
          request.actionTimeSeconds,
          request.rangeSeconds);
      List<EvaluationResult> results =
          monitor.evaluateMetrics(
              request.identifier,
              request.actionTimeSeconds,
              request.rangeSeconds,
              java.util.Optional.ofNullable(request.partitionPath));
      boolean expired = isExpired(current);
      MonitorRequestState newState = resolveStateForEvaluation(expired, results);
      MonitorState updated =
          buildSuccessState(
              running,
              results,
              newState,
              Math.max(latestTimestamp, latestTimestampFromResults(results)));
      monitorStateStore.compute(running.monitorId(), status -> status == null ? null : updated);
      LOG.info(
          "Updated monitor {}: state={}, lastEvaluatedAtSeconds={}",
          running.monitorId(),
          newState,
          updated.lastEvaluatedAtSeconds());
    } catch (Exception e) {
      LOG.warn("Monitor request {} failed", current.monitorId(), e);
      boolean expired = isExpired(current);
      MonitorRequestState failedState =
          expired ? MonitorRequestState.FAILED : MonitorRequestState.PARTIAL_FAILED;
      monitorStateStore.compute(
          current.monitorId(), status -> status == null ? null : status.withState(failedState));
      LOG.info("Updated monitor {}: state={}", current.monitorId(), failedState);
    }
  }

  private MonitorRequest validateRequest(MonitorSubmitRequest payload) {
    if (payload == null) {
      throw new MonitorServiceException(Response.Status.BAD_REQUEST, "Missing request payload");
    }
    if (StringUtils.isBlank(payload.identifier)) {
      throw new MonitorServiceException(Response.Status.BAD_REQUEST, "identifier is required");
    }
    if (payload.actionTimeSeconds <= 0) {
      throw new MonitorServiceException(
          Response.Status.BAD_REQUEST, "actionTimeSeconds must be > 0");
    }
    if (payload.rangeSeconds < 0) {
      throw new MonitorServiceException(Response.Status.BAD_REQUEST, "rangeSeconds must be >= 0");
    }

    PartitionPath partitionPath = null;
    if (StringUtils.isNotBlank(payload.partitionPath)) {
      try {
        partitionPath = PartitionUtils.parseLegacyPartitionPath(payload.partitionPath);
      } catch (IllegalArgumentException e) {
        throw new MonitorServiceException(
            Response.Status.BAD_REQUEST, "Invalid partitionPath: " + payload.partitionPath);
      }
    }

    NameIdentifier identifier;
    try {
      identifier = NameIdentifier.parse(payload.identifier);
    } catch (IllegalArgumentException e) {
      throw new MonitorServiceException(
          Response.Status.BAD_REQUEST, "Invalid identifier: " + payload.identifier);
    }

    return new MonitorRequest(
        identifier, payload.actionTimeSeconds, payload.rangeSeconds, partitionPath);
  }

  private MonitorRequest toRequest(MonitorState state) {
    NameIdentifier identifier = NameIdentifier.parse(state.tableIdentifier());
    PartitionPath partitionPath =
        StringUtils.isBlank(state.partitionPath())
            ? null
            : PartitionUtils.parseLegacyPartitionPath(state.partitionPath());
    return new MonitorRequest(
        identifier, state.actionTimeSeconds(), state.rangeSeconds(), partitionPath);
  }

  private static MonitorState buildSuccessState(
      MonitorState current,
      List<EvaluationResult> results,
      MonitorRequestState newState,
      long lastEvaluatedAtSeconds) {
    Map<String, List<org.apache.gravitino.maintenance.optimizer.api.common.MetricSample>>
        tableBefore = null;
    Map<String, List<org.apache.gravitino.maintenance.optimizer.api.common.MetricSample>>
        tableAfter = null;
    List<JobMonitorDetailInfo> jobDetails = new java.util.ArrayList<>();

    for (EvaluationResult result : results) {
      DataScope scope = result.scope();
      if (scope.type() == DataScope.Type.TABLE || scope.type() == DataScope.Type.PARTITION) {
        tableBefore = result.beforeMetrics();
        tableAfter = result.afterMetrics();
      } else if (scope.type() == DataScope.Type.JOB) {
        MonitorRequestState jobState =
            result.evaluation() ? MonitorRequestState.SUCCEEDED : MonitorRequestState.FAILED;
        jobDetails.add(
            new JobMonitorDetailInfo(
                scope.identifier().toString(),
                jobState,
                result.beforeMetrics(),
                result.afterMetrics()));
      }
    }

    return current.withEvaluationResults(
        newState, tableBefore, tableAfter, jobDetails, lastEvaluatedAtSeconds);
  }

  private boolean isExpired(MonitorState state) {
    long expireAt = state.actionTimeSeconds() + state.rangeSeconds();
    return Instant.now().getEpochSecond() > expireAt;
  }

  private void finalizeIfExpired(MonitorState state) {
    MonitorRequestState finalState = resolveFinalState(state.state());
    if (finalState == state.state()) {
      return;
    }
    monitorStateStore.compute(
        state.monitorId(), existing -> existing == null ? null : existing.withState(finalState));
  }

  private MonitorRequestState resolveFinalState(MonitorRequestState current) {
    return switch (current) {
      case PARTIAL_SUCCESS -> MonitorRequestState.SUCCEEDED;
      case PARTIAL_FAILED, FAILED, QUEUED -> MonitorRequestState.FAILED;
      default -> current;
    };
  }

  private MonitorRequestState resolveStateForEvaluation(
      boolean expired, List<EvaluationResult> results) {
    Boolean evaluation = null;
    for (EvaluationResult result : results) {
      DataScope scope = result.scope();
      if (scope.type() == DataScope.Type.TABLE || scope.type() == DataScope.Type.PARTITION) {
        if (evaluation != null) {
          LOG.warn("Multiple table/partition evaluation results found; using the first value");
          break;
        }
        evaluation = result.evaluation();
      }
    }
    if (evaluation == null) {
      evaluation = false;
    }
    if (expired) {
      return evaluation ? MonitorRequestState.SUCCEEDED : MonitorRequestState.FAILED;
    }
    return evaluation ? MonitorRequestState.PARTIAL_SUCCESS : MonitorRequestState.PARTIAL_FAILED;
  }

  private static long latestTimestampFromResults(List<EvaluationResult> results) {
    long max = 0L;
    for (EvaluationResult result : results) {
      max = Math.max(max, maxTimestamp(result.beforeMetrics()));
      max = Math.max(max, maxTimestamp(result.afterMetrics()));
    }
    return max;
  }

  private static long maxTimestamp(
      Map<String, List<org.apache.gravitino.maintenance.optimizer.api.common.MetricSample>>
          metrics) {
    if (metrics == null || metrics.isEmpty()) {
      return 0L;
    }
    return metrics.values().stream()
        .flatMap(List::stream)
        .mapToLong(
            org.apache.gravitino.maintenance.optimizer.api.common.MetricSample::timestampSeconds)
        .max()
        .orElse(0L);
  }
}
