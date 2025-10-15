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

public class MonitorRequestStatus {
  public final String requestId;
  public final MonitorRequestState state;
  public final long submittedAtMillis;
  public final Long startedAtMillis;
  public final Long finishedAtMillis;
  public final String errorMessage;

  private MonitorRequestStatus(
      String requestId,
      MonitorRequestState state,
      long submittedAtMillis,
      Long startedAtMillis,
      Long finishedAtMillis,
      String errorMessage) {
    this.requestId = requestId;
    this.state = state;
    this.submittedAtMillis = submittedAtMillis;
    this.startedAtMillis = startedAtMillis;
    this.finishedAtMillis = finishedAtMillis;
    this.errorMessage = errorMessage;
  }

  static MonitorRequestStatus queued(String requestId) {
    return new MonitorRequestStatus(
        requestId, MonitorRequestState.QUEUED, System.currentTimeMillis(), null, null, null);
  }

  static MonitorRequestStatus running(String requestId) {
    long now = System.currentTimeMillis();
    return new MonitorRequestStatus(requestId, MonitorRequestState.RUNNING, now, now, null, null);
  }

  MonitorRequestStatus withState(MonitorRequestState state) {
    long now = System.currentTimeMillis();
    if (state == MonitorRequestState.RUNNING) {
      return new MonitorRequestStatus(requestId, state, submittedAtMillis, now, null, null);
    }
    if (state == MonitorRequestState.SUCCEEDED) {
      return new MonitorRequestStatus(
          requestId, state, submittedAtMillis, startedAtMillis, now, null);
    }
    return new MonitorRequestStatus(
        requestId, state, submittedAtMillis, startedAtMillis, finishedAtMillis, errorMessage);
  }

  MonitorRequestStatus withFailure(MonitorRequestState state, String errorMessage) {
    long now = System.currentTimeMillis();
    return new MonitorRequestStatus(
        requestId, state, submittedAtMillis, startedAtMillis, now, errorMessage);
  }
}
