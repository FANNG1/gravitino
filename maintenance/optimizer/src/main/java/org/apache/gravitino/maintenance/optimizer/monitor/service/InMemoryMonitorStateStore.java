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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.UnaryOperator;

final class InMemoryMonitorStateStore implements MonitorStateStore {
  private final ConcurrentMap<String, MonitorState> states = new ConcurrentHashMap<>();

  @Override
  public Optional<MonitorState> get(String monitorId) {
    return Optional.ofNullable(states.get(monitorId));
  }

  @Override
  public List<String> listIds() {
    return new ArrayList<>(states.keySet());
  }

  @Override
  public MonitorState compute(String monitorId, UnaryOperator<MonitorState> updater) {
    return states.compute(monitorId, (id, existing) -> updater.apply(existing));
  }

  @Override
  public void put(MonitorState state) {
    states.put(state.monitorId(), state);
  }

  @Override
  public boolean delete(String monitorId) {
    return states.remove(monitorId) != null;
  }

  @Override
  public void close() {}
}
