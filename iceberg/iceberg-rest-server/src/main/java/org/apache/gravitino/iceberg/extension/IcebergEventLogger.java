/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.extension;

import java.util.Map;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.IcebergCreateTablePostEvent;
import org.apache.gravitino.listener.api.event.IcebergCreateTablePreEvent;
import org.apache.gravitino.listener.api.event.IcebergUpdateTablePostEvent;
import org.apache.gravitino.listener.api.event.IcebergUpdateTablePreEvent;
import org.apache.gravitino.listener.api.event.PreEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergEventLogger implements EventListenerPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergEventLogger.class);

  @Override
  public void init(Map<String, String> properties) throws RuntimeException {}

  @Override
  public void start() throws RuntimeException {}

  @Override
  public void stop() throws RuntimeException {}

  @Override
  public void onPostEvent(Event event) throws RuntimeException {
    if (event instanceof IcebergCreateTablePostEvent) {
      LOG.info(
          "Create table event, request: {}",
          ((IcebergCreateTablePostEvent) event).createTableRequest());
    } else if (event instanceof IcebergUpdateTablePostEvent) {
      LOG.info(
          "Update table event, request: {}",
          ((IcebergUpdateTablePostEvent) event).updateTableRequest());
    } else {
      LOG.info("Unknown event: {}", event.getClass().getSimpleName());
    }
  }

  @Override
  public void onPreEvent(PreEvent preEvent) throws RuntimeException {
    if (preEvent instanceof IcebergCreateTablePreEvent) {
      LOG.info(
          "Create table event, request: {}",
          ((IcebergCreateTablePreEvent) preEvent).createTableRequest());
    } else if (preEvent instanceof IcebergUpdateTablePreEvent) {
      LOG.info(
          "Update table event, request: {}",
          ((IcebergUpdateTablePreEvent) preEvent).updateTableRequest());
    } else {
      LOG.info("Unknown event: {}", preEvent.getClass().getSimpleName());
    }
  }

  @Override
  public Mode mode() {
    return Mode.ASYNC_ISOLATED;
  }
}
