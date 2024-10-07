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

package org.apache.gravitino.listener;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.EventListenerPlugin.PreEventCheckException;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.PreEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code EventBus} class serves as a mechanism to dispatch events to registered listeners. It
 * supports both synchronous and asynchronous listeners by categorizing them into two distinct types
 * within its internal management.
 */
public class EventBus {
  private static final Logger LOG = LoggerFactory.getLogger(EventBus.class);

  // Holds instances of EventListenerPlugin. These instances can either be
  // EventListenerPluginWrapper,
  // which are meant for synchronous event listening, or AsyncQueueListener, designed for
  // asynchronous event processing.
  private final List<EventListenerPlugin> postEventListeners;
  private final List<EventListenerPlugin> preEventListeners;

  /**
   * Constructs an EventBus with a predefined list of event listeners.
   *
   * @param eventListenerPlugins A list of {@link EventListenerPlugin} instances that are to be
   *     registered with this EventBus for event dispatch.
   */
  public EventBus(List<EventListenerPlugin> eventListenerPlugins) {
    this.postEventListeners = eventListenerPlugins;
    // todo: use a more general way to create preEventListeners
    this.preEventListeners =
        postEventListeners.stream()
            .filter(eventListenerPlugin -> !(eventListenerPlugin instanceof AsyncQueueListener))
            .collect(Collectors.toList());
  }

  /**
   * Dispatches an event to all registered listeners. Each listener processes the event based on its
   * implementation, which could be either synchronous or asynchronous.
   *
   * @param event The event to be dispatched to all registered listeners.
   */
  public void dispatchEvent(Event event) {
    if (event instanceof PreEvent) {
      dispatchPreEvent((PreEvent) event);
    } else {
      dispatchPostEvent(event);
    }
  }

  /**
   * Retrieves the list of registered post-event listeners. This method is primarily intended for
   * testing purposes to verify the correct registration and functioning of event listeners.
   *
   * @return A list of {@link EventListenerPlugin} instances currently registered with this
   *     EventBus.
   */
  @VisibleForTesting
  List<EventListenerPlugin> getPostEventListeners() {
    return postEventListeners;
  }

  public void dispatchPreEvent(PreEvent event) {
    preEventListeners.forEach(
        preEventListener -> {
          try {
            preEventListener.onPreEvent(event);
          } catch (PreEventCheckException e) {
            // todo transform to ValidateException for IcebergRESTServer
            throw new RuntimeException(e);
          } catch (Exception e) {
            LOG.warn("PreEvent handle exception,", e);
          }
        });
  }

  private void dispatchPostEvent(Event event) {
    postEventListeners.forEach(postEventListener -> postEventListener.onPostEvent(event));
  }
}
