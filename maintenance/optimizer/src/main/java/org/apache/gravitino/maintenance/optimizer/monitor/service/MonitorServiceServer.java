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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.Config;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.monitor.Monitor;
import org.apache.gravitino.maintenance.optimizer.monitor.service.rest.HealthResource;
import org.apache.gravitino.maintenance.optimizer.monitor.service.rest.MonitorRequestResource;
import org.apache.gravitino.maintenance.optimizer.monitor.service.rest.MonitorServiceExceptionMapper;
import org.apache.gravitino.server.web.JettyServer;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

public class MonitorServiceServer implements MonitorService {

  private static final String REST_SPEC = "/*";
  private final OptimizerEnv optimizerEnv;
  private final int configuredPort;
  private JettyServer server;
  private int localPort = -1;
  private boolean running;
  private final MonitorStateStore monitorStateStore = new InMemoryMonitorStateStore();
  private ExecutorService executor;
  private ScheduledExecutorService scheduler;
  private Monitor monitor;
  private MonitorStateManager requestHandler;

  public MonitorServiceServer(OptimizerEnv optimizerEnv) {
    this.optimizerEnv = optimizerEnv;
    this.configuredPort = optimizerEnv.config().get(OptimizerConfig.MONITOR_SERVICE_PORT_CONFIG);
  }

  @Override
  public synchronized void start() throws Exception {
    if (server != null) {
      return;
    }

    server = new JettyServer();
    server.initialize(buildJettyConfig(), "monitor-service", false);
    monitor = new Monitor(optimizerEnv);
    executor = Executors.newSingleThreadExecutor(new MonitorThreadFactory());
    requestHandler = new MonitorStateManager(monitor, executor, monitorStateStore);
    startScheduler();
    server.addServlet(buildServletContainer(), REST_SPEC);
    server.start();
    localPort = server.getLocalPort();
    running = true;
  }

  @Override
  public synchronized void stop() throws Exception {
    if (server == null) {
      return;
    }
    try {
      server.stop();
    } finally {
      monitorStateStore.close();
      if (executor != null) {
        executor.shutdownNow();
        executor = null;
      }
      if (scheduler != null) {
        scheduler.shutdownNow();
        scheduler = null;
      }
      requestHandler = null;
      server = null;
      localPort = -1;
      running = false;
    }
  }

  @Override
  public synchronized boolean isRunning() {
    return running;
  }

  @Override
  public synchronized int localPort() {
    return localPort > 0 ? localPort : configuredPort;
  }

  private JettyServerConfig buildJettyConfig() {
    Map<String, String> configs =
        ImmutableMap.of(
            JettyServerConfig.WEBSERVER_HTTP_PORT.getKey(), String.valueOf(configuredPort));
    Config config = new Config(false) {};
    config.loadFromMap(configs, key -> true);
    return JettyServerConfig.fromConfig(config);
  }

  private ServletContainer buildServletContainer() {
    ResourceConfig config = new ResourceConfig();
    config.register(JacksonFeature.class);
    config.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(requestHandler).to(MonitorStateManager.class);
          }
        });
    config.register(HealthResource.class);
    config.register(MonitorRequestResource.class);
    config.register(MonitorServiceExceptionMapper.class);
    return new ServletContainer(config);
  }

  private void startScheduler() {
    int intervalSeconds =
        optimizerEnv.config().get(OptimizerConfig.MONITOR_SERVICE_INTERVAL_SECONDS_CONFIG);
    scheduler = Executors.newSingleThreadScheduledExecutor(new MonitorThreadFactory());
    scheduler.scheduleAtFixedRate(
        requestHandler::runScheduledChecks, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
  }

  private static class MonitorThreadFactory implements ThreadFactory {
    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public Thread newThread(Runnable runnable) {
      Thread thread = new Thread(runnable, "monitor-service-" + counter.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    }
  }
}
