/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.iceberg.integration.test.util;

import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.CommandExecutor;
import com.datastrato.gravitino.integration.test.util.ProcessData;
import com.datastrato.gravitino.server.GravitinoServer;
import com.datastrato.gravitino.server.web.JettyServerConfig;
import java.util.concurrent.TimeUnit;
import org.testcontainers.shaded.org.awaitility.Awaitility;

public class IcebergRESTServerManagerForDeploy extends IcebergRESTServerManager {
  private static final String SCRIPT_NAME = "iceberg-rest-server.sh";

  @Override
  public void startIcebergRESTServer() throws Exception {
    CommandExecutor.executeCommandLocalHost(
        System.getenv("GRAVITINO_HOME") + "/bin/" + SCRIPT_NAME + " start",
        false,
        ProcessData.TypesOfData.OUTPUT);
    sleep(3000, false);

    serverConfig.loadFromFile(GravitinoServer.CONF_FILE);

    JettyServerConfig jettyServerConfig =
        JettyServerConfig.fromConfig(
            serverConfig, JettyServerConfig.GRAVITINO_SERVER_CONFIG_PREFIX);
    String checkServerUrl =
        "http://"
            + jettyServerConfig.getHost()
            + ":"
            + jettyServerConfig.getHttpPort()
            + "/metrics";
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(() -> AbstractIT.isHttpServerUp(checkServerUrl));
  }

  @Override
  public void stopIcebergRESTServer() {
    CommandExecutor.executeCommandLocalHost(
        System.getenv("GRAVITINO_HOME") + "/bin/" + SCRIPT_NAME + " stop",
        false,
        ProcessData.TypesOfData.OUTPUT);
    // wait for server to stop.
    sleep(1000, false);
  }

  private static void sleep(long millis, boolean logOutput) {
    if (logOutput && LOG.isInfoEnabled()) {
      LOG.info("Starting sleeping for {} milliseconds ...", millis);
      LOG.info("Caller: {}", Thread.currentThread().getStackTrace()[2]);
    }
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      LOG.error("Exception in sleep() ", e);
    }
    if (logOutput) {
      LOG.info("Finished.");
    }
  }
}
