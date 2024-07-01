/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.iceberg.integration.test.util;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

import com.datastrato.gravitino.integration.test.util.ITUtils;
import com.datastrato.gravitino.rest.RESTUtils;
import com.datastrato.gravitino.server.GravitinoServer;
import com.datastrato.gravitino.server.IcebergRESTServer;
import com.datastrato.gravitino.server.ServerConfig;
import com.datastrato.gravitino.server.web.JettyServerConfig;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;

public class IcebergRESTServerManagerForEmbedded extends IcebergRESTServerManager {

  private File mockConfDir;
  private final ExecutorService executor;
  private String checkUri;

  public IcebergRESTServerManagerForEmbedded() {
    try {
      this.mockConfDir = Files.createTempDirectory("MiniIcebergRESTServer").toFile();
      LOG.info("config dir:{}", mockConfDir.getAbsolutePath());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    mockConfDir.mkdirs();
    this.executor = Executors.newSingleThreadExecutor();
  }

  @Override
  public void startIcebergRESTServer() throws Exception {
    LOG.info("Staring MiniIcebergRESTServer ...");

    String gravitinoRootDir = System.getenv("GRAVITINO_ROOT_DIR");

    customizeConfigFile(
        Paths.get(gravitinoRootDir, "conf", IcebergRESTServer.CONF_FILE + ".template").toString(),
        Paths.get(mockConfDir.getAbsolutePath(), IcebergRESTServer.CONF_FILE).toString());

    this.serverConfig = new ServerConfig();
    Properties properties =
        serverConfig.loadPropertiesFromFile(
            new File(
                com.datastrato.gravitino.integration.test.util.ITUtils.joinPath(
                    mockConfDir.getAbsolutePath(), IcebergRESTServer.CONF_FILE)));
    serverConfig.loadFromProperties(properties);

    LOG.info("Server config:{}", serverConfig.getAllConfig());

    JettyServerConfig jettyServerConfig =
        JettyServerConfig.fromConfig(serverConfig, GravitinoServer.WEBSERVER_CONF_PREFIX);
    String host = jettyServerConfig.getHost();
    int port = jettyServerConfig.getHttpPort();
    this.checkUri = String.format("http://%s:%d", host, port);
    LOG.info("check uri:{}", checkUri);

    Future<?> future =
        executor.submit(
            () -> {
              try {
                IcebergRESTServer.main(
                    new String[] {
                      Paths.get(mockConfDir.getAbsolutePath(), IcebergRESTServer.CONF_FILE)
                          .toString()
                    });
              } catch (Exception e) {
                LOG.error("Exception in startup MiniGravitino Server ", e);
                throw new RuntimeException(e);
              }
            });
    long beginTime = System.currentTimeMillis();
    boolean started = false;

    String url = checkUri + "/metrics";
    while (System.currentTimeMillis() - beginTime < 1000 * 60 * 3) {
      started = isHttpServerUp(url);
      if (started || future.isDone()) {
        break;
      }
      sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
    }
    if (!started) {
      try {
        future.get(5, TimeUnit.SECONDS);
      } catch (Exception e) {
        throw new RuntimeException("Gravitino server start failed", e);
      }
      throw new RuntimeException("Can not start Gravitino server");
    }

    LOG.info("MiniGravitino stared.");
  }

  @Override
  public void stopIcebergRESTServer() {
    LOG.debug("MiniGravitino shutDown...");

    executor.shutdown();
    sleepUninterruptibly(500, TimeUnit.MILLISECONDS);

    long beginTime = System.currentTimeMillis();
    boolean started = true;

    while (System.currentTimeMillis() - beginTime < 1000 * 60 * 3) {
      sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
      started = isHttpServerUp(checkUri);
      if (!started) {
        break;
      }
    }

    try {
      FileUtils.deleteDirectory(mockConfDir);
    } catch (Exception e) {
      // Ignore
    }

    if (started) {
      throw new RuntimeException("Can not stop Gravitino server");
    }

    LOG.debug("MiniGravitino terminated.");
  }

  private void customizeConfigFile(String configTempFileName, String configFileName)
      throws IOException {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(
        GravitinoServer.WEBSERVER_CONF_PREFIX + JettyServerConfig.WEBSERVER_HTTP_PORT.getKey(),
        String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));

    configMap.putAll(customConfigs);

    ITUtils.rewriteConfigFile(configTempFileName, configFileName, configMap);
  }
}
