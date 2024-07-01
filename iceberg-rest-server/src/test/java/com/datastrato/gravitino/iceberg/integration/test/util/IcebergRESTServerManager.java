/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.iceberg.integration.test.util;

import com.datastrato.gravitino.Config;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.classic.methods.HttpGet;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.impl.classic.HttpClients;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.ClassicHttpResponse;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class IcebergRESTServerManager {
  protected static final Logger LOG = LoggerFactory.getLogger(IcebergRESTServerManager.class);

  protected Map<String, String> customConfigs = new HashMap<>();
  protected Config serverConfig;

  public abstract void startIcebergRESTServer() throws Exception;

  public abstract void stopIcebergRESTServer();

  public static IcebergRESTServerManager create() {
    String testMode =
        System.getProperty(com.datastrato.gravitino.integration.test.util.ITUtils.TEST_MODE);
    if (com.datastrato.gravitino.integration.test.util.ITUtils.EMBEDDED_TEST_MODE.equals(
        testMode)) {
      return new IcebergRESTServerManagerForEmbedded();
    } else {
      return new IcebergRESTServerManagerForDeploy();
    }
  }

  public void registerCustomConfigs(Map<String, String> configs) {
    customConfigs.putAll(configs);
  }

  public Config getServerConfig() {
    return serverConfig;
  }

  public static boolean isHttpServerUp(String testUrl) {
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpGet request = new HttpGet(testUrl);
      ClassicHttpResponse response = httpClient.execute(request, a -> a);
      return response.getCode() == 200;
    } catch (Exception e) {
      LOG.warn("Check Gravitino server failed: ", e);
      return false;
    }
  }
}
