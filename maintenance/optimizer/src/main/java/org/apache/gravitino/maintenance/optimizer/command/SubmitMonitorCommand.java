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

package org.apache.gravitino.maintenance.optimizer.command;

import com.google.common.base.Preconditions;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/** Submit monitor request to external monitor service. */
public class SubmitMonitorCommand implements OptimizerCommandExecutor {

  @Override
  public void execute(OptimizerCommandContext context) throws Exception {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(context.identifier()), "Missing required option --identifier");
    long actionTimeSeconds =
        OptimizerCommandUtils.parseLongOption(
            "action-time-seconds", context.actionTimeSeconds(), false);
    long rangeSeconds =
        OptimizerCommandUtils.parseLongOption("range-seconds", context.rangeSeconds(), true);
    String monitorServiceUrl =
        MonitorServiceCommandUtils.normalizeBaseUrl(context.monitorServiceUrl());

    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("identifier", context.identifier().trim());
    payload.put("actionTimeSeconds", actionTimeSeconds);
    payload.put("rangeSeconds", rangeSeconds);
    if (StringUtils.isNotBlank(context.partitionPathRaw())) {
      payload.put("partitionPath", context.partitionPathRaw().trim());
    }

    Object response = MonitorServiceCommandUtils.doPost(monitorServiceUrl + "/v1/monitor", payload);
    Preconditions.checkArgument(
        response instanceof Map, "Unexpected monitor submit response: %s", response);
    Object monitorId = ((Map<?, ?>) response).get("monitorId");
    Preconditions.checkArgument(
        monitorId != null, "Missing monitorId in monitor submit response: %s", response);
    context.output().println(monitorId);
  }
}
