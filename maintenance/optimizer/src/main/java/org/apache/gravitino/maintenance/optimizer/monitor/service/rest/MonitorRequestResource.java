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

package org.apache.gravitino.maintenance.optimizer.monitor.service.rest;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.maintenance.optimizer.monitor.service.MonitorListItem;
import org.apache.gravitino.maintenance.optimizer.monitor.service.MonitorResponse;
import org.apache.gravitino.maintenance.optimizer.monitor.service.MonitorStateManager;
import org.apache.gravitino.maintenance.optimizer.monitor.service.MonitorSubmitRequest;
import org.apache.gravitino.maintenance.optimizer.monitor.service.MonitorSubmitResponse;

@Path("/v1/monitor")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class MonitorRequestResource {

  private final MonitorStateManager handler;

  @Inject
  public MonitorRequestResource(MonitorStateManager handler) {
    this.handler = handler;
  }

  @POST
  public Response submit(MonitorSubmitRequest payload) {
    MonitorSubmitResponse response = handler.submit(payload);
    return Response.status(Response.Status.ACCEPTED).entity(response).build();
  }

  @GET
  public java.util.Collection<MonitorListItem> list() {
    return handler.listMonitors();
  }

  @GET
  @Path("{monitorId}")
  public MonitorResponse status(@PathParam("monitorId") String monitorId) {
    return handler.getStatus(monitorId);
  }

  @POST
  @Path("{monitorId}/cancel")
  public MonitorResponse cancel(@PathParam("monitorId") String monitorId) {
    return handler.cancel(monitorId);
  }
}
