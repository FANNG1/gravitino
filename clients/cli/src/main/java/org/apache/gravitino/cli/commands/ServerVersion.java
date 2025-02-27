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

package org.apache.gravitino.cli.commands;

import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.client.GravitinoAdminClient;

/** Displays the Gravitino server version. */
public class ServerVersion extends Command {

  /**
   * Displays the server version.
   *
   * @param context the command context
   */
  public ServerVersion(CommandContext context) {
    super(context);
  }

  /** Displays the server version. */
  @Override
  public void handle() {
    String version = "unknown";
    try {
      GravitinoAdminClient client = buildAdminClient();
      version = client.serverVersion().version();
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    printResults("Apache Gravitino " + version);
  }
}
