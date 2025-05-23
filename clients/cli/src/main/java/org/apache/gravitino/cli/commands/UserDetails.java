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

import java.util.List;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchUserException;

public class UserDetails extends Command {

  protected final String metalake;
  protected final String user;

  /**
   * Displays the roles of a user.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param user The name of the user.
   */
  public UserDetails(CommandContext context, String metalake, String user) {
    super(context);
    this.metalake = metalake;
    this.user = user;
  }

  /** Displays the roles of a specified user. */
  @Override
  public void handle() {
    List<String> roles = null;
    User userObject = null;

    try {
      GravitinoClient client = buildClient(metalake);
      userObject = client.getUser(user);
      roles = userObject.roles();
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchUserException err) {
      exitWithError(ErrorMessages.UNKNOWN_USER);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    if (roles == null || roles.isEmpty()) {
      printInformation("The user has no roles.");
    } else {
      printResults(userObject);
    }
  }
}
