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
import org.apache.gravitino.maintenance.optimizer.common.StatisticsInputContent;
import org.apache.gravitino.maintenance.optimizer.tool.TableRegister;

/** Handles CLI command {@code register-tables}. */
public class RegisterTablesCommand implements OptimizerCommandExecutor {

  @Override
  public void execute(OptimizerCommandContext context) {
    StatisticsInputContent inputContent =
        context
            .statisticsInputContent()
            .orElseThrow(
                () -> new IllegalArgumentException("register-tables requires --file-path."));
    Preconditions.checkArgument(
        inputContent.hasFilePath(), "register-tables requires --file-path.");
    new TableRegister(context.optimizerEnv()).registerFromFile(inputContent.filePath());
  }
}
