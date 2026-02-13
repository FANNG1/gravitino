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

package org.apache.gravitino.maintenance.optimizer.common;

/**
 * Carries optional runtime input for statistics calculators.
 *
 * <p>Only one of {@code statisticsFilePath} or {@code statisticsPayload} should be provided.
 */
public class StatisticsCalculatorContent implements OptimizerContent {
  private final String statisticsFilePath;
  private final String statisticsPayload;

  /**
   * Creates calculator content.
   *
   * @param statisticsFilePath path to an input statistics file
   * @param statisticsPayload inline statistics payload
   */
  public StatisticsCalculatorContent(String statisticsFilePath, String statisticsPayload) {
    this.statisticsFilePath = statisticsFilePath;
    this.statisticsPayload = statisticsPayload;
  }

  /** Returns the configured statistics file path, or {@code null} when absent. */
  public String statisticsFilePath() {
    return statisticsFilePath;
  }

  /** Returns the configured inline statistics payload, or {@code null} when absent. */
  public String statisticsPayload() {
    return statisticsPayload;
  }
}
