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

package com.pinterest.job;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDagRun {

  @Test
  void testDagRunWithTwoFieldsExtractsExecutionDate() {
    DagRun dagRun = new DagRun("test_dag", "manual__2025-12-24T21:58:52+00:00");

    Assertions.assertEquals("test_dag", dagRun.getDagId());
    Assertions.assertEquals("2025-12-24T21:58:52+00:00", dagRun.getExecutionDate());
    Assertions.assertEquals("manual__2025-12-24T21:58:52+00:00", dagRun.getRunId());
  }

  @Test
  void testDagRunWithInvalidRunIdFormatThrowsException() {
    String runId = "invalid_run_id_without_delimiter";
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> new DagRun("test_dag", runId));

    String expectedMessage =
        String.format(
            "Invalid runId format: expected exactly 2 parts delimited by '__', but got 1 parts in '%s'",
            runId);
    Assertions.assertEquals(expectedMessage, exception.getMessage());
  }

  @Test
  void testDagRunWithMultipleDelimitersThrowsException() {
    String runId = "manual__2025__extra";
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> new DagRun("test_dag", runId));

    String expectedMessage =
        String.format(
            "Invalid runId format: expected exactly 2 parts delimited by '__', but got 3 parts in '%s'",
            runId);
    Assertions.assertEquals(expectedMessage, exception.getMessage());
  }

  @Test
  void testDagRunWithEmptyRunIdThrowsException() {
    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, () -> new DagRun("test_dag", ""));

    String expectedMessage = "runId cannot be null or empty";
    Assertions.assertEquals(expectedMessage, exception.getMessage());
  }

  @Test
  void testDagRunWithNullRunIdThrowsException() {
    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, () -> new DagRun("test_dag", null));

    String expectedMessage = "runId cannot be null or empty";
    Assertions.assertEquals(expectedMessage, exception.getMessage());
  }
}
