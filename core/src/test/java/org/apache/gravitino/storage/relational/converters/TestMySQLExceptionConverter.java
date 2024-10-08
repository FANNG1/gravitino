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
package org.apache.gravitino.storage.relational.converters;

import java.sql.SQLException;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestMySQLExceptionConverter {

  @Test
  public void testConvertDuplicatedEntryException() {
    SQLException mockException = Mockito.mock(SQLException.class);
    Mockito.when(mockException.getErrorCode()).thenReturn(1062);
    MySQLExceptionConverter converter = new MySQLExceptionConverter();
    Assertions.assertThrows(
        EntityAlreadyExistsException.class,
        () -> converter.toGravitinoException(mockException, Entity.EntityType.METALAKE, "test"),
        String.format("The %s entity: %s already exists.", Entity.EntityType.METALAKE, "test"));
  }
}
