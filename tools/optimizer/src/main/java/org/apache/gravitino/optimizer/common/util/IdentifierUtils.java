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

package org.apache.gravitino.optimizer.common.util;

import com.google.common.base.Preconditions;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

public class IdentifierUtils {

  /**
   * Convert the name identifier to the string representation.
   *
   * @param nameIdentifier The name identifier
   * @return The string representation of the name identifier
   */
  public static TableIdentifier toIcebergTableIdentifier(NameIdentifier nameIdentifier) {
    return TableIdentifier.parse(nameIdentifier.toString());
  }

  public static String getCatalogNameFromTableIdentifier(
      NameIdentifier tableIdentifier, String defaultCatalogName) {
    Namespace namespace = tableIdentifier.namespace();
    Preconditions.checkArgument(namespace != null && namespace.levels().length >= 1);
    if (namespace.levels().length == 1) {
      return defaultCatalogName;
    }

    return namespace.levels()[0];
  }
}
