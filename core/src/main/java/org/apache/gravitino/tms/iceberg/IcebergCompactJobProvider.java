/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.tms.iceberg;

import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.tms.JobProvider;
import org.apache.gravitino.tms.MetadataPolicyObject;

public class IcebergCompactJobProvider implements JobProvider {

  @Override
  public String policyType() {
    return "iceberg-compact";
  }

  @Override
  public boolean requireStats() {
    return true;
  }

  @Override
  public boolean requireMetadata() {
    return false;
  }

  @Override
  public Map<String, String> jobProperties(MetadataPolicyObject metadataPolicyObject,
      Optional<Object> metadata, Optional<Statistics> stats) {
    return null;
  }

  @Override
  public boolean schedulable(NameIdentifier identifier, Optional<Object> metadata,
      Optional<Statistics> stats) {
    if (tooManySmallFiles(stats) || tooManyDeleteFiles(stats)) {
      return true;
    }
    return false;
  }
}
