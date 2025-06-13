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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.gravitino.Audit;
import org.apache.gravitino.MetadataObject.Type;
import org.apache.gravitino.exceptions.IllegalPolicyException;
import org.apache.gravitino.policy.Policy;

public class IcebergDataCompactPolicy implements Policy {

  @Override
  public String name() {
    return "iceberg-data-compact-policy1";
  }

  @Override
  public String type() {
    return "iceberg-data-compact";
  }

  @Override
  public boolean enabled() {
    return true;
  }

  @Override
  public boolean exclusive() {
    return true;
  }

  @Override
  public boolean inheritable() {
    return true;
  }

  @Override
  public Set<Type> supportedObjectTypes() {
    return ImmutableSet.of(Type.CATALOG, Type.SCHEMA, Type.TABLE);
  }

  @Override
  public Content content() {
    return new Content() {
      @Override
      public Map<String, String> properties() {
        return ImmutableMap.of("target-file-size-bytes", "5000000", "min-files","5");
      }
    };
  }

  public Optional<String> where() {
    return Optional.ofNullable(content().properties().get("target-file-size-bytes"));
  }

  public int targetFileSizeBytes() {
    return Integer.parseInt(content().properties().get("target-file-size-bytes"));
  }

  public Optional<Integer> minFiles() {
    return Optional.of(Integer.parseInt(content().properties().get("min-files")));
  }

  @Override
  public Optional<Boolean> inherited() {
    return Optional.empty();
  }

  @Override
  public void validate() throws IllegalPolicyException {

  }

  @Override
  public Audit auditInfo() {
    return null;
  }

  @Override
  public String comment() {
    return null;
  }
}
