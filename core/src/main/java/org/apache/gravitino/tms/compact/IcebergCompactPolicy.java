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

package org.apache.gravitino.tms.compact;

import java.util.Optional;
import java.util.Set;
import org.apache.gravitino.MetadataObject.Type;
import org.apache.gravitino.exceptions.IllegalPolicyException;
import org.apache.gravitino.policy.Policy;

public class IcebergCompactPolicy implements Policy {

  @Override
  public String name() {
    return null;
  }

  @Override
  public String type() {
    return null;
  }

  @Override
  public String comment() {
    return null;
  }

  @Override
  public boolean enabled() {
    return false;
  }

  @Override
  public boolean exclusive() {
    return false;
  }

  @Override
  public boolean inheritable() {
    return false;
  }

  @Override
  public Set<Type> supportedObjectTypes() {
    return null;
  }

  @Override
  public Content content() {
    return null;
  }

  @Override
  public Optional<Boolean> inherited() {
    return Optional.empty();
  }

  @Override
  public void validate() throws IllegalPolicyException {

  }
}
