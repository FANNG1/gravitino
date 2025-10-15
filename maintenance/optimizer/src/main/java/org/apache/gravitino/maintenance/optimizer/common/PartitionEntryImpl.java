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

import lombok.ToString;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionEntry;

@ToString
public class PartitionEntryImpl implements PartitionEntry {
  private final String partitionName;
  private final String partitionValue;

  public PartitionEntryImpl(String partitionName, String partitionValue) {
    this.partitionName = partitionName;
    this.partitionValue = partitionValue;
  }

  @Override
  public String partitionName() {
    return partitionName;
  }

  @Override
  public String partitionValue() {
    return partitionValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PartitionEntryImpl)) {
      return false;
    }
    PartitionEntryImpl that = (PartitionEntryImpl) o;
    return java.util.Objects.equals(partitionName, that.partitionName)
        && java.util.Objects.equals(partitionValue, that.partitionValue);
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(partitionName, partitionValue);
  }
}
