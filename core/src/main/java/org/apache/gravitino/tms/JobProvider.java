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

package org.apache.gravitino.tms;

import java.util.Optional;
import org.apache.gravitino.NameIdentifier;

// each policy should implement this interface to generate a job
public interface JobProvider {
  // policy type is the type of policy that this job provider can handle
  String policyType();
  // requireStats and requireMetadata are used to determine if the job provider needs to get the metadata and stats of the object
  boolean requireStats();
  boolean requireMetadata();
  // jobScheduleResourcePlugin is used to generate the job schedule resource plugin
  default JobScheduleResourcePlugin jobScheduleResourcePlugin(MetadataPolicyObject metadataPolicyObject, Optional<Object> metadata, Optional<Statistics> stats) {
    return new PolicyBasedJobPlugin(metadataPolicyObject.policy);
  }

  default JobScheduleResourcePlugin jobExecuteEnginePlugin(MetadataPolicyObject metadataPolicyObject, Optional<Object> metadata, Optional<Statistics> stats) {
    return new PolicyBasedJobPlugin(metadataPolicyObject.policy);
  }
  // generate some specific properties for job executor
  Map<String, String> jobProperties(MetadataPolicyObject metadataPolicyObject, Optional<Object> metadata, Optional<Statistics> stats);
  // check whether to schedule a job to run
  boolean schedulable(NameIdentifier identifier, Optional<Object> metadata, Optional<Statistics> stats);
}
