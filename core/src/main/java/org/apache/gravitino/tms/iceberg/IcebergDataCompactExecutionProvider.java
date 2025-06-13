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

import org.apache.gravitino.tms.execution.JobExecuteProvider;
import org.apache.gravitino.tms.execution.JobSubmitRequest;

public class IcebergDataCompactExecutionProvider implements JobExecuteProvider{

  @Override
  public String policyType() {
    return "iceberg-data-compact";
  }

  @Override
  public String engineType() {
    return "spark";
  }

  @Override
  public String getCommand(JobSubmitRequest job) {
    IcebergDataCompactPolicy policy = (IcebergDataCompactPolicy) job.metadataPolicyObject().getPolicy();
    String catalogName = job.metadataPolicyObject().getMetadataObject().getCatalogName();
    String tableName = job.metadataPolicyObject().getMetadataObject().getIdentifier();
    // construct catalog_name.system.rewrite_data_files(table => 'db.sample', where => 'id = 3 and name = "foo"', options => map('min-input-files', '2');
    String content = writeExecuteFileContent(catalogName, tableName, policy.targetFileSizeBytes(), policy.minFiles(), policy.where());
    String executeFile = writeExecuteFile(content);
    String sparkCommand = SparkUtils.getSparkCommand();
    String jars = SparkUtils.getSparkIcebergJars();
    // catalog configuration like catalog type, catalog uri
    String catalogConfigs = SparkUtils.getCatalogConfigs();

    String command = String.format("%s --jars %s %s --files %s", sparkCommand, jars, catalogConfigs, executeFile);
    return command;
  }

  @Override
  public Map<String, String> getJobResult(String jobResult) {
    return parseJobResult(jobResult);
  }
}
