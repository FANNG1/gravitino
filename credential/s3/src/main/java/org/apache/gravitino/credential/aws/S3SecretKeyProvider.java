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

package org.apache.gravitino.credential.aws;

import java.util.Map;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialContext;
import org.apache.gravitino.credential.CredentialProvider;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.config.S3CredentialConfig;

public class S3SecretKeyProvider implements CredentialProvider {

  private String accessKey;
  private String secretKey;

  @Override
  public void initialize(Map<String, String> properties) {
    S3CredentialConfig s3CredentialConfig = new S3CredentialConfig(properties);
    this.accessKey = s3CredentialConfig.accessKeyID();
    this.secretKey = s3CredentialConfig.secretAccessKey();
  }

  @Override
  public void close() {}

  @Override
  public String credentialType() {
    return Credential.S3_SECRET_KEY_CREDENTIAL_TYPE;
  }

  @Override
  public Credential getCredential(CredentialContext context) {
    return new S3SecretKeyCredential(accessKey, secretKey);
  }
}
