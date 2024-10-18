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

package org.apache.gravitino.credential;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/** S3 token credential. */
public class S3TokenCredential implements Credential {
  private String accessKeyId;
  private String secretAccessKey;
  private String sessionToken;
  private long expireTimeInMS;

  /**
   * Constructs an instance of {@link S3SecretKeyCredential} with the specified AWS S3 access key ID
   * and secret access key.
   *
   * <p>This constructor validates that both the access key ID and the secret access key are not
   * null. If either of these parameters is null, it will throw a {@link NullPointerException} with
   * an appropriate error message.
   *
   * <p>The provided credentials are used to authenticate requests made to AWS S3 services.
   *
   * @param accessKeyId the AWS S3 access key ID used for authentication. Must not be null.
   * @param secretAccessKey the AWS S3 secret access key used for authentication. Must not be null.
   * @param sessionToken AWS S3 access key ID used for authentication. Must not be null.
   * @param expireTimeInMS AWS S3 access key ID used for authentication. Must not be null.
   * @throws NullPointerException if either {@code accessKeyId} or {@code secretAccessKey} is null.
   * @since [Your Library Version]
   */
  public S3TokenCredential(
      String accessKeyId, String secretAccessKey, String sessionToken, long expireTimeInMS) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(accessKeyId), "S3 access key Id should not be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(secretAccessKey), "S3 secret access key should not be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(sessionToken), "S3 session token should not be empty");

    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
    this.sessionToken = sessionToken;
    this.expireTimeInMS = expireTimeInMS;
  }

  @Override
  public String credentialType() {
    return S3_TOKEN_CREDENTIAL_TYPE;
  }

  @Override
  public long expireTimeInMs() {
    return expireTimeInMS;
  }

  @Override
  public Map<String, String> credentialInfo() {
    return (new ImmutableMap.Builder<String, String>())
        .put(GRAVITINO_S3_ACCESS_KEY_ID, accessKeyId)
        .put(GRAVITINO_S3_SECRET_ACCESS_KEY, secretAccessKey)
        .put(GRAVITINO_S3_TOKEN, sessionToken)
        .build();
  }
}
