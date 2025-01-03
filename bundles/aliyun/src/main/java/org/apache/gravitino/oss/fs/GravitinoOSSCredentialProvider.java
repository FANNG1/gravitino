/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.oss.fs;

import static org.apache.gravitino.credential.OSSTokenCredential.GRAVITINO_OSS_SESSION_ACCESS_KEY_ID;
import static org.apache.gravitino.credential.OSSTokenCredential.GRAVITINO_OSS_SESSION_SECRET_ACCESS_KEY;
import static org.apache.gravitino.credential.OSSTokenCredential.GRAVITINO_OSS_TOKEN;

import com.aliyun.oss.common.auth.BasicCredentials;
import com.aliyun.oss.common.auth.Credentials;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.auth.DefaultCredentials;
import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.OSSSecretKeyCredential;
import org.apache.gravitino.credential.OSSTokenCredential;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem;
import org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.aliyun.oss.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitinoOSSCredentialProvider implements CredentialsProvider {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoOSSCredentialProvider.class);
  private Credentials basicCredentials;
  private final String filesetIdentifier;
  private long expirationTime;
  private final GravitinoClient client;
  private final Configuration configuration;

  public GravitinoOSSCredentialProvider(URI uri, Configuration conf) {
    this.filesetIdentifier =
        conf.get(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_IDENTIFIER);
    GravitinoVirtualFileSystem gravitinoVirtualFileSystem = new GravitinoVirtualFileSystem();
    this.client = gravitinoVirtualFileSystem.initializeClient(conf);
    this.configuration = conf;
  }

  @Override
  public void setCredentials(Credentials credentials) {}

  @Override
  public Credentials getCredentials() {
    // If the credentials are null or about to expire, refresh the credentials.
    if (basicCredentials == null || System.currentTimeMillis() > expirationTime - 5 * 60 * 1000) {
      synchronized (this) {
        refresh();
      }
    }

    return basicCredentials;
  }

  private void refresh() {
    String[] idents = filesetIdentifier.split("\\.");
    String catalog = idents[1];

    FilesetCatalog filesetCatalog = client.loadCatalog(catalog).asFilesetCatalog();

    Fileset fileset = filesetCatalog.loadFileset(NameIdentifier.of(idents[2], idents[3]));
    Credential[] credentials = fileset.supportsCredentials().getCredentials();
    Optional<Credential> optionalCredential = getCredential(credentials);

    if (!optionalCredential.isPresent()) {
      LOG.warn("No credential found for fileset: {}, try to use static AKSK", filesetIdentifier);
      expirationTime = Long.MAX_VALUE;
      this.basicCredentials =
          new DefaultCredentials(
              configuration.get(Constants.ACCESS_KEY_ID),
              configuration.get(Constants.ACCESS_KEY_SECRET));
      return;
    }

    Credential credential = optionalCredential.get();
    Map<String, String> credentialMap = credential.toProperties();

    String accessKeyId = credentialMap.get(GRAVITINO_OSS_SESSION_ACCESS_KEY_ID);
    String secretAccessKey = credentialMap.get(GRAVITINO_OSS_SESSION_SECRET_ACCESS_KEY);

    if (OSSTokenCredential.OSS_TOKEN_CREDENTIAL_TYPE.equals(
        credentialMap.get(Credential.CREDENTIAL_TYPE))) {
      String sessionToken = credentialMap.get(GRAVITINO_OSS_TOKEN);
      basicCredentials = new BasicCredentials(accessKeyId, secretAccessKey, sessionToken);
    } else {
      basicCredentials = new DefaultCredentials(accessKeyId, secretAccessKey);
    }

    expirationTime = credential.expireTimeInMs();
    if (expirationTime <= 0) {
      expirationTime = Long.MAX_VALUE;
    }
  }

  /**
   * Get the credential from the credential array. Using dynamic credential first, if not found,
   * uses static credential.
   *
   * @param credentials The credential array.
   * @return An optional credential.
   */
  private Optional<Credential> getCredential(Credential[] credentials) {
    // Use dynamic credential if found.
    Optional<Credential> dynamicCredential =
        Arrays.stream(credentials)
            .filter(
                credential ->
                    credential
                        .credentialType()
                        .equals(OSSTokenCredential.OSS_TOKEN_CREDENTIAL_TYPE))
            .findFirst();
    if (dynamicCredential.isPresent()) {
      return dynamicCredential;
    }

    // If dynamic credential not found, use the static one if possible
    return Arrays.stream(credentials)
        .filter(
            credential ->
                credential
                    .credentialType()
                    .equals(OSSSecretKeyCredential.OSS_SECRET_KEY_CREDENTIAL_TYPE))
        .findFirst();
  }
}
