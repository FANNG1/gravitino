/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the
 * Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License.  You may obtain a copy
 * of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.maintenance.optimizer.common.reader;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.gravitino.maintenance.optimizer.common.util.S3Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3FileReader implements FileContentReader<S3FileReader.S3Source> {

  private static final Logger LOG = LoggerFactory.getLogger(S3FileReader.class);

  private final S3Client s3Client;

  public S3FileReader(String s3Region) {
    this.s3Client = S3Utils.buildS3Client(s3Region);
  }

  @VisibleForTesting
  S3FileReader(S3Client s3Client) {
    this.s3Client = s3Client;
  }

  @Override
  public List<S3Source> listSources(String path) {
    Preconditions.checkArgument(S3Utils.isS3Path(path), "Path must be an S3 path: %s", path);

    if (isS3Directory(path)) {
      List<S3Source> sources = listS3Directory(path);
      if (sources.isEmpty()) {
        throw new IllegalArgumentException("No files found in directory: " + path);
      }
      return sources;
    } else {
      return List.of(new S3Source(path));
    }
  }

  @Override
  public BufferedReader getReader(S3Source source) {
    LOG.info("Reading S3 file: {}", source.s3Path);
    GetObjectRequest getObjectRequest =
        GetObjectRequest.builder().bucket(source.bucket).key(source.key).build();
    ResponseInputStream<GetObjectResponse> s3InputStream = s3Client.getObject(getObjectRequest);
    return new BufferedReader(new InputStreamReader(s3InputStream, StandardCharsets.UTF_8));
  }

  private boolean isS3Directory(String path) {
    return path.endsWith("/");
  }

  private List<S3Source> listS3Directory(String s3Path) {
    LOG.info("Listing S3 directory: {}", s3Path);
    String bucket = S3Utils.parseBucket(s3Path);
    String prefix = S3Utils.parseKey(s3Path);

    List<S3Source> sources = new ArrayList<>();
    ListObjectsV2Request listRequest =
        ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build();

    ListObjectsV2Response listResponse;
    do {
      listResponse = s3Client.listObjectsV2(listRequest);
      List<S3Source> batchSources =
          listResponse.contents().stream()
              .filter(s3Object -> !s3Object.key().endsWith("/"))
              .sorted(Comparator.comparing(S3Object::key))
              .map(s3Object -> new S3Source(bucket, s3Object.key()))
              .collect(Collectors.toList());
      sources.addAll(batchSources);

      listRequest =
          ListObjectsV2Request.builder()
              .bucket(bucket)
              .prefix(prefix)
              .continuationToken(listResponse.nextContinuationToken())
              .build();
    } while (listResponse.isTruncated());

    return sources;
  }

  @Override
  public void close() {
    s3Client.close();
  }

  public static class S3Source implements Source {
    private final String s3Path;
    private final String bucket;
    private final String key;

    public S3Source(String s3Path) {
      this.s3Path = s3Path;
      this.bucket = S3Utils.parseBucket(s3Path);
      this.key = S3Utils.parseKey(s3Path);
    }

    public S3Source(String bucket, String key) {
      this.s3Path = S3Utils.buildS3Path(bucket, key);
      this.bucket = bucket;
      this.key = key;
    }

    @Override
    public String getPath() {
      return s3Path;
    }
  }
}
