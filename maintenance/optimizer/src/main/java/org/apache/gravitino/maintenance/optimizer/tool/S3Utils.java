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
package org.apache.gravitino.maintenance.optimizer.tool;

import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class S3Utils {

  private static final Pattern S3_PATH_PATTERN =
      Pattern.compile("^(s3|s3a|s3n)://([^/]+)(?:/(.*))?$");
  private static final String S3_SCHEME = "s3://";

  private S3Utils() {}

  public static boolean isS3Path(String path) {
    return path != null && S3_PATH_PATTERN.matcher(path).matches();
  }

  public static String parseBucket(String s3Path) {
    Matcher matcher = S3_PATH_PATTERN.matcher(s3Path);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid S3 path: " + s3Path);
    }

    return matcher.group(2);
  }

  public static String parseKey(String s3Path) {
    Matcher matcher = S3_PATH_PATTERN.matcher(s3Path);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid S3 path: " + s3Path);
    }

    String key = matcher.group(3);
    return key == null ? "" : key;
  }

  public static String buildS3Path(String bucket, String key) {
    return S3_SCHEME + bucket + "/" + key;
  }

  public static S3Client buildS3Client(String regionId) {
    Region region = Region.of(regionId);
    return S3Client.builder()
        .region(region)
        .endpointOverride(
            // override S3 endpoint to resolve https://github.com/aws/aws-sdk-java-v2/issues/1786
            URI.create(String.format("https://s3.%s.amazonaws.com", region.id())))
        .build();
  }
}
