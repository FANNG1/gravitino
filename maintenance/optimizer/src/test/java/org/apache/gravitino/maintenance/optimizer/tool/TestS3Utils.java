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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;

public class TestS3Utils {

  @Test
  public void testIsS3Path() {
    assertTrue(S3Utils.isS3Path("s3://bucket/key"));
    assertTrue(S3Utils.isS3Path("s3a://bucket/key"));
    assertTrue(S3Utils.isS3Path("s3n://bucket/key"));
    assertTrue(S3Utils.isS3Path("s3://bucket"));
    assertTrue(S3Utils.isS3Path("s3://bucket/"));
    assertTrue(S3Utils.isS3Path("s3://bucket/path/to/file.txt"));

    assertFalse(S3Utils.isS3Path(null));
    assertFalse(S3Utils.isS3Path(""));
    assertFalse(S3Utils.isS3Path("/local/path"));
    assertFalse(S3Utils.isS3Path("hdfs://namenode/path"));
    assertFalse(S3Utils.isS3Path("http://example.com"));
  }

  @Test
  public void testParseBucket() {
    assertEquals("my-bucket", S3Utils.parseBucket("s3://my-bucket/key"));
    assertEquals("my-bucket", S3Utils.parseBucket("s3a://my-bucket/key"));
    assertEquals("my-bucket", S3Utils.parseBucket("s3n://my-bucket/key"));
    assertEquals("my-bucket", S3Utils.parseBucket("s3://my-bucket"));
    assertEquals("my-bucket", S3Utils.parseBucket("s3://my-bucket/"));

    // edge cases
    assertEquals("bucket-with-dashes", S3Utils.parseBucket("s3://bucket-with-dashes/key.txt"));
    assertEquals("bucket.with.dots", S3Utils.parseBucket("s3://bucket.with.dots/key"));
    assertEquals("123numeric", S3Utils.parseBucket("s3://123numeric/key"));
  }

  @Test
  public void testParseBucketInvalidPath() {
    assertThrows(IllegalArgumentException.class, () -> S3Utils.parseBucket("invalid-path"));
    assertThrows(IllegalArgumentException.class, () -> S3Utils.parseBucket("/local/path"));
    assertThrows(IllegalArgumentException.class, () -> S3Utils.parseBucket("hdfs://namenode/path"));
  }

  @Test
  public void testParseKey() {
    assertEquals("key", S3Utils.parseKey("s3://bucket/key"));
    assertEquals("path/to/file.txt", S3Utils.parseKey("s3://bucket/path/to/file.txt"));
    assertEquals("prefix/", S3Utils.parseKey("s3://bucket/prefix/"));
    assertEquals("", S3Utils.parseKey("s3://bucket"));
    assertEquals("", S3Utils.parseKey("s3://bucket/"));

    assertEquals("data/file.csv", S3Utils.parseKey("s3a://my-bucket/data/file.csv"));
    assertEquals("logs/2024/", S3Utils.parseKey("s3n://log-bucket/logs/2024/"));

    // edge cases
    assertEquals("key/with/many/slashes", S3Utils.parseKey("s3://bucket/key/with/many/slashes"));
    assertEquals("key-with-dashes.txt", S3Utils.parseKey("s3://bucket/key-with-dashes.txt"));
  }

  @Test
  public void testParseKeyInvalidPath() {
    assertThrows(IllegalArgumentException.class, () -> S3Utils.parseKey("invalid-path"));
    assertThrows(IllegalArgumentException.class, () -> S3Utils.parseKey("/local/path"));
  }

  @Test
  public void testBuildS3Path() {
    assertEquals("s3://bucket/key", S3Utils.buildS3Path("bucket", "key"));
    assertEquals(
        "s3://my-bucket/path/to/file.txt", S3Utils.buildS3Path("my-bucket", "path/to/file.txt"));
    assertEquals("s3://bucket/prefix/", S3Utils.buildS3Path("bucket", "prefix/"));
    assertEquals("s3://bucket/", S3Utils.buildS3Path("bucket", ""));
  }

  @Test
  public void testBuildS3Client() {
    S3Client client = S3Utils.buildS3Client("us-east-1");
    assertNotNull(client);
    client.close();
  }

  @Test
  public void testBuildS3ClientWithDifferentRegions() {
    S3Client clientUsEast1 = S3Utils.buildS3Client("us-east-1");
    assertNotNull(clientUsEast1);
    clientUsEast1.close();

    S3Client clientUsWest2 = S3Utils.buildS3Client("us-west-2");
    assertNotNull(clientUsWest2);
    clientUsWest2.close();

    S3Client clientEuWest1 = S3Utils.buildS3Client("eu-west-1");
    assertNotNull(clientEuWest1);
    clientEuWest1.close();
  }

  @Test
  public void testRoundTripParsing() {
    String originalPath = "s3://my-bucket/path/to/file.txt";
    String bucket = S3Utils.parseBucket(originalPath);
    String key = S3Utils.parseKey(originalPath);
    String reconstructedPath = S3Utils.buildS3Path(bucket, key);

    assertEquals("s3://my-bucket/path/to/file.txt", reconstructedPath);
  }
}
