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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.gravitino.maintenance.optimizer.common.reader.S3FileReader.S3Source;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

public class TestS3FileReader {

  private S3Client mockS3Client;
  private S3FileReader s3FileReader;

  @BeforeEach
  public void setUp() {
    mockS3Client = mock(S3Client.class);
    s3FileReader = new S3FileReader(mockS3Client);
  }

  @AfterEach
  public void tearDown() {
    if (s3FileReader != null) {
      s3FileReader.close();
    }
  }

  @Test
  public void testListSourcesSingleFile() {
    String s3Path = "s3://my-bucket/file.txt";
    List<S3Source> sources = s3FileReader.listSources(s3Path);

    assertEquals(1, sources.size());
    assertEquals(s3Path, sources.get(0).getPath());
  }

  @Test
  public void testListSourcesSingleFileS3a() {
    String s3Path = "s3a://my-bucket/data/file.csv";
    List<S3Source> sources = s3FileReader.listSources(s3Path);

    assertEquals(1, sources.size());
    assertEquals(s3Path, sources.get(0).getPath());
  }

  @Test
  public void testListSourcesSingleFileS3n() {
    String s3Path = "s3n://my-bucket/logs/log.txt";
    List<S3Source> sources = s3FileReader.listSources(s3Path);

    assertEquals(1, sources.size());
    assertEquals(s3Path, sources.get(0).getPath());
  }

  @Test
  public void testListSourcesDirectory() {
    String s3Path = "s3://my-bucket/prefix/";

    S3Object obj1 = S3Object.builder().key("prefix/file1.txt").build();
    S3Object obj2 = S3Object.builder().key("prefix/file2.txt").build();
    S3Object obj3 = S3Object.builder().key("prefix/subdir/").build();

    ListObjectsV2Response response =
        ListObjectsV2Response.builder()
            .contents(Arrays.asList(obj1, obj2, obj3))
            .isTruncated(false)
            .build();
    when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);

    List<S3Source> sources = s3FileReader.listSources(s3Path);

    assertEquals(2, sources.size());
    assertEquals("s3://my-bucket/prefix/file1.txt", sources.get(0).getPath());
    assertEquals("s3://my-bucket/prefix/file2.txt", sources.get(1).getPath());
    verify(mockS3Client, times(1)).listObjectsV2(any(ListObjectsV2Request.class));
  }

  @Test
  public void testListSourcesDirectoryWithPagination() {
    String s3Path = "s3://my-bucket/data/";

    S3Object obj1 = S3Object.builder().key("data/file1.txt").build();
    S3Object obj2 = S3Object.builder().key("data/file2.txt").build();
    S3Object obj3 = S3Object.builder().key("data/file3.txt").build();

    ListObjectsV2Response response1 =
        ListObjectsV2Response.builder()
            .contents(Arrays.asList(obj1, obj2))
            .isTruncated(true)
            .nextContinuationToken("token123")
            .build();
    ListObjectsV2Response response2 =
        ListObjectsV2Response.builder().contents(Arrays.asList(obj3)).isTruncated(false).build();
    when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(response1, response2);

    List<S3Source> sources = s3FileReader.listSources(s3Path);

    assertEquals(3, sources.size());
    assertEquals("s3://my-bucket/data/file1.txt", sources.get(0).getPath());
    assertEquals("s3://my-bucket/data/file2.txt", sources.get(1).getPath());
    assertEquals("s3://my-bucket/data/file3.txt", sources.get(2).getPath());
    verify(mockS3Client, times(2)).listObjectsV2(any(ListObjectsV2Request.class));
  }

  @Test
  public void testListSourcesEmptyDirectory() {
    String s3Path = "s3://my-bucket/empty/";

    ListObjectsV2Response response =
        ListObjectsV2Response.builder().contents(List.of()).isTruncated(false).build();
    when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> s3FileReader.listSources(s3Path));
    assertEquals("No files found in directory: " + s3Path, exception.getMessage());
  }

  @Test
  public void testListSourcesInvalidPath() {
    String localFilePath = "/local/path";
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> s3FileReader.listSources(localFilePath));
    assertEquals("Path must be an S3 path: " + localFilePath, exception.getMessage());
    String hdfsFilePath = "hdfs://namenode/path";
    IllegalArgumentException exception1 =
        assertThrows(IllegalArgumentException.class, () -> s3FileReader.listSources(hdfsFilePath));
    assertEquals("Path must be an S3 path: " + hdfsFilePath, exception1.getMessage());
  }

  @Test
  public void testClose() {
    s3FileReader.close();
    verify(mockS3Client, times(1)).close();
  }

  @Test
  public void testGetReader() throws IOException {
    String s3Path = "s3://my-bucket/data/file.txt";
    String fileContent = "line1\nline2\nline3";

    ResponseInputStream<GetObjectResponse> mockInputStream =
        createMockResponseInputStream(fileContent);
    when(mockS3Client.getObject(any(GetObjectRequest.class))).thenReturn(mockInputStream);

    List<S3Source> sources = s3FileReader.listSources(s3Path);
    assertEquals(1, sources.size());

    try (BufferedReader reader = s3FileReader.getReader(sources.get(0))) {
      assertNotNull(reader);

      String line1 = reader.readLine();
      String line2 = reader.readLine();
      String line3 = reader.readLine();
      String line4 = reader.readLine();

      assertEquals("line1", line1);
      assertEquals("line2", line2);
      assertEquals("line3", line3);
      assertNull(line4);
    }
    verify(mockS3Client, times(1)).getObject(any(GetObjectRequest.class));
  }

  @Test
  public void testGetReaderEmptyFile() throws IOException {
    String s3Path = "s3://my-bucket/empty.txt";
    String emptyContent = "";

    ResponseInputStream<GetObjectResponse> mockInputStream =
        createMockResponseInputStream(emptyContent);
    when(mockS3Client.getObject(any(GetObjectRequest.class))).thenReturn(mockInputStream);

    List<S3Source> sources = s3FileReader.listSources(s3Path);
    try (BufferedReader reader = s3FileReader.getReader(sources.get(0))) {
      assertNull(reader.readLine());
    }
    verify(mockS3Client, times(1)).getObject(any(GetObjectRequest.class));
  }

  private ResponseInputStream<GetObjectResponse> createMockResponseInputStream(String content) {
    ByteArrayInputStream byteArrayInputStream =
        new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    AbortableInputStream abortableInputStream = AbortableInputStream.create(byteArrayInputStream);
    GetObjectResponse response = GetObjectResponse.builder().build();
    return new ResponseInputStream<>(response, abortableInputStream);
  }
}
