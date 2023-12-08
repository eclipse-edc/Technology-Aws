/*
 *  Copyright (c) 2022 ZF Friedrichshafen AG
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       ZF Friedrichshafen AG - Initial implementation
 *
 */

package org.eclipse.edc.connector.dataplane.aws.s3;

import org.eclipse.edc.aws.s3.S3BucketSchema;
import org.eclipse.edc.connector.dataplane.spi.pipeline.InputStreamDataSource;
import org.eclipse.edc.spi.monitor.Monitor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class S3DataSinkTest {

    private static final String BUCKET_NAME = "bucketName";
    private static final String KEY_NAME = "keyName";
    private static final String ETAG = "eTag";
    private static final int CHUNK_SIZE_BYTES = 50;

    private S3Client s3ClientMock;
    private S3DataSink dataSink;

    private ArgumentCaptor<CompleteMultipartUploadRequest> completeMultipartUploadRequestCaptor;

    @BeforeEach
    void setup() {
        s3ClientMock = mock(S3Client.class);
        completeMultipartUploadRequestCaptor = ArgumentCaptor.forClass(CompleteMultipartUploadRequest.class);

        dataSink = S3DataSink.Builder.newInstance()
                .bucketName(BUCKET_NAME)
                .keyName(KEY_NAME)
                .client(s3ClientMock)
                .requestId(TestFunctions.createRequest(S3BucketSchema.TYPE).build().getId())
                .executorService(Executors.newFixedThreadPool(2))
                .monitor(mock(Monitor.class))
                .chunkSizeBytes(CHUNK_SIZE_BYTES)
                .build();

        when(s3ClientMock.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(CreateMultipartUploadResponse.builder().uploadId("uploadId").build());
        when(s3ClientMock.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
                .thenReturn(UploadPartResponse.builder().eTag(ETAG).build());
    }

    @ParameterizedTest
    @ArgumentsSource(SinglePartsInputs.class)
    void transferParts_singlePart_succeeds(List inputStream) {
        var result = dataSink.transferParts(inputStream);
        assertThat(result.succeeded()).isTrue();
        verify(s3ClientMock, times(inputStream.size())).completeMultipartUpload(completeMultipartUploadRequestCaptor.capture());

        var completeMultipartUploadRequest = completeMultipartUploadRequestCaptor.getValue();
        assertThat(completeMultipartUploadRequest.bucket()).isEqualTo(BUCKET_NAME);
        assertThat(completeMultipartUploadRequest.key()).isEqualTo(KEY_NAME);
        assertThat(completeMultipartUploadRequest.multipartUpload().parts()).hasSize(1);
    }

    @ParameterizedTest
    @ArgumentsSource(MultiPartsInputs.class)
    void transferParts_multiPart_succeeds(List inputStream) {
        var result = dataSink.transferParts(inputStream);
        assertThat(result.succeeded()).isTrue();
        verify(s3ClientMock, times(inputStream.size())).completeMultipartUpload(completeMultipartUploadRequestCaptor.capture());

        var completeMultipartUploadRequest = completeMultipartUploadRequestCaptor.getValue();
        assertThat(completeMultipartUploadRequest.bucket()).isEqualTo(BUCKET_NAME);
        assertThat(completeMultipartUploadRequest.key()).isEqualTo(KEY_NAME);

        assertThat(completeMultipartUploadRequest.multipartUpload().parts()).hasSize(2);
    }

    @Test
    void complete_succeedIfPutObjectSucceeds() {
        when(s3ClientMock.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());

        dataSink.registerCompletedFile("any");

        var result = dataSink.complete();

        assertThat(result.succeeded()).isTrue();
    }

    @Test
    void complete_failsIfPutObjectFails() {
        when(s3ClientMock.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenThrow(SdkException.builder().message("an error").build());

        dataSink.registerCompletedFile("any");

        var result = dataSink.complete();

        assertThat(result.failed()).isTrue();
    }

    @ParameterizedTest
    @CsvSource(value = {"object, lob,  null, foo,  foo/lob",
            "prefix/object, null, prefix, foo,  foo/prefix/object",
            "object, ''  , null, foo,  foo/object",
            "object, lob,  null, foo/, foo/lob",
            "object, null, null, foo/, foo/object",
            "prefix/object, ''  , prefix, foo/, foo/prefix/object",
            "object, lob,  null, null, lob",
            "object, lob,  null, '',   lob",
            "prefix/object, '',   prefix, '',   prefix/object"},
            nullValues = {"null"})
    void getDestinationObjectName_shouldBeProperlyConcatenated(String partName, String keyName, String keyPrefix, String folderName, String expected) {

        var dataSink = S3DataSink.Builder.newInstance()
                .bucketName(BUCKET_NAME)
                .keyName(keyName)
                .folderName(folderName)
                .keyPrefix(keyPrefix)
                .client(s3ClientMock)
                .requestId(TestFunctions.createRequest(S3BucketSchema.TYPE).build().getId())
                .executorService(Executors.newFixedThreadPool(2))
                .monitor(mock(Monitor.class))
                .chunkSizeBytes(CHUNK_SIZE_BYTES)
                .build();

        assertEquals(expected, dataSink.getDestinationObjectName(partName));
    }

    @Nested
    public class S3DataSinkBuilderTest {
        private static Stream<Arguments> invalidChunkSizes() {
            return Stream.of(
                    Arguments.of(0),
                    Arguments.of(-1)
            );
        }

        private S3DataSink createInvalidS3DataSink(int chunkSize) {
            return S3DataSink.Builder.newInstance()
                    .requestId(TestFunctions.createRequest(S3BucketSchema.TYPE).build().getId())
                    .executorService(Executors.newFixedThreadPool(2))
                    .chunkSizeBytes(chunkSize)
                    .build();
        }
    }

    private static class SinglePartsInputs implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            return Stream.of(
                    Arguments.of(List.of(new InputStreamDataSource(KEY_NAME, new ByteArrayInputStream("content smaller than a chunk size".getBytes(UTF_8))))),
                    Arguments.of(List.of(new InputStreamDataSource(KEY_NAME, new ByteArrayInputStream("content smaller than a chunk size".getBytes(UTF_8))),
                            new InputStreamDataSource(KEY_NAME, new ByteArrayInputStream("content smaller than a chunk size".getBytes(UTF_8)))))
            );
        }
    }

    private static class MultiPartsInputs implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            return Stream.of(
                    Arguments.of(List.of(new InputStreamDataSource(KEY_NAME, new ByteArrayInputStream("content bigger than 50 bytes chunk size so that it gets chunked and uploaded as a multipart upload".getBytes(UTF_8))))),
                    Arguments.of(List.of(new InputStreamDataSource(KEY_NAME, new ByteArrayInputStream("content bigger than 50 bytes chunk size so that it gets chunked and uploaded as a multipart upload".getBytes(UTF_8))),
                            new InputStreamDataSource(KEY_NAME, new ByteArrayInputStream("content bigger than 50 bytes chunk size so that it gets chunked and uploaded as a multipart upload".getBytes(UTF_8)))))
            );
        }
    }
}
