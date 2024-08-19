/*
 *  Copyright (c) 2022 - 2004 ZF Friedrichshafen AG
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

import org.eclipse.edc.aws.s3.spi.S3BucketSchema;
import org.eclipse.edc.connector.dataplane.aws.s3.exceptions.S3DataSourceException;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.InputStreamDataSource;
import org.eclipse.edc.spi.monitor.Monitor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class S3DataSinkTest {

    private static final String BUCKET_NAME = "bucketName";
    private static final String SOURCE_OBJECT_NAME = "sourceObjectName";
    private static final String DESTINATION_OBJECT_NAME = "destinationObjectName";
    private static final String ETAG = "eTag";
    private static final String ERROR_MESSAGE = "Error message";
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
                .objectName(DESTINATION_OBJECT_NAME)
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
    @ArgumentsSource(PartsInputs.class)
    void transferParts_succeeds(List<DataSource.Part> inputStream, int expectedPartsPerObject) {
        var isSingleObject = inputStream.size() == 1;

        var result = dataSink.transferParts(inputStream);
        assertThat(result.succeeded()).isTrue();
        verify(s3ClientMock, times(inputStream.size())).completeMultipartUpload(completeMultipartUploadRequestCaptor
                .capture());

        var completeMultipartUploadRequests = completeMultipartUploadRequestCaptor.getAllValues();

		for(var request: completeMultipartUploadRequests){
			assertThat(request.bucket()).isEqualTo(BUCKET_NAME);
			assertThat(request.key())
					.isEqualTo(isSingleObject ? DESTINATION_OBJECT_NAME : SOURCE_OBJECT_NAME);
			assertThat(request.multipartUpload().parts()).hasSize(expectedPartsPerObject);
		}
    }

    @Test
    void transferParts_failed_to_download() {
        var part = mock(DataSource.Part.class);

        when(part.name()).thenReturn(SOURCE_OBJECT_NAME);
        when(part.openStream()).thenThrow(new S3DataSourceException(ERROR_MESSAGE, new RuntimeException()));

        var result = dataSink.transferParts(List.of(part));

        String expectedMessage = "GENERAL_ERROR: Failed to download the %s object: %s".formatted(DESTINATION_OBJECT_NAME, ERROR_MESSAGE);

        assertThat(result.failed()).isTrue();
        assertThat(result.getFailureDetail()).isEqualTo(expectedMessage);
    }

    @ParameterizedTest
    @ArgumentsSource(PartsInputs.class)
    void transferParts_fails_to_upload(List<DataSource.Part> inputStream) {
        var isSingleObject = inputStream.size() == 1;

        var s3DatasinkS3Client = mock(S3Client.class);

        var s3Datasink = S3DataSink.Builder.newInstance()
                .bucketName(BUCKET_NAME)
                .objectName(DESTINATION_OBJECT_NAME)
                .client(s3DatasinkS3Client)
                .requestId(TestFunctions.createRequest(S3BucketSchema.TYPE).build().getId())
                .executorService(Executors.newFixedThreadPool(2))
                .monitor(mock(Monitor.class))
                .chunkSizeBytes(CHUNK_SIZE_BYTES)
                .build();

        when(s3DatasinkS3Client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenThrow(new RuntimeException(ERROR_MESSAGE));

        var result = s3Datasink.transferParts(inputStream);

        var expectedMessage = "GENERAL_ERROR: Failed to upload the %s object: %s"
                .formatted(isSingleObject ? DESTINATION_OBJECT_NAME : SOURCE_OBJECT_NAME, ERROR_MESSAGE);

        assertThat(result.failed()).isTrue();
        assertThat(result.getFailureDetail()).isEqualTo(expectedMessage);
    }

    private static class PartsInputs implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            var emptyContent = "";
            var smallContent = "content smaller than a chunk size";
            var bigContent = "content bigger than 50 bytes chunk size so that it gets chunked and uploaded as a multipart upload";
            return Stream.of(
                    Arguments.of(
                            List.of(createDataSource(emptyContent)), 1),
                    Arguments.of(
                            List.of(createDataSource(smallContent)), 1),
                    Arguments.of(
                            List.of(createDataSource(bigContent)), 2),
                    Arguments.of(
                            List.of(createDataSource(emptyContent), createDataSource(smallContent)), 1),
                    Arguments.of(
                            List.of(createDataSource(bigContent), createDataSource(bigContent)), 2));
        }
    }

    private static InputStreamDataSource createDataSource(String text) {
        if (text.length() > 0){
                return new InputStreamDataSource(SOURCE_OBJECT_NAME, new ByteArrayInputStream(text.getBytes(UTF_8)));
        }
        return new InputStreamDataSource(SOURCE_OBJECT_NAME, new ByteArrayInputStream(new byte[0]));
    }
}
