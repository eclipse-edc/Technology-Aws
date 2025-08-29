/*
 *  Copyright (c) 2023 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
 *
 */

package org.eclipse.edc.connector.dataplane.aws.s3;

import org.eclipse.edc.connector.dataplane.aws.s3.exceptions.S3DataSourceException;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class S3DataSourceTest {

    private static final String BUCKET_NAME = "bucketName";
    private static final String OBJECT_NAME = "object-1";
    private static final String OBJECT_FOLDER_NAME = "my-folder/";
    private static final String OBJECT_PREFIX = "my-prefix/";
    private static final String ERROR_MESSAGE = "Error message";
    private final S3Client s3Client = mock();

    @Test
    void should_select_prefixed_objects_case_key_prefix_is_present() {
        var mockResponse = ListObjectsV2Response.builder().contents(
                S3Object.builder().key("my-prefix/object-1").build(),
                S3Object.builder().key("my-prefix/object-2").build()
        ).build();

        var s3Datasource = S3DataSource.Builder.newInstance()
                .bucketName(BUCKET_NAME)
                .objectName(OBJECT_NAME)
                .objectPrefix(OBJECT_PREFIX)
                .client(s3Client)
                .build();

        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(mockResponse);

        var result = s3Datasource.openPartStream();

        assertThat(result.succeeded()).isTrue();
        verify(s3Client, atLeastOnce()).listObjectsV2(any(ListObjectsV2Request.class));
        assertThat(result.getContent()).hasSize(2);
    }

    @Test
    void should_fail_case_no_object_is_found() {
        var mockResponse = ListObjectsV2Response.builder().build();

        var s3Datasource = S3DataSource.Builder.newInstance()
                .bucketName(BUCKET_NAME)
                .objectName(OBJECT_NAME)
                .objectPrefix(OBJECT_PREFIX)
                .client(s3Client)
                .build();

        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(mockResponse);

        var result = s3Datasource.openPartStream();

        assertThat(result.failed()).isTrue();
        assertThat(result.getFailure().getFailureDetail()).isEqualTo("GENERAL_ERROR: Error listing S3 objects in the bucket: Object not found");
    }

    @Test
    void should_select_single_object_case_key_prefix_is_not_present() {
        var s3Datasource = S3DataSource.Builder.newInstance()
                .bucketName(BUCKET_NAME)
                .objectName(OBJECT_NAME)
                .objectPrefix(null)
                .client(s3Client)
                .build();

        var result = s3Datasource.openPartStream();

        assertThat(result.succeeded()).isTrue();
        verify(s3Client, never()).listObjectsV2(any(ListObjectsV2Request.class));
        assertThat(result.getContent()).hasSize(1);
    }

    @Test
    void should_throw_datasource_exception_case_object_fetching_fails() {
        var mockResponse = ListObjectsV2Response.builder().contents(
                S3Object.builder().key("my-prefix/object-1").build(),
                S3Object.builder().key("my-prefix/object-2").build()
        ).build();

        var mockThrowable = new RuntimeException(ERROR_MESSAGE);

        var s3Datasource = S3DataSource.Builder.newInstance()
                .bucketName(BUCKET_NAME)
                .objectName(OBJECT_NAME)
                .objectPrefix(OBJECT_PREFIX)
                .client(s3Client)
                .build();

        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(mockResponse);
        when(s3Client.getObject(any(GetObjectRequest.class))).thenThrow(mockThrowable);

        var s3DataSourceException = assertThrows(S3DataSourceException.class, () ->
                s3Datasource.openPartStream().getContent().map(DataSource.Part::openStream).toList());
        assertThat(s3DataSourceException).hasCause(mockThrowable);
        assertThat(s3DataSourceException.getMessage()).isEqualTo(ERROR_MESSAGE);
    }

    @ParameterizedTest
    @ArgumentsSource(S3DataSourceInput.class)
    void shouldSelectFilteredByFolderNameAndOrPrefixS3Objects(String folderName, String prefix, String name, String key, String expectedValue) {
        var mockResponse = ListObjectsV2Response.builder().contents(
                S3Object.builder().key(key).build()
        ).build();

        var s3Datasource = S3DataSource.Builder.newInstance()
                .bucketName(BUCKET_NAME)
                .objectName(name)
                .folderName(folderName)
                .objectPrefix(prefix)
                .client(s3Client)
                .build();

        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(mockResponse);

        var result = s3Datasource.openPartStream();
        assertThat(result.succeeded()).isTrue();
        var list = result.getContent().toList();
        assertThat(list.get(0).name()).isEqualTo(expectedValue);
    }

    @Test
    void shouldFail_when() {
        var s3Part = new S3DataSource.S3Part(s3Client, OBJECT_PREFIX + OBJECT_NAME, BUCKET_NAME, OBJECT_FOLDER_NAME);

        var expectedRequest = HeadObjectRequest.builder()
                .key(OBJECT_FOLDER_NAME + OBJECT_PREFIX + OBJECT_NAME)
                .bucket(BUCKET_NAME)
                .build();

        when(s3Client.headObject(eq(expectedRequest)))
                .thenReturn(HeadObjectResponse.builder().contentLength(2L).build());

        assertDoesNotThrow(s3Part::size);
    }


    @Nested
    class Close {

        @Test
        void shouldNotCloseClient_becauseItCouldBeReused() {
            var s3Datasource = S3DataSource.Builder.newInstance()
                    .bucketName(BUCKET_NAME)
                    .objectName(OBJECT_NAME)
                    .objectPrefix(OBJECT_PREFIX)
                    .client(s3Client)
                    .build();

            s3Datasource.close();

            verify(s3Client, never()).close();
        }

    }

    private static class S3DataSourceInput implements ArgumentsProvider {
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {

            return Stream.of(
                    Arguments.of(OBJECT_FOLDER_NAME, OBJECT_PREFIX, OBJECT_NAME, OBJECT_FOLDER_NAME + OBJECT_PREFIX + OBJECT_NAME, OBJECT_PREFIX + OBJECT_NAME),
                    Arguments.of(OBJECT_FOLDER_NAME.substring(0, OBJECT_FOLDER_NAME.length() - 1), null, OBJECT_NAME, OBJECT_FOLDER_NAME + OBJECT_NAME, OBJECT_NAME),
                    Arguments.of(null, OBJECT_PREFIX, OBJECT_NAME, OBJECT_PREFIX + OBJECT_NAME, OBJECT_PREFIX + OBJECT_NAME),
                    Arguments.of(null, null, OBJECT_NAME, OBJECT_NAME, OBJECT_NAME));
        }
    }
}
