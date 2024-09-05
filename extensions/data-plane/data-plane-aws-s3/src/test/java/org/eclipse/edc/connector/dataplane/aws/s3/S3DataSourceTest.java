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
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class S3DataSourceTest {

    private static final String BUCKET_NAME = "bucketName";
    private static final String OBJECT_NAME = "object-1";
    private static final String OBJECT_PREFIX = "my-prefix/";
    private static final String ERROR_MESSAGE = "Error message";
    private final S3Client s3ClientMock = mock(S3Client.class);

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
                .client(s3ClientMock)
                .build();

        when(s3ClientMock.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(mockResponse);

        var result = s3Datasource.openPartStream();

        assertThat(result.succeeded()).isTrue();
        verify(s3ClientMock, atLeastOnce()).listObjectsV2(any(ListObjectsV2Request.class));
        assertThat(result.getContent()).hasSize(2);
    }

    @Test
    void should_fail_case_no_object_is_found() {

        var mockResponse = ListObjectsV2Response.builder().build();

        var s3Datasource = S3DataSource.Builder.newInstance()
                .bucketName(BUCKET_NAME)
                .objectName(OBJECT_NAME)
                .objectPrefix(OBJECT_PREFIX)
                .client(s3ClientMock)
                .build();

        when(s3ClientMock.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(mockResponse);

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
                .client(s3ClientMock)
                .build();

        var result = s3Datasource.openPartStream();

        assertThat(result.succeeded()).isTrue();
        verify(s3ClientMock, never()).listObjectsV2(any(ListObjectsV2Request.class));
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
                .client(s3ClientMock)
                .build();

        when(s3ClientMock.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(mockResponse);
        when(s3ClientMock.getObject(any(GetObjectRequest.class))).thenThrow(mockThrowable);

        var s3DataSourceException = assertThrows(S3DataSourceException.class, () ->
                s3Datasource.openPartStream().getContent().map(DataSource.Part::openStream).toList());
        assertThat(s3DataSourceException).hasCause(mockThrowable);
        assertThat(s3DataSourceException.getMessage()).isEqualTo(ERROR_MESSAGE);
    }

}
