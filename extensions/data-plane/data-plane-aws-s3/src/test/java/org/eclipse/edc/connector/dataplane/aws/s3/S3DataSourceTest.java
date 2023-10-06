/*
 *  Copyright (c) 2022 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
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

import org.eclipse.edc.connector.dataplane.aws.s3.exception.S3ObjectNotFoundException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class S3DataSourceTest {

    private static final String BUCKET_NAME = "bucketName";
    private static final String KEY_NAME = "object-1";
    private static final String KEY_PREFIX = "my-prefix/";
    private final S3Client s3ClientMock = mock(S3Client.class);

    @Test
    void should_select_prefixed_objects_case_key_prefix_is_present() {

        var mockResponse = ListObjectsV2Response.builder().contents(
                S3Object.builder().key("my-prefix/object-1").build(),
                S3Object.builder().key("my-prefix/object-2").build()
        ).build();

        var s3Datasource = S3DataSource.Builder.newInstance()
                .bucketName(BUCKET_NAME)
                .keyName(KEY_NAME)
                .keyPrefix(KEY_PREFIX)
                .client(s3ClientMock)
                .build();

        when(s3ClientMock.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(mockResponse);

        var content = s3Datasource.openPartStream().getContent();

        verify(s3ClientMock, atLeastOnce()).listObjectsV2(any(ListObjectsV2Request.class));
        assertThat(content).hasSize(2);
    }

    @Test
    void should_throw_exception_case_no_object_is_found() {

        var mockResponse = ListObjectsV2Response.builder().build();

        var s3Datasource = S3DataSource.Builder.newInstance()
                .bucketName(BUCKET_NAME)
                .keyName(KEY_NAME)
                .keyPrefix(KEY_PREFIX)
                .client(s3ClientMock)
                .build();

        when(s3ClientMock.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(mockResponse);

        assertThatThrownBy(s3Datasource::openPartStream)
                .isInstanceOf(S3ObjectNotFoundException.class)
                .hasMessage("Object not found");
    }

    @Test
    void should_select_single_object_case_key_prefix_is_not_present() {

        var s3Datasource = S3DataSource.Builder.newInstance()
                .bucketName(BUCKET_NAME)
                .keyName(KEY_NAME)
                .keyPrefix(null)
                .client(s3ClientMock)
                .build();

        var content = s3Datasource.openPartStream().getContent();

        verify(s3ClientMock, never()).listObjectsV2(any(ListObjectsV2Request.class));
        assertThat(content).hasSize(1);
    }

}
