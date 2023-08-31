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

import org.eclipse.edc.aws.s3.S3BucketSchema;
import org.eclipse.edc.aws.s3.testfixtures.AbstractS3Test;
import org.eclipse.edc.aws.s3.testfixtures.annotations.AwsS3IntegrationTest;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.internal.async.ByteArrayAsyncResponseTransformer;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.edc.aws.s3.S3BucketSchema.ACCESS_KEY_ID;
import static org.eclipse.edc.aws.s3.S3BucketSchema.BUCKET_NAME;
import static org.eclipse.edc.aws.s3.S3BucketSchema.ENDPOINT_OVERRIDE;
import static org.eclipse.edc.aws.s3.S3BucketSchema.SECRET_ACCESS_KEY;
import static org.mockito.Mockito.mock;

@AwsS3IntegrationTest
public class S3DataPlaneIntegrationTest extends AbstractS3Test {

    private final String sourceBucketName = "source-" + UUID.randomUUID();
    private final String destinationBucketName = "destination-" + UUID.randomUUID();

    @BeforeEach
    void setup() {
        createBucket(sourceBucketName, MinioInstance.SOURCE);
        createBucket(destinationBucketName, MinioInstance.DESTINATION);
    }

    @AfterEach
    void tearDown() {
        deleteBucket(sourceBucketName, MinioInstance.SOURCE);
        deleteBucket(destinationBucketName, MinioInstance.DESTINATION);
    }

    @Test
    void shouldCopyFromSourceToSink() {
        var body = UUID.randomUUID().toString();
        var key = UUID.randomUUID().toString();
        putStringOnBucket(sourceBucketName, key, body, MinioInstance.SOURCE);

        var vault = mock(Vault.class);
        var typeManager = new TypeManager();

        var sinkFactory = new S3DataSinkFactory(destinationClientProvider, Executors.newSingleThreadExecutor(), mock(Monitor.class), vault, typeManager);
        var sourceFactory = new S3DataSourceFactory(sourceClientProvider, vault, typeManager);
        var sourceAddress = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .keyName(key)
                .property(BUCKET_NAME, sourceBucketName)
                .property(S3BucketSchema.REGION, REGION)
                .property(ACCESS_KEY_ID, getSourceCredentials().accessKeyId())
                .property(SECRET_ACCESS_KEY, getSourceCredentials().secretAccessKey())
                .build();

        var destinationAddress = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .keyName(key)
                .property(BUCKET_NAME, destinationBucketName)
                .property(S3BucketSchema.REGION, REGION)
                .property(ACCESS_KEY_ID, getDestinationCredentials().accessKeyId())
                .property(SECRET_ACCESS_KEY, getDestinationCredentials().secretAccessKey())
                .property(ENDPOINT_OVERRIDE, DESTINATION_MINIO_ENDPOINT)
                .build();

        var request = DataFlowRequest.Builder.newInstance()
                .id(UUID.randomUUID().toString())
                .processId(UUID.randomUUID().toString())
                .sourceDataAddress(sourceAddress)
                .destinationDataAddress(destinationAddress)
                .build();

        var sink = sinkFactory.createSink(request);
        var source = sourceFactory.createSource(request);

        var transferResult = sink.transfer(source);

        assertThat(transferResult).succeedsWithin(5, SECONDS);
        assertThat(getObject(key)).succeedsWithin(5, SECONDS)
            .extracting(ResponseBytes::response)
            .extracting(GetObjectResponse::contentLength)
            .extracting(Long::intValue)
            .isEqualTo(body.length());
        assertThat(getObject(key + ".complete")).succeedsWithin(5, SECONDS);
    }

    private CompletableFuture<ResponseBytes<GetObjectResponse>> getObject(String key) {
        var getObjectRequest = GetObjectRequest.builder().bucket(destinationBucketName).key(key).build();
        return getMinioClient(MinioInstance.DESTINATION).getObject(getObjectRequest, new ByteArrayAsyncResponseTransformer<>());
    }

}
