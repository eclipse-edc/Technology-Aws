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
 *       ZF Friedrichshafen AG - Initial implementation
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
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static java.lang.Integer.parseInt;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.edc.aws.s3.S3BucketSchema.ACCESS_KEY_ID;
import static org.eclipse.edc.aws.s3.S3BucketSchema.BUCKET_NAME;
import static org.eclipse.edc.aws.s3.S3BucketSchema.KEY_PREFIX;
import static org.eclipse.edc.aws.s3.S3BucketSchema.SECRET_ACCESS_KEY;
import static org.eclipse.edc.connector.dataplane.aws.s3.DataPlaneS3Extension.DEFAULT_CHUNK_SIZE_IN_MB;
import static org.mockito.Mockito.mock;

@TestInstance(Lifecycle.PER_CLASS)
@AwsS3IntegrationTest
public class S3DataPlaneIntegrationTest extends AbstractS3Test {

    private final String sourceBucketName = "source-" + UUID.randomUUID();
    private final String destinationBucketName = "destination-" + UUID.randomUUID();
    private final int defaultChunkSizeInBytes = 1024 * 1024 * parseInt(DEFAULT_CHUNK_SIZE_IN_MB);


    @BeforeEach
    void setup() {
        sourceClient.createBucket(sourceBucketName);
        destinationClient.createBucket(destinationBucketName);
    }

    @AfterEach
    void tearDown() {
        sourceClient.deleteBucket(sourceBucketName);
        destinationClient.deleteBucket(destinationBucketName);
    }

    @ParameterizedTest(name = "With prefix: {4}")
    @ArgumentsSource(S3DataPlaneIntegrationTestArgumentProvider.class)
    void shouldCopyFromSourceToSink(String expectedKeyName, List<String> expectedKeys, List<String> keysToInitializeBucket, String body, String keyPrefix) {
        keysToInitializeBucket.forEach(key -> sourceClient.putStringOnBucket(sourceBucketName, key, body));

        var vault = mock(Vault.class);
        var typeManager = new TypeManager();

        var sinkFactory = new S3DataSinkFactory(destinationClient.getClientProvider(), Executors.newSingleThreadExecutor(), mock(Monitor.class), vault, typeManager, defaultChunkSizeInBytes);
        var sourceFactory = new S3DataSourceFactory(sourceClient.getClientProvider(), vault, typeManager);
        var sourceAddress = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .keyName(expectedKeyName)
                .property(BUCKET_NAME, sourceBucketName)
                .property(S3BucketSchema.REGION, REGION)
                .property(ACCESS_KEY_ID, sourceClient.getCredentials().accessKeyId())
                .property(SECRET_ACCESS_KEY, sourceClient.getCredentials().secretAccessKey())
                .property(KEY_PREFIX, keyPrefix)
                .build();

        var destinationAddress = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .keyName(expectedKeyName)
                .property(BUCKET_NAME, destinationBucketName)
                .property(S3BucketSchema.REGION, REGION)
                .property(ACCESS_KEY_ID, destinationClient.getCredentials().accessKeyId())
                .property(SECRET_ACCESS_KEY, destinationClient.getCredentials().secretAccessKey())
                .property(KEY_PREFIX, keyPrefix)
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
        expectedKeys.forEach(key -> {
                    assertThat(destinationClient.getObject(destinationBucketName, key)).succeedsWithin(5, SECONDS)
                            .extracting(ResponseBytes::response)
                            .extracting(GetObjectResponse::contentLength)
                            .extracting(Long::intValue)
                            .isEqualTo(body.length());
                    assertThat(destinationClient.getObject(destinationBucketName, expectedKeyName + ".complete")).succeedsWithin(5, SECONDS);
                }
        );
    }

    public static class S3DataPlaneIntegrationTestArgumentProvider implements ArgumentsProvider {
        private final String keyPrefix = UUID.randomUUID().toString();

        private final String keyPrefixDelimiter = "/";

        private final String body = UUID.randomUUID().toString();

        private final List<String> keysForSingleFileTransfer = List.of(
                generateRandomKey()
        );

        private final List<String> keysForMultipleFileTransfer = List.of(
                generateRandomKeyWithPrefix(),
                generateRandomKeyWithPrefix(),
                generateRandomKeyWithPrefix()
        );

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            return Stream.of(
                    Arguments.of(
                            keysForSingleFileTransfer.get(0),
                            keysForSingleFileTransfer,
                            keysForSingleFileTransfer,
                            body,
                            null
                    ),
                    Arguments.of(
                            keyPrefix,
                            keysForMultipleFileTransfer.stream().map(key -> keyPrefix + keyPrefixDelimiter + key).toList(),
                            keysForMultipleFileTransfer,
                            body,
                            keyPrefix
                    )
            );
        }

        private String generateRandomKey() {
            return UUID.randomUUID().toString();
        }

        private String generateRandomKeyWithPrefix() {
            return keyPrefix + keyPrefixDelimiter + generateRandomKey();
        }
    }
}
