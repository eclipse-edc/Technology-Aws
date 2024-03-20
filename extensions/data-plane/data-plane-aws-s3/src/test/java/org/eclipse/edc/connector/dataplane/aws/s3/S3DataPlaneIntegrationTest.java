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
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
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

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static java.lang.Integer.parseInt;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
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

    @ParameterizedTest
    @ArgumentsSource(ObjectNamesToTransferProvider.class)
    void should_copy_using_destination_object_name_case_single_transfer(String[] objectNames) {

        var isSingleObject = objectNames.length == 1;
        var objectNameInDestination = "object-name-in-destination";
        var objectContent = UUID.randomUUID().toString();

        for (var objectName : objectNames) {
            sourceClient.putStringOnBucket(sourceBucketName, objectName, objectContent);
        }

        var vault = mock(Vault.class);
        var typeManager = new TypeManager();

        var sinkFactory = new S3DataSinkFactory(destinationClient.getClientProvider(), Executors.newSingleThreadExecutor(), mock(Monitor.class), vault, typeManager, defaultChunkSizeInBytes);
        var sourceFactory = new S3DataSourceFactory(sourceClient.getClientProvider(), vault, typeManager);
        var sourceAddress = createDataAddress(objectNames, isSingleObject);

        var destinationAddress = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .keyName(KEY_NAME)
                .property(S3BucketSchema.BUCKET_NAME, destinationBucketName)
                .property(S3BucketSchema.REGION, REGION)
                .property(S3BucketSchema.OBJECT_NAME, objectNameInDestination)
                .property(S3BucketSchema.ACCESS_KEY_ID, destinationClient.getCredentials().accessKeyId())
                .property(S3BucketSchema.SECRET_ACCESS_KEY, destinationClient.getCredentials().secretAccessKey())
                .property(S3BucketSchema.ENDPOINT_OVERRIDE, DESTINATION_MINIO_ENDPOINT)
                .build();

        var request = DataFlowStartMessage.Builder.newInstance()
                .id(UUID.randomUUID().toString())
                .processId(UUID.randomUUID().toString())
                .sourceDataAddress(sourceAddress)
                .destinationDataAddress(destinationAddress)
                .build();

        var sink = sinkFactory.createSink(request);
        var source = sourceFactory.createSource(request);

        var transferResult = sink.transfer(source);

        assertThat(transferResult).succeedsWithin(5, SECONDS);

        if (isSingleObject) {
            assertThat(destinationClient.getObject(destinationBucketName, objectNameInDestination))
                    .succeedsWithin(5, SECONDS)
                    .extracting(ResponseBytes::response)
                    .extracting(GetObjectResponse::contentLength)
                    .extracting(Long::intValue)
                    .isEqualTo(objectContent.length());
        } else {
            for (var objectName : objectNames) {
                assertThat(destinationClient.getObject(destinationBucketName, objectName))
                        .succeedsWithin(5, SECONDS)
                        .extracting(ResponseBytes::response)
                        .extracting(GetObjectResponse::contentLength)
                        .extracting(Long::intValue)
                        .isEqualTo(objectContent.length());
            }
        }
    }

    @ParameterizedTest
    @ArgumentsSource(ObjectNamesToTransferProvider.class)
    void should_copy_to_folder_case_property_is_present(String[] objectNames) {

        var isSingleObject = objectNames.length == 1;
        var objectNameInDestination = "object-name-in-destination";
        var folderNameInDestination = "folder-name-in-destination/";
        var objectBody = UUID.randomUUID().toString();

        for (var objectToTransfer : objectNames) {
            sourceClient.putStringOnBucket(sourceBucketName, objectToTransfer, objectBody);
        }

        var vault = mock(Vault.class);
        var typeManager = new TypeManager();

        var sinkFactory = new S3DataSinkFactory(destinationClient.getClientProvider(), Executors.newSingleThreadExecutor(), mock(Monitor.class), vault, typeManager, defaultChunkSizeInBytes);
        var sourceFactory = new S3DataSourceFactory(sourceClient.getClientProvider(), vault, typeManager);
        var sourceAddress = createDataAddress(objectNames, isSingleObject);

        var destinationAddress = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .keyName(KEY_NAME)
                .property(S3BucketSchema.BUCKET_NAME, destinationBucketName)
                .property(S3BucketSchema.REGION, REGION)
                .property(S3BucketSchema.OBJECT_NAME, objectNameInDestination)
                .property(S3BucketSchema.FOLDER_NAME, folderNameInDestination)
                .property(S3BucketSchema.ACCESS_KEY_ID, destinationClient.getCredentials().accessKeyId())
                .property(S3BucketSchema.SECRET_ACCESS_KEY, destinationClient.getCredentials().secretAccessKey())
                .property(S3BucketSchema.ENDPOINT_OVERRIDE, DESTINATION_MINIO_ENDPOINT)
                .build();

        var request = DataFlowStartMessage.Builder.newInstance()
                .id(UUID.randomUUID().toString())
                .processId(UUID.randomUUID().toString())
                .sourceDataAddress(sourceAddress)
                .destinationDataAddress(destinationAddress)
                .build();

        var sink = sinkFactory.createSink(request);
        var source = sourceFactory.createSource(request);

        var transferResult = sink.transfer(source);

        assertThat(transferResult).succeedsWithin(5, SECONDS);

        if (isSingleObject) {
            assertThat(destinationClient.getObject(destinationBucketName, folderNameInDestination +
                    objectNameInDestination)).succeedsWithin(5, SECONDS)
                    .extracting(ResponseBytes::response)
                    .extracting(GetObjectResponse::contentLength)
                    .extracting(Long::intValue)
                    .isEqualTo(objectBody.length());
        } else {
            for (var objectName : objectNames) {
                assertThat(destinationClient.getObject(destinationBucketName, folderNameInDestination +
                        objectName)).succeedsWithin(5, SECONDS)
                        .extracting(ResponseBytes::response)
                        .extracting(GetObjectResponse::contentLength)
                        .extracting(Long::intValue)
                        .isEqualTo(objectBody.length());
            }
        }
    }

    private DataAddress createDataAddress(String[] assetNames, boolean isSingleObject) {
        var dataAddressBuilder = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .keyName(KEY_NAME)
                .property(S3BucketSchema.BUCKET_NAME, sourceBucketName)
                .property(S3BucketSchema.REGION, REGION)
                .property(S3BucketSchema.ACCESS_KEY_ID, sourceClient.getCredentials().accessKeyId())
                .property(S3BucketSchema.SECRET_ACCESS_KEY, sourceClient.getCredentials().secretAccessKey());

        return isSingleObject ? dataAddressBuilder.property(S3BucketSchema.OBJECT_NAME, assetNames[0]).build() :
                dataAddressBuilder.property(S3BucketSchema.OBJECT_PREFIX, OBJECT_PREFIX).build();

    }

    private static class ObjectNamesToTransferProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            return Stream.of(
                    Arguments.of((Object) new String[]{
                            OBJECT_PREFIX + "1-" + OBJECT_NAME,
                            OBJECT_PREFIX + "2-" + OBJECT_NAME,
                            OBJECT_PREFIX + "3-" + OBJECT_NAME}),
                    Arguments.of((Object) new String[]{
                            OBJECT_NAME}));
        }
    }
}
