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

import org.eclipse.edc.aws.s3.spi.S3BucketSchema;
import org.eclipse.edc.aws.s3.testfixtures.S3TestClient;
import org.eclipse.edc.json.JacksonTypeManager;
import org.eclipse.edc.junit.annotations.EndToEndTest;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.eclipse.edc.validator.spi.DataAddressValidatorRegistry;
import org.eclipse.edc.validator.spi.ValidationResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.edc.util.configuration.ConfigurationFunctions.propOrEnv;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Testcontainers
@EndToEndTest
public class S3DataPlaneIntegrationTest {

    // Adding REGION to bucket prevents errors of
    //      "A conflicting conditional operation is currently in progress against this resource."
    // when bucket is rapidly added/deleted and consistency propagation causes this error.
    // (Should not be necessary if REGION remains static, but added to prevent future frustration.)
    // [see http://stackoverflow.com/questions/13898057/aws-error-message-a-conflicting-conditional-operation-is-currently-in-progress]
    private static final String REGION = propOrEnv("it.aws.region", Region.US_EAST_1.id());
    private static final String OBJECT_PREFIX = "object-prefix/";
    private static final String KEY_NAME = "key-name";

    @Container
    private final MinioContainer sourceMinio = new MinioContainer();
    @Container
    private final MinioContainer destinationMinio = new MinioContainer();

    private final String sourceBucketName = "source-" + UUID.randomUUID();
    private final String destinationBucketName = "destination-" + UUID.randomUUID();
    private S3TestClient sourceClient;
    private S3TestClient destinationClient;
    private S3DataSinkFactory sinkFactory;
    private S3DataSourceFactory sourceFactory;

    @BeforeEach
    void setup() {
        DataAddressValidatorRegistry validator = mock();
        when(validator.validateSource(any())).thenReturn(ValidationResult.success());
        when(validator.validateDestination(any())).thenReturn(ValidationResult.success());

        sourceClient = S3TestClient.create("http://localhost:" + sourceMinio.getFirstMappedPort(), REGION);
        destinationClient = S3TestClient.create("http://localhost:" + destinationMinio.getFirstMappedPort(), REGION);

        var typeManager = new JacksonTypeManager();
        var chunkSizeInBytes = 1024 * 1024 * 20;
        sourceFactory = new S3DataSourceFactory(sourceClient.getClientProvider(), mock(), mock(), typeManager, validator);
        sinkFactory = new S3DataSinkFactory(destinationClient.getClientProvider(), Executors.newSingleThreadExecutor(), mock(), mock(), typeManager, chunkSizeInBytes, validator);

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
    void should_copy_using_destination_object_name_case_single_transfer(List<String> objectNames) {

        var isSingleObject = objectNames.size() == 1;
        var objectNameInDestination = "object-name-in-destination";
        var objectContent = UUID.randomUUID().toString();

        //Put folder 0 byte size file marker. AWS does this when a folder is created via the console.
        if(!isSingleObject)
            sourceClient.putStringOnBucket(sourceBucketName, OBJECT_PREFIX, "");

        for (var objectName : objectNames) {
            sourceClient.putStringOnBucket(sourceBucketName, objectName, objectContent);
        }

        var sourceAddress = createDataAddress(objectNames, isSingleObject);

        var destinationAddress = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .keyName(KEY_NAME)
                .property(S3BucketSchema.BUCKET_NAME, destinationBucketName)
                .property(S3BucketSchema.REGION, REGION)
                .property(S3BucketSchema.OBJECT_NAME, objectNameInDestination)
                .property(S3BucketSchema.ACCESS_KEY_ID, destinationClient.getCredentials().accessKeyId())
                .property(S3BucketSchema.SECRET_ACCESS_KEY, destinationClient.getCredentials().secretAccessKey())
                .property(S3BucketSchema.ENDPOINT_OVERRIDE, "http://localhost:" + destinationMinio.getFirstMappedPort())
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

            assertThat(destinationClient.getObject(destinationBucketName,
                        OBJECT_PREFIX)).failsWithin(5, SECONDS)
                        .withThrowableOfType(ExecutionException.class)
                        .withCauseInstanceOf(NoSuchKeyException.class);
        }
    }

    @ParameterizedTest
    @ArgumentsSource(ObjectNamesToTransferProvider.class)
    void should_copy_to_folder_case_property_is_present(List<String> objectNames) {

        var isSingleObject = objectNames.size() == 1;
        var objectNameInDestination = "object-name-in-destination";
        var folderNameInDestination = "folder-name-in-destination/";
        var objectBody = UUID.randomUUID().toString();

        //Put folder 0 byte size file marker. AWS does this when a folder is created via the console.
        if(!isSingleObject)
            sourceClient.putStringOnBucket(sourceBucketName, OBJECT_PREFIX, "");

        for (var objectToTransfer : objectNames) {
            sourceClient.putStringOnBucket(sourceBucketName, objectToTransfer, objectBody);
        }

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
                .property(S3BucketSchema.ENDPOINT_OVERRIDE, "http://localhost:" + destinationMinio.getFirstMappedPort())
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
            assertThat(destinationClient.getObject(destinationBucketName, folderNameInDestination +
                        OBJECT_PREFIX)).failsWithin(5, SECONDS)
                        .withThrowableOfType(ExecutionException.class)
                        .withCauseInstanceOf(NoSuchKeyException.class);
        }
        

    }

    private DataAddress createDataAddress(List<String> assetNames, boolean isSingleObject) {
        var dataAddressBuilder = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .keyName(KEY_NAME)
                .property(S3BucketSchema.BUCKET_NAME, sourceBucketName)
                .property(S3BucketSchema.REGION, REGION)
                .property(S3BucketSchema.ACCESS_KEY_ID, sourceClient.getCredentials().accessKeyId())
                .property(S3BucketSchema.SECRET_ACCESS_KEY, sourceClient.getCredentials().secretAccessKey());

        return isSingleObject ? dataAddressBuilder.property(S3BucketSchema.OBJECT_NAME, assetNames.get(0)).build() :
                dataAddressBuilder.property(S3BucketSchema.OBJECT_PREFIX, OBJECT_PREFIX).build();

    }

    private static class ObjectNamesToTransferProvider implements ArgumentsProvider {

        private static final String OBJECT_NAME = "text-document.txt";

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            return Stream.of(
                    Arguments.of(List.of(
                            OBJECT_PREFIX + "1-" + OBJECT_NAME,
                            OBJECT_PREFIX + "2-" + OBJECT_NAME,
                            OBJECT_PREFIX + "3-" + OBJECT_NAME
                    )),
                    Arguments.of(List.of(OBJECT_NAME))
            );
        }
    }
}
