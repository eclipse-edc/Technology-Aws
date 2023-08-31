/*
 *  Copyright (c) 2020 - 2022 Microsoft Corporation
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Microsoft Corporation - initial API and implementation
 *       NTT DATA - added endpoint override
 *
 */

package org.eclipse.edc.aws.s3.testfixtures;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import okhttp3.Request;
import org.eclipse.edc.aws.s3.AwsClientProvider;
import org.eclipse.edc.aws.s3.AwsClientProviderConfiguration;
import org.eclipse.edc.aws.s3.AwsClientProviderImpl;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.eclipse.edc.junit.testfixtures.TestUtils.testHttpClient;
import static org.eclipse.edc.util.configuration.ConfigurationFunctions.propOrEnv;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Base class for tests that need an S3 bucket created and deleted on every test run.
 */
public abstract class AbstractS3Test {

    protected static final String REGION = propOrEnv("it.aws.region", Region.US_EAST_1.id());
    // Adding REGION to bucket prevents errors of
    //      "A conflicting conditional operation is currently in progress against this resource."
    // when bucket is rapidly added/deleted and consistency propagation causes this error.
    // (Should not be necessary if REGION remains static, but added to prevent future frustration.)
    // [see http://stackoverflow.com/questions/13898057/aws-error-message-a-conflicting-conditional-operation-is-currently-in-progress]
    protected static final String SOURCE_MINIO_ENDPOINT = "http://localhost:9000";

    protected static final String DESTINATION_MINIO_ENDPOINT = "http://localhost:9002";
    protected static final URI SOURCE_S3_ENDPOINT = URI.create(propOrEnv("it.aws.endpoint", SOURCE_MINIO_ENDPOINT));

    protected static final URI DESTINATION_S3_ENDPOINT = URI.create(propOrEnv("it.aws.endpoint", DESTINATION_MINIO_ENDPOINT));

    protected S3AsyncClient s3AsyncSourceClient;

    protected S3AsyncClient s3AsyncDestinationClient;

    protected final UUID processId = UUID.randomUUID();

    protected String bucketName = createBucketName();
    private final AwsClientProviderConfiguration sourceConfiguration = AwsClientProviderConfiguration.Builder.newInstance()
        .credentialsProvider(this::getSourceCredentials)
        .endpointOverride(SOURCE_S3_ENDPOINT)
        .build();

    private final AwsClientProviderConfiguration destinationConfiguration = AwsClientProviderConfiguration.Builder.newInstance()
        .credentialsProvider(this::getDestinationCredentials)
        .endpointOverride(DESTINATION_S3_ENDPOINT)
        .build();
    protected AwsClientProvider sourceClientProvider = new AwsClientProviderImpl(sourceConfiguration);

    protected AwsClientProvider destinationClientProvider = new AwsClientProviderImpl(destinationConfiguration);

    @BeforeAll
    static void prepareAll() {
        await().atLeast(Duration.ofSeconds(2))
            .atMost(Duration.ofSeconds(15))
            .with()
            .pollInterval(Duration.ofSeconds(2))
            .ignoreException(IOException.class) // thrown by pingMinio
            .ignoreException(ConnectException.class)
            .until(AbstractS3Test::isBackendAvailable);
    }

    private static boolean isBackendAvailable() throws IOException {
        if (isMinio()) {
            return isMinioAvailable(SOURCE_S3_ENDPOINT) && isMinioAvailable(DESTINATION_S3_ENDPOINT);
        } else {
            return true;
        }
    }

    private static boolean isMinio() {
        return SOURCE_MINIO_ENDPOINT.equals(SOURCE_S3_ENDPOINT.toString()) &&
            DESTINATION_MINIO_ENDPOINT.equals(DESTINATION_S3_ENDPOINT.toString());
    }

    /**
     * pings <a href="https://docs.min.io/minio/baremetal/monitoring/healthcheck-probe.html">MinIO's health endpoint</a>
     *
     * @return true if HTTP status [200..300[
     */
    private static boolean isMinioAvailable(URI minioUri) throws IOException {
        var httpClient = testHttpClient();
        var healthRq = new Request.Builder().url(minioUri + "/minio/health/live").get().build();
        try (var response = httpClient.execute(healthRq)) {
            return response.isSuccessful();
        }
    }

    @BeforeEach
    public void setupClients() {
        s3AsyncSourceClient = sourceClientProvider.s3AsyncClient(REGION);
        s3AsyncDestinationClient = destinationClientProvider.s3AsyncClient(REGION);
        createBucket(bucketName, MinioInstance.SOURCE);
    }

    @AfterEach
    void cleanup() {
        deleteBucket(bucketName, MinioInstance.SOURCE);
    }

    @NotNull
    protected String createBucketName() {
        return "test-bucket-" + processId + "-" + REGION;
    }

    protected void createBucket(String bucketName, MinioInstance minioInstance) {
        if (bucketExists(bucketName, minioInstance)) {
            fail("Bucket " + bucketName + " exists. Choose a different bucket name to continue test");
        }

        getMinioClient(minioInstance).createBucket(CreateBucketRequest.builder().bucket(bucketName).build()).join();

        if (!bucketExists(bucketName, minioInstance)) {
            fail("Setup incomplete, tests will fail");
        }
    }

    protected void deleteBucket(String bucketName, MinioInstance minioInstance) {

        S3AsyncClient s3AsyncClient = getMinioClient(minioInstance);

        try {
            if (s3AsyncClient == null) {
                return;
            }

            // Empty the bucket before deleting it, otherwise the AWS S3 API fails
            deleteBucketObjects(bucketName, minioInstance);

            s3AsyncClient.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build()).join();
        } catch (Exception e) {
            System.err.println("Unable to delete bucket " + bucketName + e);
        }

        if (bucketExists(bucketName, minioInstance)) {
            fail("Incomplete teardown, subsequent tests might fail");
        }
    }

    protected CompletableFuture<PutObjectResponse> putTestFile(String key, File file, String bucketName, MinioInstance minioInstance) {
        return getMinioClient(minioInstance).putObject(PutObjectRequest.builder().bucket(bucketName).key(key).build(), file.toPath());
    }

    protected void putStringOnBucket(String bucketName, String key, String content, MinioInstance minioInstance) {
        var request = PutObjectRequest.builder().bucket(bucketName).key(key).build();
        var response = getMinioClient(minioInstance).putObject(request, AsyncRequestBody.fromString(content));
        assertThat(response).succeedsWithin(10, TimeUnit.SECONDS);
    }

    protected @NotNull AwsCredentials getSourceCredentials() {
        var profile = propOrEnv("it.aws.profile", null);
        if (profile != null) {
            return ProfileCredentialsProvider.create(profile).resolveCredentials();
        }

        var accessKeyId = propOrEnv("S3_ACCESS_KEY_ID_SOURCE", "root");
        Objects.requireNonNull(accessKeyId, "S3_ACCESS_KEY_ID_SOURCE cannot be null!");
        var secretKey = propOrEnv("S3_SECRET_ACCESS_KEY_SOURCE", "password");
        Objects.requireNonNull(secretKey, "S3_SECRET_ACCESS_KEY_SOURCE cannot be null");

        return AwsBasicCredentials.create(accessKeyId, secretKey);
    }

    protected @NotNull AwsCredentials getDestinationCredentials() {
        var profile = propOrEnv("it.aws.profile", null);
        if (profile != null) {
            return ProfileCredentialsProvider.create(profile).resolveCredentials();
        }

        var accessKeyId = propOrEnv("S3_ACCESS_KEY_ID_DESTINATION", "root");
        Objects.requireNonNull(accessKeyId, "S3_ACCESS_KEY_ID_DESTINATION cannot be null!");
        var secretKey = propOrEnv("S3_SECRET_ACCESS_KEY_DESTINATION", "password");
        Objects.requireNonNull(secretKey, "S3_SECRET_ACCESS_KEY_DESTINATION cannot be null");

        return AwsBasicCredentials.create(accessKeyId, secretKey);
    }

    private void deleteBucketObjects(String bucketName, MinioInstance minioInstance) {
        S3AsyncClient s3AsyncClient = getMinioClient(minioInstance);

        var objectListing = s3AsyncClient.listObjects(ListObjectsRequest.builder().bucket(bucketName).build()).join();

        CompletableFuture.allOf(objectListing.contents().stream()
            .map(object -> s3AsyncClient.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(object.key()).build()))
            .toArray(CompletableFuture[]::new)).join();

        for (var objectSummary : objectListing.contents()) {
            s3AsyncClient.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(objectSummary.key()).build()).join();
        }

        if (objectListing.isTruncated()) {
            deleteBucketObjects(bucketName, minioInstance);
        }
    }

    private boolean bucketExists(String bucketName, MinioInstance minioInstance) {
        try {
            HeadBucketRequest request = HeadBucketRequest.builder().bucket(bucketName).build();
            return getMinioClient(minioInstance).headBucket(request).join()
                .sdkHttpResponse()
                .isSuccessful();
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchBucketException) {
                return false;
            } else {
                throw e;
            }
        }
    }
    protected S3AsyncClient getMinioClient(MinioInstance minio) {
        return minio.equals(MinioInstance.SOURCE) ? s3AsyncSourceClient : s3AsyncDestinationClient;
    }
    public enum MinioInstance {
        SOURCE,
        DESTINATION;
    }
}

