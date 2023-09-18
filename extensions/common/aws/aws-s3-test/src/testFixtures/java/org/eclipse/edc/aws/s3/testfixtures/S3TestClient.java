/*
 *  Copyright (c) 2023 ZF Friedrichshafen AG
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

package org.eclipse.edc.aws.s3.testfixtures;

import okhttp3.Request;
import org.eclipse.edc.aws.s3.AwsClientProvider;
import org.eclipse.edc.aws.s3.AwsClientProviderConfiguration;
import org.eclipse.edc.aws.s3.AwsClientProviderImpl;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.internal.async.ByteArrayAsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.edc.junit.testfixtures.TestUtils.testHttpClient;
import static org.eclipse.edc.util.configuration.ConfigurationFunctions.propOrEnv;
import static org.junit.jupiter.api.Assertions.fail;

public class S3TestClient {

    private final String url;

    private final URI s3Endpoint;

    private final S3AsyncClient s3AsyncClient;

    private final AwsClientProvider clientProvider;

    private S3TestClient(String url, String region) {
        this.url = url;
        this.s3Endpoint = URI.create(propOrEnv("it.aws.endpoint", url));
        AwsClientProviderConfiguration configuration = AwsClientProviderConfiguration.Builder.newInstance()
                .credentialsProvider(this::getCredentials)
                .endpointOverride(this.s3Endpoint)
                .build();
        this.clientProvider = new AwsClientProviderImpl(configuration);
        this.s3AsyncClient = clientProvider.s3AsyncClient(region);
    }

    public static S3TestClient create(String url, String region) {
        return new S3TestClient(url, region);
    }

    public @NotNull AwsCredentials getCredentials() {
        var profile = propOrEnv("it.aws.profile", null);
        if (profile != null) {
            return ProfileCredentialsProvider.create(profile).resolveCredentials();
        }

        var accessKeyId = propOrEnv("S3_ACCESS_KEY_ID", "root");
        Objects.requireNonNull(accessKeyId, "S3_ACCESS_KEY_ID cannot be null!");
        var secretKey = propOrEnv("S3_SECRET_ACCESS_KEY", "password");
        Objects.requireNonNull(secretKey, "S3_SECRET_ACCESS_KEY cannot be null");

        return AwsBasicCredentials.create(accessKeyId, secretKey);
    }

    /**
     * pings <a href="https://docs.min.io/minio/baremetal/monitoring/healthcheck-probe.html">MinIO's health endpoint</a>
     *
     * @return true if HTTP status [200..300[
     */
    protected boolean isAvailable()  throws IOException {
        var httpClient = testHttpClient();
        var healthRq = new Request.Builder().url(s3Endpoint + "/minio/health/live").get().build();
        try (var response = httpClient.execute(healthRq)) {
            return response.isSuccessful();
        }
    }

    public void createBucket(String bucketName) {
        if (bucketExists(bucketName)) {
            fail("Bucket " + bucketName + " exists. Choose a different bucket name to continue test");
        }

        s3AsyncClient.createBucket(CreateBucketRequest.builder().bucket(bucketName).build()).join();

        if (!bucketExists(bucketName)) {
            fail("Setup incomplete, tests will fail");
        }
    }

    private boolean bucketExists(String bucketName) {
        try {
            HeadBucketRequest request = HeadBucketRequest.builder().bucket(bucketName).build();
            return s3AsyncClient.headBucket(request).join()
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

    public void deleteBucket(String bucketName) {
        try {
            if (s3AsyncClient == null) {
                return;
            }

            // Empty the bucket before deleting it, otherwise the AWS S3 API fails
            deleteBucketObjects(bucketName);

            s3AsyncClient.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build()).join();
        } catch (Exception e) {
            System.err.println("Unable to delete bucket " + bucketName + e);
        }

        if (bucketExists(bucketName)) {
            fail("Incomplete teardown, subsequent tests might fail");
        }
    }

    private void deleteBucketObjects(String bucketName) {
        var objectListing = s3AsyncClient.listObjects(ListObjectsRequest.builder().bucket(bucketName).build()).join();

        CompletableFuture.allOf(objectListing.contents().stream()
              .map(object -> s3AsyncClient.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(object.key()).build()))
              .toArray(CompletableFuture[]::new)).join();

        for (var objectSummary : objectListing.contents()) {
            s3AsyncClient.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(objectSummary.key()).build()).join();
        }

        if (objectListing.isTruncated()) {
            deleteBucketObjects(bucketName);
        }
    }

    public void putStringOnBucket(String bucketName, String key, String content) {
        var request = PutObjectRequest.builder().bucket(bucketName).key(key).build();
        var response = s3AsyncClient.putObject(request, AsyncRequestBody.fromString(content));
        assertThat(response).succeedsWithin(10, TimeUnit.SECONDS);
    }

    public void putTestFile(String key, File file, String bucketName) {
        s3AsyncClient.putObject(PutObjectRequest.builder().bucket(bucketName).key(key).build(), file.toPath());
    }

    public CompletableFuture<ResponseBytes<GetObjectResponse>> getObject(String bucketName, String key) {
        return s3AsyncClient
             .getObject(GetObjectRequest.builder().bucket(bucketName).key(key).build(), new ByteArrayAsyncResponseTransformer<>());
    }

    protected boolean isMinio() {
        return url.equals(s3Endpoint.toString());
    }

    public AwsClientProvider getClientProvider() {
        return clientProvider;
    }

    public S3AsyncClient getS3AsyncClient() {
        return s3AsyncClient;
    }
}
