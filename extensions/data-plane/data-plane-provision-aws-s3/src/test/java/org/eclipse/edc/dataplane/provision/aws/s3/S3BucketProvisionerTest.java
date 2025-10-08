/*
 *  Copyright (c) 2025 Think-it GmbH
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Think-it GmbH - initial API and implementation
 */

package org.eclipse.edc.dataplane.provision.aws.s3;

import dev.failsafe.RetryPolicy;
import org.eclipse.edc.aws.s3.AwsClientProvider;
import org.eclipse.edc.aws.s3.S3ClientRequest;
import org.eclipse.edc.aws.s3.spi.S3BucketSchema;
import org.eclipse.edc.connector.dataplane.spi.provision.ProvisionResource;
import org.eclipse.edc.json.JacksonTypeManager;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.iam.IamAsyncClient;
import software.amazon.awssdk.services.iam.model.CreateRoleRequest;
import software.amazon.awssdk.services.iam.model.CreateRoleResponse;
import software.amazon.awssdk.services.iam.model.GetUserResponse;
import software.amazon.awssdk.services.iam.model.PutRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.PutRolePolicyResponse;
import software.amazon.awssdk.services.iam.model.Role;
import software.amazon.awssdk.services.iam.model.User;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.sts.StsAsyncClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.time.Instant;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.edc.junit.assertions.AbstractResultAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class S3BucketProvisionerTest {

    private final IamAsyncClient iamClient = mock();
    private final StsAsyncClient stsClient = mock();
    private final S3AsyncClient s3Client = mock();
    private final AwsClientProvider clientProvider = mock();
    private final Vault vault = mock();
    private S3BucketProvisioner provisioner;

    @BeforeEach
    void setUp() {
        when(clientProvider.iamAsyncClient(any(S3ClientRequest.class))).thenReturn(iamClient);
        when(clientProvider.s3AsyncClient(any(S3ClientRequest.class))).thenReturn(s3Client);
        when(clientProvider.stsAsyncClient(any(S3ClientRequest.class))).thenReturn(stsClient);

        var configuration = new S3BucketProvisionerConfiguration(2, 3600);

        provisioner = new S3BucketProvisioner(clientProvider, mock(Monitor.class), vault, RetryPolicy.ofDefaults(), configuration, new JacksonTypeManager());
    }

    @Test
    void shouldCreateTemporaryCredentialsAndBucket() {
        var userResponse = GetUserResponse.builder().user(User.builder().arn("testarn").build()).build();
        var createRoleResponse = CreateRoleResponse.builder().role(Role.builder().roleName("roleName").arn("testarn").build()).build();
        var putRolePolicyResponse = PutRolePolicyResponse.builder().build();
        when(iamClient.getUser()).thenReturn(completedFuture(userResponse));
        when(iamClient.createRole(isA(CreateRoleRequest.class))).thenReturn(completedFuture(createRoleResponse));
        when(iamClient.putRolePolicy(isA(PutRolePolicyRequest.class))).thenReturn(completedFuture(putRolePolicyResponse));

        var credentials = Credentials.builder()
                .accessKeyId("accessKeyId").secretAccessKey("secretAccessKey").sessionToken("sessionToken")
                .expiration(Instant.now()).build();
        var assumeRoleResponse = AssumeRoleResponse.builder().credentials(credentials).build();
        when(stsClient.assumeRole(isA(AssumeRoleRequest.class))).thenReturn(completedFuture(assumeRoleResponse));

        when(s3Client.headBucket(isA(HeadBucketRequest.class))).thenReturn(failedFuture(new RuntimeException("any")));
        var createBucketResponse = CreateBucketResponse.builder().build();
        when(s3Client.createBucket(isA(CreateBucketRequest.class))).thenReturn(completedFuture(createBucketResponse));
        var definition = ProvisionResource.Builder.newInstance()
                .dataAddress(DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE)
                        .property(S3BucketSchema.REGION, Region.US_EAST_1.id())
                        .property(S3BucketSchema.BUCKET_NAME, "test")
                        .build())
                .flowId("test")
                .build();

        var response = provisioner.provision(definition);

        assertThat(response).succeedsWithin(1, SECONDS).satisfies(result -> {
            assertThat(result).isSucceeded().satisfies(provisionedResource -> {
                var dataAddress = provisionedResource.getDataAddress();
                assertThat(dataAddress.getKeyName()).isNotBlank();
                verify(vault).storeSecret(eq(dataAddress.getKeyName()), any());
            });
        });

        verify(iamClient).putRolePolicy(isA(PutRolePolicyRequest.class));
        verify(s3Client).createBucket(isA(CreateBucketRequest.class));
    }

    @Test
    void shouldFailOnError() {
        when(s3Client.headBucket(isA(HeadBucketRequest.class))).thenReturn(failedFuture(new RuntimeException("any")));
        when(s3Client.createBucket(isA(CreateBucketRequest.class))).thenReturn(failedFuture(new RuntimeException("any")));
        var definition = ProvisionResource.Builder.newInstance()
                .dataAddress(DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE)
                        .property(S3BucketSchema.REGION, Region.US_EAST_1.id())
                        .property(S3BucketSchema.BUCKET_NAME, "test")
                        .build())
                .flowId("test")
                .build();

        var response = provisioner.provision(definition);

        assertThat(response).failsWithin(1, SECONDS);
        verify(s3Client).createBucket(isA(CreateBucketRequest.class));
    }

    @Test
    void shouldCreateTemporaryCredentialsAndUseExistingBucket_whenBucketAlreadyExist() {
        var userResponse = GetUserResponse.builder().user(User.builder().arn("testarn").build()).build();
        var createRoleResponse = CreateRoleResponse.builder().role(Role.builder().roleName("roleName").arn("testarn").build()).build();
        var putRolePolicyResponse = PutRolePolicyResponse.builder().build();
        when(iamClient.getUser()).thenReturn(completedFuture(userResponse));
        when(iamClient.createRole(isA(CreateRoleRequest.class))).thenReturn(completedFuture(createRoleResponse));
        when(iamClient.putRolePolicy(isA(PutRolePolicyRequest.class))).thenReturn(completedFuture(putRolePolicyResponse));

        var credentials = Credentials.builder()
                .accessKeyId("accessKeyId").secretAccessKey("secretAccessKey").sessionToken("sessionToken")
                .expiration(Instant.now()).build();
        var assumeRoleResponse = AssumeRoleResponse.builder().credentials(credentials).build();
        when(stsClient.assumeRole(isA(AssumeRoleRequest.class))).thenReturn(completedFuture(assumeRoleResponse));
        when(s3Client.headBucket(isA(HeadBucketRequest.class))).thenReturn(completedFuture(null));
        var definition = ProvisionResource.Builder.newInstance()
                .dataAddress(DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE)
                        .property(S3BucketSchema.REGION, Region.US_EAST_1.id())
                        .property(S3BucketSchema.BUCKET_NAME, "test")
                        .build())
                .flowId("test")
                .build();

        var response = provisioner.provision(definition);

        assertThat(response).succeedsWithin(1, SECONDS).satisfies(result -> {
            assertThat(result).isSucceeded().satisfies(provisionedResource -> {
                var dataAddress = provisionedResource.getDataAddress();
                assertThat(dataAddress.getKeyName()).isNotBlank();
                verify(vault).storeSecret(eq(dataAddress.getKeyName()), any());
            });
        });
        verify(iamClient).putRolePolicy(isA(PutRolePolicyRequest.class));
        verify(s3Client).headBucket(isA(HeadBucketRequest.class));
        verify(s3Client, times(0)).createBucket(isA(CreateBucketRequest.class));
    }

}
