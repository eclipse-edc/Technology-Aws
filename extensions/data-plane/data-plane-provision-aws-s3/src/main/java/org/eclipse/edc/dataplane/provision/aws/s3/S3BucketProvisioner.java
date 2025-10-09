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
 *
 */

package org.eclipse.edc.dataplane.provision.aws.s3;

import com.fasterxml.jackson.core.JsonProcessingException;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import org.eclipse.edc.aws.s3.AwsClientProvider;
import org.eclipse.edc.aws.s3.AwsTemporarySecretToken;
import org.eclipse.edc.aws.s3.S3ClientRequest;
import org.eclipse.edc.aws.s3.spi.S3BucketSchema;
import org.eclipse.edc.connector.dataplane.spi.provision.ProvisionResource;
import org.eclipse.edc.connector.dataplane.spi.provision.ProvisionedResource;
import org.eclipse.edc.connector.dataplane.spi.provision.Provisioner;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.response.ResponseStatus;
import org.eclipse.edc.spi.response.StatusResult;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.jetbrains.annotations.Nullable;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.iam.IamAsyncClient;
import software.amazon.awssdk.services.iam.model.CreateRoleRequest;
import software.amazon.awssdk.services.iam.model.CreateRoleResponse;
import software.amazon.awssdk.services.iam.model.GetUserResponse;
import software.amazon.awssdk.services.iam.model.PutRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.Role;
import software.amazon.awssdk.services.iam.model.Tag;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.sts.StsAsyncClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;

/**
 * Asynchronously provisions S3 buckets.
 */
public class S3BucketProvisioner implements Provisioner {

    // Do not modify this trust policy
    private static final String ASSUME_ROLE_TRUST = "{" +
            "  \"Version\": \"2012-10-17\"," +
            "  \"Statement\": [" +
            "    {" +
            "      \"Effect\": \"Allow\"," +
            "      \"Principal\": {" +
            "        \"AWS\": \"%s\"" +
            "      }," +
            "      \"Action\": \"sts:AssumeRole\"" +
            "    }" +
            "  ]" +
            "}";
    // Do not modify this bucket policy
    private static final String BUCKET_POLICY = "{" +
            "    \"Version\": \"2012-10-17\"," +
            "    \"Statement\": [" +
            "        {" +
            "            \"Sid\": \"TemporaryAccess\", " +
            "            \"Effect\": \"Allow\"," +
            "            \"Action\": \"s3:PutObject\"," +
            "            \"Resource\": \"arn:aws:s3:::%s/*\"" +
            "        }" +
            "    ]" +
            "}";

    private final AwsClientProvider clientProvider;
    private final Monitor monitor;
    private final Vault vault;
    private final TypeManager typeManager;
    private final RetryPolicy<Object> retryPolicy;
    private final S3BucketProvisionerConfiguration configuration;

    public S3BucketProvisioner(AwsClientProvider clientProvider, Monitor monitor, Vault vault, RetryPolicy<Object> retryPolicy, S3BucketProvisionerConfiguration configuration, TypeManager typeManager) {
        this.clientProvider = clientProvider;
        this.monitor = monitor;
        this.configuration = configuration;
        this.vault = vault;
        this.typeManager = typeManager;
        this.retryPolicy = RetryPolicy.builder(retryPolicy.getConfig())
                .withMaxRetries(configuration.maxRetries())
                .handle(AwsServiceException.class)
                .build();
    }

    @Override
    public String supportedType() {
        return S3BucketSchema.TYPE;
    }

    @Override
    public CompletableFuture<StatusResult<ProvisionedResource>> provision(ProvisionResource resource) {
        var dataAddress = resource.getDataAddress();
        var clientRequest = S3ClientRequest.from(
                dataAddress.getStringProperty(S3BucketSchema.REGION),
                dataAddress.getStringProperty(S3BucketSchema.ENDPOINT_OVERRIDE));

        var s3AsyncClient = clientProvider.s3AsyncClient(clientRequest);
        var iamClient = clientProvider.iamAsyncClient(clientRequest);
        var stsClient = clientProvider.stsAsyncClient(clientRequest);
        var bucketName = dataAddress.getStringProperty(S3BucketSchema.BUCKET_NAME);
        var headBucketRequest = HeadBucketRequest.builder().bucket(bucketName).build();

        return s3AsyncClient.headBucket(headBucketRequest).thenCompose(response -> {
            monitor.debug("S3ProvisionPipeline: bucket " + bucketName + " already exists, skipping creation");
            return CompletableFuture.completedFuture(null);
        }).exceptionally(throwable -> {
            monitor.debug("S3ProvisionPipeline: creating bucket " + bucketName + " as it does not exist");
            var createBucketRequest = CreateBucketRequest.builder().bucket(bucketName)
                    .createBucketConfiguration(CreateBucketConfiguration.builder().build()).build();
            return s3AsyncClient.createBucket(createBucketRequest);
        })
        .thenCompose(r -> getUser(iamClient))
        .thenCompose(response -> createRole(iamClient, response, resource.getFlowId()))
        .thenCompose(response -> createRolePolicy(iamClient, response, bucketName, resource.getFlowId()))
        .thenCompose(role -> assumeRole(stsClient, role)
                .thenApply(credentials -> provisionSucceeded(resource, role, credentials)));
    }

    private CompletableFuture<Role> createRolePolicy(IamAsyncClient iamAsyncClient, CreateRoleResponse response, @Nullable String bucketName, String flowId) {
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            var role = response.role();
            var policyRequest = PutRolePolicyRequest.builder()
                    .policyName(flowId)
                    .roleName(role.roleName())
                    .policyDocument(format(BUCKET_POLICY, bucketName))
                    .build();

            monitor.debug("S3ProvisionPipeline: attach bucket policy to role " + role.arn());
            return iamAsyncClient.putRolePolicy(policyRequest)
                    .thenApply(policyResponse -> role);
        });
    }

    private CompletableFuture<CreateRoleResponse> createRole(IamAsyncClient iamClient, GetUserResponse response, String flowId) {
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            var userArn = response.user().arn();
            var tag = Tag.builder().key("dataspaceconnector:process").value(flowId).build();

            monitor.debug("S3ProvisionPipeline: create role for user" + userArn);
            var createRoleRequest = CreateRoleRequest.builder()
                    .roleName(flowId).description("EDC transfer process role")
                    .assumeRolePolicyDocument(format(ASSUME_ROLE_TRUST, userArn))
                    .maxSessionDuration(configuration.roleMaxSessionDuration())
                    .tags(tag)
                    .build();

            return iamClient.createRole(createRoleRequest);
        });
    }

    private CompletableFuture<GetUserResponse> getUser(IamAsyncClient iamAsyncClient) {
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            monitor.debug("S3ProvisionPipeline: get user");
            return iamAsyncClient.getUser();
        });
    }

    private CompletableFuture<Credentials> assumeRole(StsAsyncClient stsClient, Role role) {
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            monitor.debug("S3ProvisionPipeline: attempting to assume the role");
            var roleRequest = AssumeRoleRequest.builder()
                    .roleArn(role.arn())
                    .roleSessionName("transfer")
                    .externalId("123")
                    .build();

            return stsClient.assumeRole(roleRequest).thenApply(AssumeRoleResponse::credentials);
        });
    }

    private StatusResult<ProvisionedResource> provisionSucceeded(ProvisionResource resourceDefinition, Role role, Credentials credentials) {
        var secretToken = new AwsTemporarySecretToken(credentials.accessKeyId(), credentials.secretAccessKey(), credentials.sessionToken(), credentials.expiration().toEpochMilli());
        var originalDataAddress = resourceDefinition.getDataAddress();
        var keyName = "resourceDefinition-" + resourceDefinition.getId() + "-secret-" + UUID.randomUUID();

        try {
            vault.storeSecret(keyName, typeManager.getMapper().writeValueAsString(secretToken));
        } catch (JsonProcessingException e) {
            return StatusResult.failure(ResponseStatus.FATAL_ERROR, "Cannot serialize secret token: " + e.getMessage());
        }

        var provisionedResource = ProvisionedResource.Builder.from(resourceDefinition)
                .dataAddress(DataAddress.Builder.newInstance()
                        .properties(originalDataAddress.getProperties())
                        .keyName(keyName)
                        .build())
                .property(S3BucketSchema.ROLE_NAME, role.roleName())
                .build();

        return StatusResult.success(provisionedResource);
    }

}


