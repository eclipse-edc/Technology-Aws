/*
 *  Copyright (c) 2020, 2021 Microsoft Corporation
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Microsoft Corporation - initial API and implementation
 *
 */

package org.eclipse.edc.connector.provision.aws.s3;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import org.eclipse.edc.aws.s3.AwsClientProvider;
import org.eclipse.edc.aws.s3.AwsSecretToken;
import org.eclipse.edc.aws.s3.S3ClientRequest;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.security.Vault;
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

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.SECRET_ACCESS_ALIAS_PREFIX;

public class S3ProvisionPipeline {

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

    private final RetryPolicy<Object> retryPolicy;
    private final AwsClientProvider clientProvider;
    private final Monitor monitor;
    private final Vault vault;
    private final int roleMaxSessionDuration;

    private S3ProvisionPipeline(RetryPolicy<Object> retryPolicy, AwsClientProvider clientProvider,
                                Monitor monitor, Vault vault, int roleMaxSessionDuration) {
        this.retryPolicy = retryPolicy;
        this.clientProvider = clientProvider;
        this.monitor = monitor;
        this.vault = vault;
        this.roleMaxSessionDuration = roleMaxSessionDuration;
    }

    /**
     * Performs a non-blocking provisioning operation.
     */
    public CompletableFuture<S3ProvisionResponse> provision(S3BucketResourceDefinition resourceDefinition) {
        var rq = createClientRequest(resourceDefinition);
        var s3AsyncClient = clientProvider.s3AsyncClient(rq);
        var iamClient = clientProvider.iamAsyncClient(rq);
        var stsClient = clientProvider.stsAsyncClient(rq);
        var bucketName = resourceDefinition.getBucketName();
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
                .thenCompose(response -> createRole(iamClient, resourceDefinition, response))
                .thenCompose(response -> createRolePolicy(iamClient, resourceDefinition, response))
                .thenCompose(role -> assumeRole(stsClient, role));
    }

    private S3ClientRequest createClientRequest(S3BucketResourceDefinition resourceDefinition) {
        return extractSecretToken(resourceDefinition)
                .map(secretToken -> S3ClientRequest.from(
                        resourceDefinition.getRegionId(),
                        resourceDefinition.getEndpointOverride(),
                        secretToken))
                .orElseGet(() -> S3ClientRequest.from(
                        resourceDefinition.getRegionId(),
                        resourceDefinition.getEndpointOverride()));
    }

    private Optional<AwsSecretToken> extractSecretToken(S3BucketResourceDefinition resourceDefinition) {
        return Optional.ofNullable(resourceDefinition.getAccessKeyId())
                .map(accessKeyId -> {
                    var secretAccessKey = vault.resolveSecret(SECRET_ACCESS_ALIAS_PREFIX + resourceDefinition.getId());
                    return secretAccessKey != null ? new AwsSecretToken(accessKeyId, secretAccessKey) : null;
                });
    }

    private CompletableFuture<Role> createRolePolicy(IamAsyncClient iamAsyncClient, S3BucketResourceDefinition resourceDefinition, CreateRoleResponse response) {
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            Role role = response.role();
            PutRolePolicyRequest policyRequest = PutRolePolicyRequest.builder()
                    .policyName(resourceDefinition.getTransferProcessId())
                    .roleName(role.roleName())
                    .policyDocument(format(BUCKET_POLICY, resourceDefinition.getBucketName()))
                    .build();

            monitor.debug("S3ProvisionPipeline: attach bucket policy to role " + role.arn());
            return iamAsyncClient.putRolePolicy(policyRequest)
                    .thenApply(policyResponse -> role);
        });
    }

    private CompletableFuture<CreateRoleResponse> createRole(IamAsyncClient iamClient, S3BucketResourceDefinition resourceDefinition, GetUserResponse response) {
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            String userArn = response.user().arn();
            Tag tag = Tag.builder().key("dataspaceconnector:process").value(resourceDefinition.getTransferProcessId()).build();

            monitor.debug("S3ProvisionPipeline: create role for user" + userArn);
            CreateRoleRequest createRoleRequest = CreateRoleRequest.builder()
                    .roleName(resourceDefinition.getTransferProcessId()).description("EDC transfer process role")
                    .assumeRolePolicyDocument(format(ASSUME_ROLE_TRUST, userArn))
                    .maxSessionDuration(roleMaxSessionDuration)
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

    private CompletableFuture<S3ProvisionResponse> assumeRole(StsAsyncClient stsClient, Role role) {
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            monitor.debug("S3ProvisionPipeline: attempting to assume the role");
            AssumeRoleRequest roleRequest = AssumeRoleRequest.builder()
                    .roleArn(role.arn())
                    .roleSessionName("transfer")
                    .externalId("123")
                    .build();

            return stsClient.assumeRole(roleRequest)
                    .thenApply(response -> new S3ProvisionResponse(role, response.credentials()));
        });
    }

    static class Builder {
        private final RetryPolicy<Object> retryPolicy;
        private int roleMaxSessionDuration;
        private Monitor monitor;
        private AwsClientProvider clientProvider;
        private Vault vault;

        private Builder(RetryPolicy<Object> retryPolicy) {
            this.retryPolicy = retryPolicy;
        }

        public static Builder newInstance(RetryPolicy<Object> policy) {
            return new Builder(policy);
        }

        public Builder roleMaxSessionDuration(int roleMaxSessionDuration) {
            this.roleMaxSessionDuration = roleMaxSessionDuration;
            return this;
        }

        public Builder monitor(Monitor monitor) {
            this.monitor = monitor;
            return this;
        }

        public Builder clientProvider(AwsClientProvider clientProvider) {
            this.clientProvider = clientProvider;
            return this;
        }

        public Builder vault(Vault vault) {
            this.vault = vault;
            return this;
        }

        public S3ProvisionPipeline build() {
            Objects.requireNonNull(retryPolicy);
            Objects.requireNonNull(clientProvider);
            Objects.requireNonNull(monitor);
            Objects.requireNonNull(vault);
            return new S3ProvisionPipeline(retryPolicy, clientProvider, monitor, vault, roleMaxSessionDuration);
        }
    }

}
