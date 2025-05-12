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
import org.eclipse.edc.connector.controlplane.transfer.spi.types.DeprovisionedResource;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.security.Vault;
import software.amazon.awssdk.services.iam.IamAsyncClient;
import software.amazon.awssdk.services.iam.model.DeleteRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.DeleteRolePolicyResponse;
import software.amazon.awssdk.services.iam.model.DeleteRoleRequest;
import software.amazon.awssdk.services.iam.model.DeleteRoleResponse;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.SECRET_ACCESS_ALIAS_PREFIX;

public class S3DeprovisionPipeline {

    private final RetryPolicy<Object> retryPolicy;
    private final AwsClientProvider clientProvider;
    private final Monitor monitor;
    private final Vault vault;

    public S3DeprovisionPipeline(RetryPolicy<Object> retryPolicy, AwsClientProvider clientProvider, Monitor monitor, Vault vault) {
        this.retryPolicy = retryPolicy;
        this.clientProvider = clientProvider;
        this.monitor = monitor;
        this.vault = vault;
    }

    /**
     * Performs a non-blocking deprovisioning operation.
     */
    public CompletableFuture<DeprovisionedResource> deprovision(S3BucketProvisionedResource resource) {
        var rq = createClientRequest(resource);
        var s3Client = clientProvider.s3AsyncClient(rq);
        var iamClient = clientProvider.iamAsyncClient(rq);

        var bucketName = resource.getBucketName();
        var role = resource.getRole();

        var listObjectsRequest = ListObjectsV2Request.builder().bucket(bucketName).build();
        monitor.debug("S3DeprovisionPipeline: list objects");
        return s3Client.listObjectsV2(listObjectsRequest)
                .thenCompose(listObjectsResponse -> deleteObjects(s3Client, bucketName, listObjectsResponse))
                .thenCompose(deleteObjectsResponse -> deleteBucket(s3Client, bucketName))
                .thenCompose(listAttachedRolePoliciesResponse -> deleteRolePolicy(iamClient, role))
                .thenCompose(deleteRolePolicyResponse -> deleteRole(iamClient, role))
                .thenCompose(deleteSecretResponse -> deleteSecret(resource))
                .thenApply(response -> DeprovisionedResource.Builder.newInstance().provisionedResourceId(resource.getId()).build());
    }

    private CompletableFuture<DeleteRoleResponse> deleteRole(IamAsyncClient iamClient, String role) {
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            monitor.debug("S3DeprovisionPipeline: delete role");
            return iamClient.deleteRole(DeleteRoleRequest.builder().roleName(role).build());
        });
    }

    private CompletableFuture<DeleteRolePolicyResponse> deleteRolePolicy(IamAsyncClient iamClient, String role) {
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            monitor.debug("S3DeprovisionPipeline: deleting inline policies for Role " + role);
            return iamClient.deleteRolePolicy(DeleteRolePolicyRequest.builder().roleName(role).policyName(role).build());
        });
    }

    private CompletableFuture<DeleteBucketResponse> deleteBucket(S3AsyncClient s3Client, String bucketName) {
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            monitor.debug("S3DeprovisionPipeline: delete bucket");
            return s3Client.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build());
        });
    }

    private CompletableFuture<DeleteObjectsResponse> deleteObjects(S3AsyncClient s3Client, String bucketName, ListObjectsV2Response listObjectsResponse) {
        var identifiers = listObjectsResponse.contents().stream()
                .map(s3object -> ObjectIdentifier.builder().key(s3object.key()).build())
                .collect(Collectors.toList());

        var deleteRequest = DeleteObjectsRequest.builder()
                .bucket(bucketName).delete(Delete.builder().objects(identifiers).build())
                .build();
        monitor.debug("S3DeprovisionPipeline: delete bucket contents: " + identifiers.stream().map(ObjectIdentifier::key).collect(joining(", ")));
        return s3Client.deleteObjects(deleteRequest);
    }

    private CompletableFuture<?> deleteSecret(S3BucketProvisionedResource resource) {
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            monitor.debug("S3DeprovisionPipeline: delete secret from vault");
            var response = vault.deleteSecret(SECRET_ACCESS_ALIAS_PREFIX + resource.getId());
            return CompletableFuture.completedFuture(response);
        });
    }

    private S3ClientRequest createClientRequest(S3BucketProvisionedResource resource) {
        return extractSecretToken(resource)
                .map(secretToken -> S3ClientRequest.from(
                        resource.getRegion(),
                        resource.getEndpointOverride(),
                        secretToken))
                .orElseGet(() -> S3ClientRequest.from(
                        resource.getRegion(),
                        resource.getEndpointOverride()));
    }

    private Optional<AwsSecretToken> extractSecretToken(S3BucketProvisionedResource resource) {
        return Optional.ofNullable(resource.getAccessKeyId())
                .map(accessKeyId -> {
                    var secretAccessKey = vault.resolveSecret(SECRET_ACCESS_ALIAS_PREFIX + resource.getId());
                    return secretAccessKey != null ? new AwsSecretToken(accessKeyId, secretAccessKey) : null;
                });
    }

    static class Builder {
        private final RetryPolicy<Object> retryPolicy;
        private Monitor monitor;
        private AwsClientProvider clientProvider;
        private Vault vault;

        private Builder(RetryPolicy<Object> retryPolicy) {
            this.retryPolicy = retryPolicy;
        }

        public static Builder newInstance(RetryPolicy<Object> policy) {
            return new Builder(policy);
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

        public S3DeprovisionPipeline build() {
            Objects.requireNonNull(retryPolicy);
            Objects.requireNonNull(clientProvider);
            Objects.requireNonNull(monitor);
            Objects.requireNonNull(vault);
            return new S3DeprovisionPipeline(retryPolicy, clientProvider, monitor, vault);
        }
    }
}
