/*
 *  Copyright (c) 2024 Cofinity-X
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Cofinity-X - initial API and implementation
 *
 */

package org.eclipse.edc.connector.provision.aws.s3.copy;

import com.fasterxml.jackson.core.type.TypeReference;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import org.eclipse.edc.aws.s3.AwsClientProvider;
import org.eclipse.edc.aws.s3.AwsSecretToken;
import org.eclipse.edc.aws.s3.AwsTemporarySecretToken;
import org.eclipse.edc.aws.s3.S3ClientRequest;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.DeprovisionedResource;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.SecretToken;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.util.string.StringUtils;
import software.amazon.awssdk.services.iam.IamAsyncClient;
import software.amazon.awssdk.services.iam.model.DeleteRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.DeleteRolePolicyResponse;
import software.amazon.awssdk.services.iam.model.DeleteRoleRequest;
import software.amazon.awssdk.services.iam.model.DeleteRoleResponse;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteBucketPolicyRequest;
import software.amazon.awssdk.services.s3.model.GetBucketPolicyRequest;
import software.amazon.awssdk.services.s3.model.GetBucketPolicyResponse;
import software.amazon.awssdk.services.s3.model.PutBucketPolicyRequest;
import software.amazon.awssdk.services.s3.model.S3Response;

import java.io.IOException;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static java.util.Optional.ofNullable;

public class CrossAccountCopyDeprovisionPipeline {
    
    private final AwsClientProvider clientProvider;
    private final Vault vault;
    private final RetryPolicy<Object> retryPolicy;
    private final TypeManager typeManager;
    private final Monitor monitor;
    
    private CrossAccountCopyDeprovisionPipeline(AwsClientProvider clientProvider, Vault vault, RetryPolicy<Object> retryPolicy, TypeManager typeManager, Monitor monitor) {
        this.clientProvider = clientProvider;
        this.vault = vault;
        this.retryPolicy = retryPolicy;
        this.typeManager = typeManager;
        this.monitor = monitor;
    }
    
    public CompletableFuture<DeprovisionedResource> deprovision(CrossAccountCopyProvisionedResource provisionedResource) {
        var secretToken = getSecretTokenFromVault(provisionedResource.getDestinationKeyName());
        var s3ClientRequest = S3ClientRequest.from(provisionedResource.getDestinationRegion(), null, secretToken);
        var s3Client = clientProvider.s3AsyncClient(s3ClientRequest);
        
        var iamClient = clientProvider.iamAsyncClient();
        var roleName = provisionedResource.getSourceAccountRole().roleName();
        
        var getBucketPolicyRequest = GetBucketPolicyRequest.builder()
                .bucket(provisionedResource.getDestinationBucketName())
                .build();
        
        monitor.debug("S3 CrossAccountCopyProvisionPipeline: getting destination bucket policy");
        return s3Client.getBucketPolicy(getBucketPolicyRequest)
                .thenCompose(response -> updateBucketPolicy(s3Client, provisionedResource, response))
                .thenCompose(response -> deleteRolePolicy(iamClient, roleName))
                .thenCompose(response -> deleteRole(iamClient, roleName))
                .thenApply(response -> DeprovisionedResource.Builder.newInstance()
                        .provisionedResourceId(provisionedResource.getId())
                        .build());
    }
    
    private CompletableFuture<? extends S3Response> updateBucketPolicy(S3AsyncClient s3Client,
                                                             CrossAccountCopyProvisionedResource provisionedResource,
                                                             GetBucketPolicyResponse bucketPolicyResponse) {
        var bucketPolicy = bucketPolicyResponse.policy();
        var typeReference = new TypeReference<HashMap<String,Object>>() {};
        var statementSid = provisionedResource.getBucketPolicyStatementSid();
        var policyJson = Json.createObjectBuilder(typeManager.readValue(bucketPolicy, typeReference)).build();
        
        var statementsBuilder = Json.createArrayBuilder();
        
        policyJson.getJsonArray("Statement").forEach(entry -> {
            var statement = (JsonObject) entry;
            if (!statementSid.equals(statement.getJsonString("Sid").getString())) {
                statementsBuilder.add(statement);
            }
        });
        
        var updatedBucketPolicy = Json.createObjectBuilder(policyJson)
                .add("Statement", statementsBuilder)
                .build();
        
        // since putting a bucket policy with empty statement array fails using the SDK, the bucket
        // policy is deleted if no statements are present
        if (updatedBucketPolicy.getJsonArray("Statement").isEmpty()) {
            return Failsafe.with(retryPolicy).getStageAsync(() -> {
                monitor.debug("S3 CrossAccountCopyProvisionPipeline: updating destination bucket policy");
                var deleteBucketPolicyRequest = DeleteBucketPolicyRequest.builder()
                        .bucket(provisionedResource.getDestinationBucketName())
                        .build();
                return s3Client.deleteBucketPolicy(deleteBucketPolicyRequest);
            });
        } else {
            return Failsafe.with(retryPolicy).getStageAsync(() -> {
                monitor.debug("S3 CrossAccountCopyProvisionPipeline: updating destination bucket policy");
                var putBucketPolicyRequest = PutBucketPolicyRequest.builder()
                        .bucket(provisionedResource.getDestinationBucketName())
                        .policy(updatedBucketPolicy.toString())
                        .build();
                return s3Client.putBucketPolicy(putBucketPolicyRequest);
            });
        }
    }
    
    private CompletableFuture<DeleteRolePolicyResponse> deleteRolePolicy(IamAsyncClient iamClient, String roleName) {
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            var deleteRolePolicyRequest = DeleteRolePolicyRequest.builder()
                    .roleName(roleName)
                    .policyName(roleName)
                    .build();
            
            monitor.debug("S3 CrossAccountCopyProvisionPipeline: deleting IAM role policy");
            return iamClient.deleteRolePolicy(deleteRolePolicyRequest);
        });
    }
    
    private CompletableFuture<DeleteRoleResponse> deleteRole(IamAsyncClient iamClient, String roleName) {
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            monitor.debug("S3 CrossAccountCopyProvisionPipeline: deleting IAM role");
            var deleteRoleRequest = DeleteRoleRequest.builder()
                    .roleName(roleName)
                    .build();
            
            return iamClient.deleteRole(deleteRoleRequest);
        });
    }
    
    private SecretToken getSecretTokenFromVault(String secretKeyName) {
        return ofNullable(secretKeyName)
                .filter(keyName -> !StringUtils.isNullOrBlank(keyName))
                .map(vault::resolveSecret)
                .filter(secret -> !StringUtils.isNullOrBlank(secret))
                .map(this::deserializeSecretToken)
                .orElse(null);
    }
    
    private SecretToken deserializeSecretToken(String secret) {
        try {
            var objectMapper = typeManager.getMapper();
            var tree = objectMapper.readTree(secret);
            if (tree.has("sessionToken")) {
                return objectMapper.treeToValue(tree, AwsTemporarySecretToken.class);
            } else {
                return objectMapper.treeToValue(tree, AwsSecretToken.class);
            }
        } catch (IOException e) {
            throw new EdcException(e);
        }
    }
    
    static class Builder {
        private AwsClientProvider clientProvider;
        private Vault vault;
        private RetryPolicy<Object> retryPolicy;
        private TypeManager typeManager;
        private Monitor monitor;
        
        private Builder() {}
        
        public static Builder newInstance() {
            return new Builder();
        }
        
        public Builder clientProvider(AwsClientProvider clientProvider) {
            this.clientProvider = clientProvider;
            return this;
        }
        
        public Builder vault(Vault vault) {
            this.vault = vault;
            return this;
        }
        
        public Builder retryPolicy(RetryPolicy<Object> retryPolicy) {
            this.retryPolicy = retryPolicy;
            return this;
        }
        
        public Builder typeManager(TypeManager typeManager) {
            this.typeManager = typeManager;
            return this;
        }
        
        public Builder monitor(Monitor monitor) {
            this.monitor = monitor;
            return this;
        }
        
        public CrossAccountCopyDeprovisionPipeline build() {
            Objects.requireNonNull(clientProvider);
            Objects.requireNonNull(vault);
            Objects.requireNonNull(retryPolicy);
            Objects.requireNonNull(typeManager);
            Objects.requireNonNull(monitor);
            return new CrossAccountCopyDeprovisionPipeline(clientProvider, vault, retryPolicy, typeManager, monitor);
        }
    }
}
