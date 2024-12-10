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
import org.eclipse.edc.aws.s3.AwsClientProvider;
import org.eclipse.edc.aws.s3.S3ClientRequest;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import software.amazon.awssdk.services.iam.IamAsyncClient;
import software.amazon.awssdk.services.iam.model.CreateRoleRequest;
import software.amazon.awssdk.services.iam.model.CreateRoleResponse;
import software.amazon.awssdk.services.iam.model.GetUserResponse;
import software.amazon.awssdk.services.iam.model.PutRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.Tag;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetBucketPolicyRequest;
import software.amazon.awssdk.services.s3.model.PutBucketPolicyRequest;
import software.amazon.awssdk.services.sts.StsAsyncClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.BUCKET_NAME;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.ENDPOINT_OVERRIDE;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.REGION;
import static org.eclipse.edc.connector.provision.aws.s3.copy.util.S3CopyConstants.PLACEHOLDER_DESTINATION_BUCKET;
import static org.eclipse.edc.connector.provision.aws.s3.copy.util.S3CopyConstants.PLACEHOLDER_ROLE_ARN;
import static org.eclipse.edc.connector.provision.aws.s3.copy.util.S3CopyConstants.PLACEHOLDER_SOURCE_BUCKET;
import static org.eclipse.edc.connector.provision.aws.s3.copy.util.S3CopyConstants.PLACEHOLDER_STATEMENT_SID;
import static org.eclipse.edc.connector.provision.aws.s3.copy.util.S3CopyConstants.PLACEHOLDER_USER_ARN;
import static org.eclipse.edc.connector.provision.aws.s3.copy.util.S3CopyConstants.S3_BUCKET_POLICY_STATEMENT;
import static org.eclipse.edc.connector.provision.aws.s3.copy.util.S3CopyTemplates.BUCKET_POLICY_STATEMENT_TEMPLATE;
import static org.eclipse.edc.connector.provision.aws.s3.copy.util.S3CopyTemplates.CROSS_ACCOUNT_ROLE_POLICY_TEMPLATE;
import static org.eclipse.edc.connector.provision.aws.s3.copy.util.S3CopyTemplates.CROSS_ACCOUNT_ROLE_TRUST_POLICY_TEMPLATE;
import static org.eclipse.edc.connector.provision.aws.s3.copy.util.S3CopyTemplates.EMPTY_BUCKET_POLICY;
import static org.eclipse.edc.connector.provision.aws.s3.copy.util.S3CopyUtils.getSecretTokenFromVault;
import static org.eclipse.edc.connector.provision.aws.s3.copy.util.S3CopyUtils.resourceIdentifier;

public class S3CopyProvisionPipeline {
    
    private final AwsClientProvider clientProvider;
    private final Vault vault;
    private final RetryPolicy<Object> retryPolicy;
    private final TypeManager typeManager;
    private final Monitor monitor;
    private final String componentId;
    private final int maxRoleSessionDuration;
    
    private S3CopyProvisionPipeline(AwsClientProvider clientProvider, Vault vault,
                                    RetryPolicy<Object> retryPolicy, TypeManager typeManager,
                                    Monitor monitor, String componentId, int maxRoleSessionDuration) {
        this.clientProvider = clientProvider;
        this.vault = vault;
        this.retryPolicy = retryPolicy;
        this.typeManager = typeManager;
        this.monitor = monitor;
        this.componentId = componentId;
        this.maxRoleSessionDuration = maxRoleSessionDuration;
    }
    
    public CompletableFuture<S3ProvisionResponse> provision(S3CopyResourceDefinition resourceDefinition) {
        // create IAM & STS client for source account -> configure & assume IAM role
        var sourceClientRequest = S3ClientRequest.from(resourceDefinition.getSourceDataAddress().getStringProperty(REGION), resourceDefinition.getEndpointOverride(), null);
        var iamClient = clientProvider.iamAsyncClient(sourceClientRequest);
        var stsClient = clientProvider.stsAsyncClient(sourceClientRequest);
        
        // create S3 client for destination account -> update bucket policy to allow source account role to write objects
        var destinationSecretToken = getSecretTokenFromVault(resourceDefinition.getDestinationKeyName(), vault, typeManager);
        var destinationClientRequest = S3ClientRequest.from(resourceDefinition.getDestinationRegion(), resourceDefinition.getEndpointOverride(), destinationSecretToken);
        var s3Client = clientProvider.s3AsyncClient(destinationClientRequest);
        
        monitor.debug("S3CopyProvisionPipeline: getting IAM user");
        return iamClient.getUser()
                .thenCompose(response -> createRole(iamClient, resourceDefinition, response))
                .thenCompose(response -> putRolePolicy(iamClient, resourceDefinition, response))
                .thenCompose(provisionSteps -> getDestinationBucketPolicy(s3Client, resourceDefinition, provisionSteps))
                .thenCompose(provisionSteps -> updateDestinationBucketPolicy(s3Client, resourceDefinition, provisionSteps))
                .thenCompose(role -> assumeRole(stsClient, resourceDefinition, role));
    }
    
    private CompletableFuture<CreateRoleResponse> createRole(IamAsyncClient iamClient,
                                                             S3CopyResourceDefinition resourceDefinition,
                                                             GetUserResponse getUserResponse) {
        var roleName = resourceIdentifier(resourceDefinition);
        var trustPolicy = CROSS_ACCOUNT_ROLE_TRUST_POLICY_TEMPLATE
                .replace(PLACEHOLDER_USER_ARN, getUserResponse.user().arn());
        
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            var createRoleRequest = CreateRoleRequest.builder()
                    .roleName(roleName)
                    .description(format("Role for EDC transfer: %s", roleName))
                    .assumeRolePolicyDocument(trustPolicy)
                    .maxSessionDuration(maxRoleSessionDuration)
                    .tags(roleTags(resourceDefinition))
                    .build();
            
            monitor.debug(format("S3CopyProvisionPipeline: creating IAM role '%s'", roleName));
            return iamClient.createRole(createRoleRequest);
        });
    }
    
    private CompletableFuture<S3CopyProvisionSteps> putRolePolicy(IamAsyncClient iamClient,
                                                                  S3CopyResourceDefinition resourceDefinition,
                                                                  CreateRoleResponse createRoleResponse) {
        var rolePolicy = CROSS_ACCOUNT_ROLE_POLICY_TEMPLATE
                .replace(PLACEHOLDER_SOURCE_BUCKET, resourceDefinition.getSourceDataAddress().getStringProperty(BUCKET_NAME))
                .replace(PLACEHOLDER_DESTINATION_BUCKET, resourceDefinition.getDestinationBucketName());
        
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            var role = createRoleResponse.role();
            var putRolePolicyRequest = PutRolePolicyRequest.builder()
                    .roleName(role.roleName())
                    .policyName(resourceIdentifier(resourceDefinition))
                    .policyDocument(rolePolicy)
                    .build();
            
            monitor.debug("S3CopyProvisionPipeline: putting IAM role policy");
            return iamClient.putRolePolicy(putRolePolicyRequest)
                    .thenApply(policyResponse -> new S3CopyProvisionSteps(role));
        });
    }
    
    private CompletableFuture<S3CopyProvisionSteps> getDestinationBucketPolicy(S3AsyncClient s3Client,
                                                                               S3CopyResourceDefinition resourceDefinition,
                                                                               S3CopyProvisionSteps provisionSteps) {
        var getBucketPolicyRequest = GetBucketPolicyRequest.builder()
                .bucket(resourceDefinition.getDestinationBucketName())
                .build();
        
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            monitor.debug("S3CopyProvisionPipeline: getting destination bucket policy");
            return s3Client.getBucketPolicy(getBucketPolicyRequest)
                    .handle((result, ex) -> {
                        if (ex == null) {
                            provisionSteps.setBucketPolicy(result.policy());
                            return provisionSteps;
                        } else {
                            provisionSteps.setBucketPolicy(EMPTY_BUCKET_POLICY);
                            return provisionSteps;
                        }
                    });
        });
    }
    
    private CompletableFuture<S3CopyProvisionSteps> updateDestinationBucketPolicy(S3AsyncClient s3Client,
                                                                                  S3CopyResourceDefinition resourceDefinition,
                                                                                  S3CopyProvisionSteps provisionSteps) {
        var bucketPolicyStatement = BUCKET_POLICY_STATEMENT_TEMPLATE
                .replace(PLACEHOLDER_STATEMENT_SID, resourceDefinition.getBucketPolicyStatementSid())
                .replace(PLACEHOLDER_ROLE_ARN, provisionSteps.getRole().arn())
                .replace(PLACEHOLDER_DESTINATION_BUCKET, resourceDefinition.getDestinationBucketName());
        
        var typeReference = new TypeReference<HashMap<String, Object>>() {};
        var statementJson = Json.createObjectBuilder(typeManager.readValue(bucketPolicyStatement, typeReference)).build();
        var policyJson = Json.createObjectBuilder(typeManager.readValue(provisionSteps.getBucketPolicy(), typeReference)).build();
        
        var statements = Json.createArrayBuilder(policyJson.getJsonArray(S3_BUCKET_POLICY_STATEMENT))
                .add(statementJson)
                .build();
        var updatedBucketPolicy = Json.createObjectBuilder(policyJson)
                .add(S3_BUCKET_POLICY_STATEMENT, statements)
                .build().toString();
        
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            monitor.debug("S3CopyProvisionPipeline: updating destination bucket policy");
            var putBucketPolicyRequest = PutBucketPolicyRequest.builder()
                    .bucket(resourceDefinition.getDestinationBucketName())
                    .policy(updatedBucketPolicy)
                    .build();
            return s3Client.putBucketPolicy(putBucketPolicyRequest)
                    .thenApply(response -> provisionSteps);
        });
    }
    
    private CompletableFuture<S3ProvisionResponse> assumeRole(StsAsyncClient stsClient,
                                                              S3CopyResourceDefinition resourceDefinition,
                                                              S3CopyProvisionSteps provisionSteps) {
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            var role = provisionSteps.getRole();
            var assumeRoleRequest = AssumeRoleRequest.builder()
                    .roleArn(role.arn())
                    .roleSessionName(resourceIdentifier(resourceDefinition))
                    .build();
            
            monitor.debug(format("S3CopyProvisionPipeline: assuming role '%s'", role.arn()));
            return stsClient.assumeRole(assumeRoleRequest)
                    .thenApply(response -> new S3ProvisionResponse(role, response.credentials()));
        });
    }
    
    private List<Tag> roleTags(S3CopyResourceDefinition resourceDefinition) {
        var edcTag = Tag.builder()
                .key("created-by")
                .value("EDC")
                .build();
        var componentIdTag = Tag.builder()
                .key("edc:component-id")
                .value(componentId)
                .build();
        var tpTag = Tag.builder()
                .key("edc:transfer-process-id")
                .value(resourceDefinition.getTransferProcessId())
                .build();
        return List.of(edcTag, componentIdTag, tpTag);
    }
    
    static class Builder {
        private AwsClientProvider clientProvider;
        private Vault vault;
        private RetryPolicy<Object> retryPolicy;
        private TypeManager typeManager;
        private Monitor monitor;
        private String componentId;
        private int maxRoleSessionDuration;
        
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
        
        public Builder componentId(String componentId) {
            this.componentId = componentId;
            return this;
        }
        
        public Builder maxRoleSessionDuration(int maxRoleSessionDuration) {
            this.maxRoleSessionDuration = maxRoleSessionDuration;
            return this;
        }
        
        public S3CopyProvisionPipeline build() {
            Objects.requireNonNull(clientProvider);
            Objects.requireNonNull(vault);
            Objects.requireNonNull(retryPolicy);
            Objects.requireNonNull(typeManager);
            Objects.requireNonNull(monitor);
            Objects.requireNonNull(componentId);
            return new S3CopyProvisionPipeline(clientProvider, vault, retryPolicy, typeManager, monitor, componentId, maxRoleSessionDuration);
        }
    }
}
