/*
 *  Copyright (c) 2025 Cofinity-X
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

package org.eclipse.edc.dataplane.provision.aws.s3.copy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import jakarta.json.Json;
import org.eclipse.edc.aws.s3.AwsClientProvider;
import org.eclipse.edc.aws.s3.AwsTemporarySecretToken;
import org.eclipse.edc.aws.s3.S3ClientRequest;
import org.eclipse.edc.aws.s3.spi.S3BucketSchema;
import org.eclipse.edc.connector.dataplane.spi.provision.ProvisionResource;
import org.eclipse.edc.connector.dataplane.spi.provision.ProvisionedResource;
import org.eclipse.edc.connector.dataplane.spi.provision.Provisioner;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.response.ResponseStatus;
import org.eclipse.edc.spi.response.StatusResult;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import software.amazon.awssdk.services.iam.IamAsyncClient;
import software.amazon.awssdk.services.iam.model.CreateRoleRequest;
import software.amazon.awssdk.services.iam.model.CreateRoleResponse;
import software.amazon.awssdk.services.iam.model.GetUserResponse;
import software.amazon.awssdk.services.iam.model.PutRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.Role;
import software.amazon.awssdk.services.iam.model.Tag;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetBucketPolicyRequest;
import software.amazon.awssdk.services.s3.model.PutBucketPolicyRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sts.StsAsyncClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static java.lang.String.format;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.eclipse.edc.aws.s3.copy.lib.S3CopyUtils.getSecretTokenFromVault;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.BUCKET_NAME;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.ENDPOINT_OVERRIDE;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.OBJECT_NAME;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.REGION;
import static org.eclipse.edc.dataplane.provision.aws.s3.copy.AwsS3CopyProvisionExtension.S3_COPY_PROVISION_TYPE;
import static org.eclipse.edc.dataplane.provision.aws.s3.copy.util.S3CopyPolicyUtils.STATEMENT_ATTRIBUTE;
import static org.eclipse.edc.dataplane.provision.aws.s3.copy.util.S3CopyPolicyUtils.bucketPolicyStatement;
import static org.eclipse.edc.dataplane.provision.aws.s3.copy.util.S3CopyPolicyUtils.crossAccountRolePolicy;
import static org.eclipse.edc.dataplane.provision.aws.s3.copy.util.S3CopyPolicyUtils.emptyBucketPolicy;
import static org.eclipse.edc.dataplane.provision.aws.s3.copy.util.S3CopyPolicyUtils.roleTrustPolicy;
import static org.eclipse.edc.dataplane.provision.aws.s3.copy.util.S3CopyProvisionUtils.resourceIdentifier;

/**
 * Provisions and deprovisions AWS resources and policies to enable a cross-account copy of S3 objects.
 */
public class S3CopyProvisioner implements Provisioner {

    private static final String S3_ERROR_CODE_NO_SUCH_BUCKET_POLICY = "NoSuchBucketPolicy";
    private final AwsClientProvider clientProvider;
    private final Vault vault;
    private final RetryPolicy<Object> retryPolicy;
    private final TypeManager typeManager;
    private final Monitor monitor;
    private final String componentId;
    private final int maxRoleSessionDuration;
    
    public S3CopyProvisioner(AwsClientProvider clientProvider, Vault vault,
                             TypeManager typeManager,
                             Monitor monitor, String componentId,
                             int maxRoleSessionDuration, RetryPolicy<Object> retryPolicy) {
        this.clientProvider = clientProvider;
        this.vault = vault;
        this.typeManager = typeManager;
        this.monitor = monitor;
        this.componentId = componentId;
        this.maxRoleSessionDuration = maxRoleSessionDuration;
        this.retryPolicy = retryPolicy;
    }

    @Override
    public String supportedType() {
        return S3_COPY_PROVISION_TYPE;
    }

    @Override
    public CompletableFuture<StatusResult<ProvisionedResource>> provision(ProvisionResource provisionResource) {
        var source = provisionResource.getDataAddress();
        var destination = (DataAddress) provisionResource.getProperty("newDestination");

        // create IAM & STS client for source account -> create, configure & assume IAM role
        var sourceClientRequest = S3ClientRequest.from(source.getStringProperty(REGION), source.getStringProperty(ENDPOINT_OVERRIDE));
        var iamClient = clientProvider.iamAsyncClient(sourceClientRequest);
        var stsClient = clientProvider.stsAsyncClient(sourceClientRequest);

        // create S3 client for destination account -> update S3 bucket policy
        var secretTokenResult = getSecretTokenFromVault(destination.getKeyName(), vault, typeManager);
        if (secretTokenResult.failed()) {
            return failedFuture(new EdcException(secretTokenResult.getFailureDetail()));
        }
        var destinationClientRequest = S3ClientRequest.from(destination.getStringProperty(REGION), destination.getStringProperty(ENDPOINT_OVERRIDE), secretTokenResult.getContent());
        var s3Client = clientProvider.s3AsyncClient(destinationClientRequest);

        monitor.debug("getting IAM user");
        var resourceIdentifier = resourceIdentifier(provisionResource);
        return iamClient.getUser()
                .thenCompose(getUserResponse -> createRole(iamClient, getUserResponse, resourceIdentifier, provisionResource.getFlowId())
                    .thenCompose(role -> putRolePolicy(iamClient, source, destination, resourceIdentifier, role)
                        .thenCompose(ignore -> getBucketPolicy(s3Client, destination.getStringProperty(BUCKET_NAME)))
                        .thenCompose(bucketPolicy -> updateBucketPolicy(s3Client, destination, role, bucketPolicy, resourceIdentifier))
                        .thenCompose(ignore -> assumeRole(stsClient, resourceIdentifier, role))
                        .thenApply(credentials -> provisioningSucceeded(provisionResource, credentials, role))
                    )
                );
    }

    private CompletableFuture<Role> createRole(IamAsyncClient iamClient,
                                                             GetUserResponse getUserResponse, String roleName, String flowId) {
        var trustPolicy = roleTrustPolicy(getUserResponse.user().arn());

        var createRoleRequest = CreateRoleRequest.builder()
                .roleName(roleName)
                .description(format("Role for EDC transfer: %s", roleName))
                .assumeRolePolicyDocument(trustPolicy.toString())
                .maxSessionDuration(maxRoleSessionDuration)
                .tags(roleTags(flowId))
                .build();

        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            monitor.debug(format("creating IAM role '%s'", roleName));
            return iamClient.createRole(createRoleRequest).thenApply(CreateRoleResponse::role);
        });
    }

    private CompletableFuture<Role> putRolePolicy(IamAsyncClient iamClient, DataAddress source,
                                                                  DataAddress destination, String resourceIdentifier, Role role) {
        var sourceBucket = source.getStringProperty(BUCKET_NAME);
        var sourceObject = source.getStringProperty(OBJECT_NAME);
        var destinationBucket = destination.getStringProperty(BUCKET_NAME);
        var destinationObject = destination.getStringProperty(OBJECT_NAME);

        var rolePolicy = crossAccountRolePolicy(sourceBucket, sourceObject, destinationBucket, destinationObject);

        var putRolePolicyRequest = PutRolePolicyRequest.builder()
                .roleName(role.roleName())
                .policyName(resourceIdentifier)
                .policyDocument(rolePolicy.toString())
                .build();

        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            monitor.debug("putting IAM role policy");
            return iamClient.putRolePolicy(putRolePolicyRequest)
                    .thenApply(policyResponse -> role);
        });
    }

    private CompletableFuture<String> getBucketPolicy(S3AsyncClient s3Client, String destinationBucketName) {
        var getBucketPolicyRequest = GetBucketPolicyRequest.builder()
                .bucket(destinationBucketName)
                .build();

        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            monitor.debug("getting destination bucket policy");
            return s3Client.getBucketPolicy(getBucketPolicyRequest)
                    .handle((result, ex) -> {
                        if (ex == null) {
                            return result.policy();
                        } else {
                            if (ex instanceof CompletionException &&
                                    ex.getCause() instanceof S3Exception s3Exception &&
                                    s3Exception.awsErrorDetails().errorCode().equals(S3_ERROR_CODE_NO_SUCH_BUCKET_POLICY)) {
                                // accessing the bucket policy works, but no bucket policy is set
                                return emptyBucketPolicy().toString();
                            }

                            throw new CompletionException("Failed to get destination bucket policy", ex);
                        }
                    });
        });
    }

    private CompletableFuture<Role> updateBucketPolicy(S3AsyncClient s3Client,
                                                       DataAddress destination, Role role, String bucketPolicy, String resourceIdentifier) {
        var destinationBucket = destination.getStringProperty(BUCKET_NAME);

        var statement = bucketPolicyStatement(resourceIdentifier, role.arn(), destinationBucket);

        var typeReference = new TypeReference<HashMap<String, Object>>() {};
        var policyJson = Json.createObjectBuilder(typeManager.readValue(bucketPolicy, typeReference)).build();

        var statements = Json.createArrayBuilder(policyJson.getJsonArray(STATEMENT_ATTRIBUTE))
                .add(statement)
                .build();
        var updatedBucketPolicy = Json.createObjectBuilder(policyJson)
                .add(STATEMENT_ATTRIBUTE, statements)
                .build().toString();

        var putBucketPolicyRequest = PutBucketPolicyRequest.builder()
                .bucket(destinationBucket)
                .policy(updatedBucketPolicy)
                .build();

        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            monitor.debug("updating destination bucket policy");
            return s3Client.putBucketPolicy(putBucketPolicyRequest)
                    .thenApply(response -> role);
        });
    }

    private CompletableFuture<Credentials> assumeRole(StsAsyncClient stsClient,
                                                      String resourceIdentifier, Role role) {
        var assumeRoleRequest = AssumeRoleRequest.builder()
                .roleArn(role.arn())
                .roleSessionName(resourceIdentifier)
                .build();

        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            monitor.debug(format("assuming role '%s'", role.arn()));
            return stsClient.assumeRole(assumeRoleRequest)
                    .thenApply(AssumeRoleResponse::credentials);
        });
    }

    private List<Tag> roleTags(String flowId) {
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
                .value(flowId)
                .build();
        return List.of(edcTag, componentIdTag, tpTag);
    }

    private StatusResult<ProvisionedResource> provisioningSucceeded(ProvisionResource provisionResource,
                                                                  Credentials credentials, Role role) {
        var secretToken = new AwsTemporarySecretToken(credentials.accessKeyId(), credentials.secretAccessKey(),
                credentials.sessionToken(), credentials.expiration().toEpochMilli());

        var keyName = "resourceDefinition-" + provisionResource.getId() + "-secret-" + UUID.randomUUID();

        try {
            vault.storeSecret(keyName, typeManager.getMapper().writeValueAsString(secretToken));
        } catch (JsonProcessingException e) {
            return StatusResult.failure(ResponseStatus.FATAL_ERROR, "Cannot serialize secret token: " + e.getMessage());
        }

        var originalDataAddress = provisionResource.getDataAddress();

        var provisionedResource = ProvisionedResource.Builder.from(provisionResource)
                .dataAddress(DataAddress.Builder.newInstance()
                        .properties(originalDataAddress.getProperties())
                        .keyName(keyName)
                        .build())
                .property(S3BucketSchema.ROLE_NAME, role.roleName())
                .build();

        return StatusResult.success(provisionedResource);
    }

}
