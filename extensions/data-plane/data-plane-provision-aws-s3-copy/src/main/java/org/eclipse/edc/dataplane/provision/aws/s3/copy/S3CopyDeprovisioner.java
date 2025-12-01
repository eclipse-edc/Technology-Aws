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

import com.fasterxml.jackson.core.type.TypeReference;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import org.eclipse.edc.aws.s3.AwsClientProvider;
import org.eclipse.edc.aws.s3.S3ClientRequest;
import org.eclipse.edc.connector.dataplane.spi.provision.DeprovisionedResource;
import org.eclipse.edc.connector.dataplane.spi.provision.Deprovisioner;
import org.eclipse.edc.connector.dataplane.spi.provision.ProvisionResource;
import org.eclipse.edc.participantcontext.spi.service.ParticipantContextSupplier;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.response.StatusResult;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import software.amazon.awssdk.regions.Region;
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

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.eclipse.edc.aws.s3.copy.lib.S3CopyUtils.getSecretTokenFromVault;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.BUCKET_NAME;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.ENDPOINT_OVERRIDE;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.REGION;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.ROLE_NAME;
import static org.eclipse.edc.dataplane.provision.aws.s3.copy.AwsS3CopyProvisionExtension.S3_COPY_PROVISION_TYPE;
import static org.eclipse.edc.dataplane.provision.aws.s3.copy.util.S3CopyPolicyUtils.BUCKET_POLICY_STATEMENT_SID_ATTRIBUTE;
import static org.eclipse.edc.dataplane.provision.aws.s3.copy.util.S3CopyPolicyUtils.STATEMENT_ATTRIBUTE;
import static org.eclipse.edc.dataplane.provision.aws.s3.copy.util.S3CopyProvisionUtils.resourceIdentifier;

/**
 * Provisions and deprovisions AWS resources and policies to enable a cross-account copy of S3 objects.
 */
public class S3CopyDeprovisioner implements Deprovisioner {

    private final AwsClientProvider clientProvider;
    private final Vault vault;
    private final RetryPolicy<Object> retryPolicy;
    private final ParticipantContextSupplier participantContextSupplier;
    private final TypeManager typeManager;
    private final Monitor monitor;

    public S3CopyDeprovisioner(AwsClientProvider clientProvider, Vault vault,
                               TypeManager typeManager,
                               Monitor monitor, RetryPolicy<Object> retryPolicy,
                               ParticipantContextSupplier participantContextSupplier) {
        this.clientProvider = clientProvider;
        this.vault = vault;
        this.typeManager = typeManager;
        this.monitor = monitor;
        this.retryPolicy = retryPolicy;
        this.participantContextSupplier = participantContextSupplier;
    }

    @Override
    public String supportedType() {
        return S3_COPY_PROVISION_TYPE;
    }

    @Override
    public CompletableFuture<StatusResult<DeprovisionedResource>> deprovision(ProvisionResource provisionResource) {
        var source = provisionResource.getDataAddress();
        var destination = (DataAddress) provisionResource.getProperty("newDestination");

        // create S3 client for destination account -> update S3 bucket policy
        var secretTokenResult = getSecretTokenFromVault(participantContextSupplier, destination.getKeyName(), vault, typeManager);
        if (secretTokenResult.failed()) {
            return failedFuture(new EdcException(secretTokenResult.getFailureDetail()));
        }
        var s3ClientRequest = S3ClientRequest.from(destination.getStringProperty(REGION), destination.getStringProperty(ENDPOINT_OVERRIDE), secretTokenResult.getContent());
        var s3Client = clientProvider.s3AsyncClient(s3ClientRequest);

        // create IAM client for source account -> delete IAM role
        var iamClient = clientProvider.iamAsyncClient(S3ClientRequest.from(Region.AWS_GLOBAL.id(), source.getStringProperty(ENDPOINT_OVERRIDE)));

        var roleName = (String) provisionResource.getProvisionedResource().getProperty(ROLE_NAME);

        var getBucketPolicyRequest = GetBucketPolicyRequest.builder()
                .bucket(destination.getStringProperty(BUCKET_NAME))
                .build();

        var resourceIdentifier = resourceIdentifier(provisionResource);
        monitor.debug("getting destination bucket policy");
        return s3Client.getBucketPolicy(getBucketPolicyRequest)
                .thenCompose(response -> updateBucketPolicy(s3Client, destination, response, resourceIdentifier))
                .thenCompose(response -> deleteRolePolicy(iamClient, roleName))
                .thenCompose(response -> deleteRole(iamClient, roleName))
                .thenApply(response -> DeprovisionedResource.Builder.from(provisionResource).build())
                .thenApply(StatusResult::success);
    }

    private CompletableFuture<? extends S3Response> updateBucketPolicy(S3AsyncClient s3Client, DataAddress destination,
                                                                       GetBucketPolicyResponse bucketPolicyResponse, String resourceIdentifier) {
        var bucketPolicy = bucketPolicyResponse.policy();

        var typeReference = new TypeReference<HashMap<String, Object>>() {};
        var policyJson = Json.createObjectBuilder(typeManager.readValue(bucketPolicy, typeReference)).build();

        var statementsBuilder = Json.createArrayBuilder();

        policyJson.getJsonArray(STATEMENT_ATTRIBUTE).forEach(entry -> {
            var statement = (JsonObject) entry;
            var sid = statement.getJsonString(BUCKET_POLICY_STATEMENT_SID_ATTRIBUTE);

            // add all previously existing statements to bucket policy, omit only statement with Sid specified in provisioned resource
            if (sid == null || !resourceIdentifier.equals(sid.getString())) {
                statementsBuilder.add(statement);
            }
        });

        var updatedBucketPolicy = Json.createObjectBuilder(policyJson)
                .add(STATEMENT_ATTRIBUTE, statementsBuilder)
                .build();

        // since putting a bucket policy with empty statement array fails using the SDK, the bucket
        // policy is deleted if no statements are left
        if (updatedBucketPolicy.getJsonArray(STATEMENT_ATTRIBUTE).isEmpty()) {
            var deleteBucketPolicyRequest = DeleteBucketPolicyRequest.builder()
                    .bucket(destination.getStringProperty(BUCKET_NAME))
                    .build();

            return Failsafe.with(retryPolicy).getStageAsync(() -> {
                monitor.debug("deleting destination bucket policy");
                return s3Client.deleteBucketPolicy(deleteBucketPolicyRequest);
            });
        } else {
            var putBucketPolicyRequest = PutBucketPolicyRequest.builder()
                    .bucket(destination.getStringProperty(BUCKET_NAME))
                    .policy(updatedBucketPolicy.toString())
                    .build();

            return Failsafe.with(retryPolicy).getStageAsync(() -> {
                monitor.debug("updating destination bucket policy");
                return s3Client.putBucketPolicy(putBucketPolicyRequest);
            });
        }
    }

    private CompletableFuture<DeleteRolePolicyResponse> deleteRolePolicy(IamAsyncClient iamClient, String roleName) {
        var deleteRolePolicyRequest = DeleteRolePolicyRequest.builder()
                .roleName(roleName)
                .policyName(roleName)
                .build();

        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            monitor.debug("deleting IAM role policy");
            return iamClient.deleteRolePolicy(deleteRolePolicyRequest);
        });
    }

    private CompletableFuture<DeleteRoleResponse> deleteRole(IamAsyncClient iamClient, String roleName) {
        var deleteRoleRequest = DeleteRoleRequest.builder()
                .roleName(roleName)
                .build();

        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            monitor.debug("deleting IAM role");
            return iamClient.deleteRole(deleteRoleRequest);
        });
    }
}
