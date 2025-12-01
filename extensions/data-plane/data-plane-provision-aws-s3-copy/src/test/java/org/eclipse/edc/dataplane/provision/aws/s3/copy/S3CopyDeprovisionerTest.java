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
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.failsafe.RetryPolicy;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import org.eclipse.edc.aws.s3.AwsClientProvider;
import org.eclipse.edc.aws.s3.AwsSecretToken;
import org.eclipse.edc.aws.s3.S3ClientRequest;
import org.eclipse.edc.aws.s3.spi.S3BucketSchema;
import org.eclipse.edc.connector.dataplane.spi.provision.ProvisionResource;
import org.eclipse.edc.connector.dataplane.spi.provision.ProvisionedResource;
import org.eclipse.edc.json.JacksonTypeManager;
import org.eclipse.edc.participantcontext.spi.service.ParticipantContextSupplier;
import org.eclipse.edc.participantcontext.spi.types.ParticipantContext;
import org.eclipse.edc.spi.result.ServiceResult;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.services.iam.IamAsyncClient;
import software.amazon.awssdk.services.iam.model.DeleteRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.DeleteRolePolicyResponse;
import software.amazon.awssdk.services.iam.model.DeleteRoleRequest;
import software.amazon.awssdk.services.iam.model.DeleteRoleResponse;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteBucketPolicyRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketPolicyResponse;
import software.amazon.awssdk.services.s3.model.GetBucketPolicyRequest;
import software.amazon.awssdk.services.s3.model.GetBucketPolicyResponse;
import software.amazon.awssdk.services.s3.model.PutBucketPolicyRequest;
import software.amazon.awssdk.services.s3.model.PutBucketPolicyResponse;
import software.amazon.awssdk.services.sts.StsAsyncClient;

import java.util.HashMap;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.edc.dataplane.provision.aws.s3.copy.util.S3CopyPolicyUtils.STATEMENT_ATTRIBUTE;
import static org.eclipse.edc.dataplane.provision.aws.s3.copy.util.S3CopyPolicyUtils.bucketPolicyStatement;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class S3CopyDeprovisionerTest {
    
    private final TypeManager typeManager = new JacksonTypeManager();

    private final AwsClientProvider clientProvider = mock();
    private final IamAsyncClient iamClient = mock();
    private final StsAsyncClient stsClient = mock();
    private final S3AsyncClient s3Client = mock();
    private final Vault vault = mock();

    private final String sourceBucket = "source-bucket";
    private final String sourceObject = "source-object";
    private final String destinationBucket = "destination-bucket";
    private final String destinationObject = "destination-object";
    private final String policyStatementSid = "edc-transfer_tp-id";
    private final String roleName = "roleName";

    private final ParticipantContextSupplier participantContextSupplier = Mockito.mock();
    private final S3CopyDeprovisioner deprovisioner = new S3CopyDeprovisioner(clientProvider, vault, typeManager, mock(),
            RetryPolicy.ofDefaults(), participantContextSupplier);

    @BeforeEach
    void setUp() {
        when(clientProvider.iamAsyncClient(any(S3ClientRequest.class))).thenReturn(iamClient);
        when(clientProvider.stsAsyncClient(any(S3ClientRequest.class))).thenReturn(stsClient);
        when(clientProvider.s3AsyncClient(any(S3ClientRequest.class))).thenReturn(s3Client);
        var participantContext = ParticipantContext.Builder.newInstance().participantContextId("participantContextId").identity("any").build();
        when(participantContextSupplier.get()).thenReturn(ServiceResult.success(participantContext));
    }

    @Test
    void shouldDeprovisionResources() throws Exception {
        var resource = provisionedResource();
        
        var secretToken = new AwsSecretToken("accessKeyId", "secretAccessKey");
        when(vault.resolveSecret("participantContextId", ((DataAddress) resource.getProperty("newDestination")).getKeyName()))
                .thenReturn(typeManager.getMapper().writeValueAsString(secretToken));
        
        var getBucketPolicyResponse = getNoneEmptyBucketPolicyResponse();
        when(s3Client.getBucketPolicy(any(GetBucketPolicyRequest.class))).thenReturn(completedFuture(getBucketPolicyResponse));
        
        var deleteBucketPolicyResponse = DeleteBucketPolicyResponse.builder().build();
        when(s3Client.deleteBucketPolicy(any(DeleteBucketPolicyRequest.class))).thenReturn(completedFuture(deleteBucketPolicyResponse));
        
        var deleteRolePolicyResponse = DeleteRolePolicyResponse.builder().build();
        when(iamClient.deleteRolePolicy(any(DeleteRolePolicyRequest.class))).thenReturn(completedFuture(deleteRolePolicyResponse));
        
        var deleteRoleResponse = DeleteRoleResponse.builder().build();
        when(iamClient.deleteRole(any(DeleteRoleRequest.class))).thenReturn(completedFuture(deleteRoleResponse));

        var future = deprovisioner.deprovision(resource);

        assertThat(future).succeedsWithin(1, SECONDS);
        
        // verify bucket policy fetched for destination bucket
        verify(s3Client).getBucketPolicy(argThat((GetBucketPolicyRequest request) -> request.bucket().equals(destinationBucket)));
        
        // verify bucket policy deleted for destination bucket
        verify(s3Client).deleteBucketPolicy(argThat((DeleteBucketPolicyRequest request) -> request.bucket().equals(destinationBucket)));
        
        // verify role policy deleted for provisioned role
        verify(iamClient).deleteRolePolicy((argThat((DeleteRolePolicyRequest request) -> request.roleName().equals(roleName))));
        
        // verify provisioned role deleted
        verify(iamClient).deleteRole(argThat((DeleteRoleRequest request) -> request.roleName().equals(roleName)));
    }
    
    @Test
    void otherBucketPolicyStatements_shouldOnlyRemoveProvisionedStatement() throws Exception {
        var resource = provisionedResource();
        
        var secretToken = new AwsSecretToken("accessKeyId", "secretAccessKey");
        when(vault.resolveSecret("participantContextId", ((DataAddress) resource.getProperty("newDestination")).getKeyName()))
                .thenReturn(typeManager.getMapper().writeValueAsString(secretToken));
        
        var getBucketPolicyResponse = getBucketPolicyResponseWithMultipleStatements();
        when(s3Client.getBucketPolicy(any(GetBucketPolicyRequest.class))).thenReturn(completedFuture(getBucketPolicyResponse));
        
        var putBucketPolicyResponse = PutBucketPolicyResponse.builder().build();
        when(s3Client.putBucketPolicy(any(PutBucketPolicyRequest.class))).thenReturn(completedFuture(putBucketPolicyResponse));
        
        var deleteRolePolicyResponse = DeleteRolePolicyResponse.builder().build();
        when(iamClient.deleteRolePolicy(any(DeleteRolePolicyRequest.class))).thenReturn(completedFuture(deleteRolePolicyResponse));
        
        var deleteRoleResponse = DeleteRoleResponse.builder().build();
        when(iamClient.deleteRole(any(DeleteRoleRequest.class))).thenReturn(completedFuture(deleteRoleResponse));

        var future = deprovisioner.deprovision(resource);

        assertThat(future).succeedsWithin(1, SECONDS);

        // verify bucket policy fetched for destination bucket
        verify(s3Client).getBucketPolicy(argThat((GetBucketPolicyRequest request) -> request.bucket().equals(destinationBucket)));
        
        // verify only provisioned statement deleted from bucket policy
        var putBucketPolicyRequestCaptor = ArgumentCaptor.forClass(PutBucketPolicyRequest.class);
        verify(s3Client).putBucketPolicy(putBucketPolicyRequestCaptor.capture());
        var putBucketPolicyRequest = putBucketPolicyRequestCaptor.getValue();
        assertThat(putBucketPolicyRequest.bucket()).isEqualTo(destinationBucket);
        verifyBucketPolicy(putBucketPolicyRequest.policy(), "someOtherSid", "someRoleArn");
        
        // verify role policy deleted for provisioned role
        verify(iamClient).deleteRolePolicy((argThat((DeleteRolePolicyRequest request) -> request.roleName().equals(roleName))));
        
        // verify provisioned role deleted
        verify(iamClient).deleteRole(argThat((DeleteRoleRequest request) -> request.roleName().equals(roleName)));
    }
    
    @Test
    void onError_shouldReturnFailedFuture() throws Exception {
        var resource = provisionedResource();
        
        var secretToken = new AwsSecretToken("accessKeyId", "secretAccessKey");
        when(vault.resolveSecret(((DataAddress) resource.getProperty("newDestination")).getKeyName())).thenReturn(typeManager.getMapper().writeValueAsString(secretToken));
        
        when(s3Client.getBucketPolicy(any(GetBucketPolicyRequest.class))).thenReturn(failedFuture(new RuntimeException("error")));
        
        var response = deprovisioner.deprovision(resource);
        
        assertThat(response).failsWithin(1, SECONDS);
    }
    
    private void verifyBucketPolicy(String policy, String statementSid, String roleArn) throws Exception {
        var typeReference = new TypeReference<HashMap<String, Object>>() {};
        var policyJson = Json.createObjectBuilder(typeManager.readValue(policy, typeReference)).build();
        var statements = policyJson.getJsonArray(STATEMENT_ATTRIBUTE);
        
        assertThat(statements).hasSize(1);
        
        var expectedStatement = bucketPolicyStatement(statementSid, roleArn, destinationBucket);
        
        assertThat(toObjectNode(statements.get(0).toString())).isEqualTo(toObjectNode(expectedStatement));
    }

    private ProvisionResource provisionedResource() {
        var provisionResource = ProvisionResource.Builder.newInstance()
                .flowId("tp-id")
                .property(S3BucketSchema.ROLE_NAME, roleName)
                .dataAddress(DataAddress.Builder.newInstance()
                        .type(S3BucketSchema.TYPE)
                        .property(S3BucketSchema.REGION, "eu-central-1")
                        .property(S3BucketSchema.BUCKET_NAME, sourceBucket)
                        .property(S3BucketSchema.OBJECT_NAME, sourceObject)
                        .build())
                .property("newDestination", DataAddress.Builder.newInstance()
                        .type(S3BucketSchema.TYPE)
                        .keyName("destination-key-name")
                        .property(S3BucketSchema.BUCKET_NAME, destinationBucket)
                        .property(S3BucketSchema.OBJECT_NAME, destinationObject)
                        .build())
                .build();

        var provisionedResource = ProvisionedResource.Builder.from(provisionResource)
                .property(S3BucketSchema.ROLE_NAME, "roleName").build();
        provisionResource.transitionProvisioned(provisionedResource);
        return provisionResource;
    }

    private GetBucketPolicyResponse getNoneEmptyBucketPolicyResponse() {
        var noneEmptyPolicy = "{\n" +
                "    \"Version\": \"2012-10-17\",\n" +
                "    \"Statement\": [\n" +
                "        {\n" +
                "            \"Sid\": \"" + policyStatementSid + "\",\n" +
                "            \"Effect\": \"Allow\",\n" +
                "            \"Principal\": {\n" +
                "                \"AWS\": \"someRoleArn\"\n" +
                "            },\n" +
                "            \"Action\": [\n" +
                "                \"s3:ListBucket\",\n" +
                "                \"s3:PutObject\",\n" +
                "                \"s3:PutObjectAcl\",\n" +
                "                \"s3:PutObjectTagging\",\n" +
                "                \"s3:GetObjectTagging\",\n" +
                "                \"s3:GetObjectVersion\",\n" +
                "                \"s3:GetObjectVersionTagging\"\n" +
                "            ],\n" +
                "            \"Resource\": [\n" +
                "                \"arn:aws:s3:::" + destinationBucket + "\",\n" +
                "                \"arn:aws:s3:::" + destinationBucket + "/*\"\n" +
                "            ]\n" +
                "        }" +
                "    ]\n" +
                "}";
        
        return GetBucketPolicyResponse.builder()
                .policy(noneEmptyPolicy)
                .build();
    }
    
    private GetBucketPolicyResponse getBucketPolicyResponseWithMultipleStatements() {
        var noneEmptyPolicy = "{\n" +
                "    \"Version\": \"2012-10-17\",\n" +
                "    \"Statement\": [\n" +
                "        {\n" +
                "            \"Sid\": \"" + policyStatementSid + "\",\n" +
                "            \"Effect\": \"Allow\",\n" +
                "            \"Principal\": {\n" +
                "                \"AWS\": \"someRoleArn\"\n" +
                "            },\n" +
                "            \"Action\": [\n" +
                "                \"s3:ListBucket\",\n" +
                "                \"s3:PutObject\",\n" +
                "                \"s3:PutObjectAcl\",\n" +
                "                \"s3:PutObjectTagging\",\n" +
                "                \"s3:GetObjectTagging\",\n" +
                "                \"s3:GetObjectVersion\",\n" +
                "                \"s3:GetObjectVersionTagging\"\n" +
                "            ],\n" +
                "            \"Resource\": [\n" +
                "                \"arn:aws:s3:::" + destinationBucket + "\",\n" +
                "                \"arn:aws:s3:::" + destinationBucket + "/*\"\n" +
                "            ]\n" +
                "        }," +
                "        {\n" +
                "            \"Sid\": \"someOtherSid\",\n" +
                "            \"Effect\": \"Allow\",\n" +
                "            \"Principal\": {\n" +
                "                \"AWS\": \"someRoleArn\"\n" +
                "            },\n" +
                "            \"Action\": [\n" +
                "                \"s3:ListBucket\",\n" +
                "                \"s3:PutObject\",\n" +
                "                \"s3:PutObjectAcl\",\n" +
                "                \"s3:PutObjectTagging\",\n" +
                "                \"s3:GetObjectTagging\",\n" +
                "                \"s3:GetObjectVersion\",\n" +
                "                \"s3:GetObjectVersionTagging\"\n" +
                "            ],\n" +
                "            \"Resource\": [\n" +
                "                \"arn:aws:s3:::" + destinationBucket + "\",\n" +
                "                \"arn:aws:s3:::" + destinationBucket + "/*\"\n" +
                "            ]\n" +
                "        }" +
                "    ]\n" +
                "}";
        
        return GetBucketPolicyResponse.builder()
                .policy(noneEmptyPolicy)
                .build();
    }

    private ObjectNode toObjectNode(String json) throws Exception {
        return (ObjectNode) typeManager.getMapper().readTree(json);
    }
    
    private ObjectNode toObjectNode(JsonObject jsonObject) throws Exception {
        return toObjectNode(jsonObject.toString());
    }
}
