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

package org.eclipse.edc.connector.provision.aws.s3.copy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import dev.failsafe.RetryPolicy;
import jakarta.json.Json;
import org.eclipse.edc.aws.s3.AwsClientProvider;
import org.eclipse.edc.aws.s3.AwsSecretToken;
import org.eclipse.edc.aws.s3.AwsTemporarySecretToken;
import org.eclipse.edc.aws.s3.S3ClientRequest;
import org.eclipse.edc.aws.s3.spi.S3BucketSchema;
import org.eclipse.edc.json.JacksonTypeManager;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.iam.IamAsyncClient;
import software.amazon.awssdk.services.iam.model.CreateRoleRequest;
import software.amazon.awssdk.services.iam.model.CreateRoleResponse;
import software.amazon.awssdk.services.iam.model.DeleteRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.DeleteRolePolicyResponse;
import software.amazon.awssdk.services.iam.model.DeleteRoleRequest;
import software.amazon.awssdk.services.iam.model.DeleteRoleResponse;
import software.amazon.awssdk.services.iam.model.GetUserResponse;
import software.amazon.awssdk.services.iam.model.PutRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.PutRolePolicyResponse;
import software.amazon.awssdk.services.iam.model.Role;
import software.amazon.awssdk.services.iam.model.User;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteBucketPolicyRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketPolicyResponse;
import software.amazon.awssdk.services.s3.model.GetBucketPolicyRequest;
import software.amazon.awssdk.services.s3.model.GetBucketPolicyResponse;
import software.amazon.awssdk.services.s3.model.PutBucketPolicyRequest;
import software.amazon.awssdk.services.s3.model.PutBucketPolicyResponse;
import software.amazon.awssdk.services.sts.StsAsyncClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.time.Instant;
import java.util.HashMap;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.edc.connector.provision.aws.s3.copy.util.S3CopyProvisionConstants.PLACEHOLDER_DESTINATION_BUCKET;
import static org.eclipse.edc.connector.provision.aws.s3.copy.util.S3CopyProvisionConstants.PLACEHOLDER_DESTINATION_OBJECT;
import static org.eclipse.edc.connector.provision.aws.s3.copy.util.S3CopyProvisionConstants.PLACEHOLDER_ROLE_ARN;
import static org.eclipse.edc.connector.provision.aws.s3.copy.util.S3CopyProvisionConstants.PLACEHOLDER_SOURCE_BUCKET;
import static org.eclipse.edc.connector.provision.aws.s3.copy.util.S3CopyProvisionConstants.PLACEHOLDER_SOURCE_OBJECT;
import static org.eclipse.edc.connector.provision.aws.s3.copy.util.S3CopyProvisionConstants.PLACEHOLDER_STATEMENT_SID;
import static org.eclipse.edc.connector.provision.aws.s3.copy.util.S3CopyProvisionConstants.PLACEHOLDER_USER_ARN;
import static org.eclipse.edc.connector.provision.aws.s3.copy.util.S3CopyProvisionConstants.S3_BUCKET_POLICY_STATEMENT;
import static org.eclipse.edc.connector.provision.aws.s3.copy.util.S3CopyProvisionTemplates.BUCKET_POLICY_STATEMENT_TEMPLATE;
import static org.eclipse.edc.connector.provision.aws.s3.copy.util.S3CopyProvisionTemplates.CROSS_ACCOUNT_ROLE_POLICY_TEMPLATE;
import static org.eclipse.edc.connector.provision.aws.s3.copy.util.S3CopyProvisionTemplates.CROSS_ACCOUNT_ROLE_TRUST_POLICY_TEMPLATE;
import static org.eclipse.edc.connector.provision.aws.s3.copy.util.S3CopyProvisionTemplates.EMPTY_BUCKET_POLICY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class S3CopyProvisionerTest {
    
    private S3CopyProvisioner provisioner;
    private TypeManager typeManager = new JacksonTypeManager();
    
    private AwsClientProvider clientProvider = mock(AwsClientProvider.class);
    private IamAsyncClient iamClient = mock(IamAsyncClient.class);
    private StsAsyncClient stsClient = mock(StsAsyncClient.class);
    private S3AsyncClient s3Client = mock(S3AsyncClient.class);
    private Vault vault = mock(Vault.class);
    
    private String sourceBucket = "source-bucket";
    private String sourceObject = "source-object";
    private String destinationBucket = "destination-bucket";
    private String destinationObject = "destination-object";
    private String policyStatementSid = "sid-123";
    private String userArn = "arn:aws:iam::123456789123:user/userName";
    private String roleArn = "arn:aws:iam::123456789123:role/roleName";
    private String roleName = "roleName";
    private String roleAccessKeyId = "123";
    private String roleSecretAccessKey = "456";
    private String roleSessionToken = "789";
    
    @BeforeEach
    void setUp() {
        when(clientProvider.iamAsyncClient(any(S3ClientRequest.class))).thenReturn(iamClient);
        when(clientProvider.stsAsyncClient(any(S3ClientRequest.class))).thenReturn(stsClient);
        when(clientProvider.s3AsyncClient(any(S3ClientRequest.class))).thenReturn(s3Client);
        
        provisioner = new S3CopyProvisioner(clientProvider, vault, RetryPolicy.ofDefaults(),
                typeManager, mock(Monitor.class), "componentId", 2, 3600);
    }
    
    @Test
    void provision_shouldProvisionResources() throws JsonProcessingException {
        var definition = resourceDefinition();
        
        var secretToken = new AwsSecretToken("accessKeyId", "secretAccessKey");
        when(vault.resolveSecret(definition.getDestinationKeyName())).thenReturn(typeManager.getMapper().writeValueAsString(secretToken));
        
        var getUserResponse = getUserResponse();
        when(iamClient.getUser()).thenReturn(completedFuture(getUserResponse));
        
        var createRoleResponse = createRoleResponse();
        when(iamClient.createRole(any(CreateRoleRequest.class))).thenReturn(completedFuture(createRoleResponse));
        
        var putRolePolicyResponse = PutRolePolicyResponse.builder().build();
        when(iamClient.putRolePolicy(any(PutRolePolicyRequest.class))).thenReturn(completedFuture(putRolePolicyResponse));
        
        var getBucketPolicyResponse = getBucketPolicyResponse();
        when(s3Client.getBucketPolicy(any(GetBucketPolicyRequest.class))).thenReturn(completedFuture(getBucketPolicyResponse));
        
        var putBucketPolicyResponse = PutBucketPolicyResponse.builder().build();
        when(s3Client.putBucketPolicy(any(PutBucketPolicyRequest.class))).thenReturn(completedFuture(putBucketPolicyResponse));
        
        var assumeRoleResponse = assumeRoleResponse();
        when(stsClient.assumeRole(any(AssumeRoleRequest.class))).thenReturn(completedFuture(assumeRoleResponse));
        
        var provisionResponse = provisioner.provision(definition, Policy.Builder.newInstance().build()).join().getContent();
        
        // verify correct properties on provisioned resource
        assertThat(provisionResponse.getResource()).isInstanceOfSatisfying(S3CopyProvisionedResource.class, resource -> {
            assertThat(resource.getDestinationRegion()).isEqualTo(definition.getDestinationRegion());
            assertThat(resource.getDestinationBucketName()).isEqualTo(definition.getDestinationBucketName());
            assertThat(resource.getDestinationKeyName()).isEqualTo(definition.getDestinationKeyName());
            assertThat(resource.getBucketPolicyStatementSid()).isEqualTo(definition.getBucketPolicyStatementSid());
            assertThat(resource.getSourceAccountRoleName()).isEqualTo(createRoleResponse().role().roleName());
        });
        
        // verify correct credentials returned
        assertThat(provisionResponse.getSecretToken()).isInstanceOfSatisfying(AwsTemporarySecretToken.class, token -> {
            assertThat(token.accessKeyId()).isEqualTo(roleAccessKeyId);
            assertThat(token.secretAccessKey()).isEqualTo(roleSecretAccessKey);
            assertThat(token.sessionToken()).isEqualTo(roleSessionToken);
        });
        
        // verify trust policy of created role
        var createRoleRequestCaptor = ArgumentCaptor.forClass(CreateRoleRequest.class);
        verify(iamClient).createRole(createRoleRequestCaptor.capture());
        var createRoleRequest = createRoleRequestCaptor.getValue();
        var expectedTrustPolicy = CROSS_ACCOUNT_ROLE_TRUST_POLICY_TEMPLATE
                .replace(PLACEHOLDER_USER_ARN, userArn);
        assertThat(createRoleRequest.assumeRolePolicyDocument()).isEqualTo(expectedTrustPolicy);
        
        // verify role policy of created role
        var putRolePolicyRequestCaptor = ArgumentCaptor.forClass(PutRolePolicyRequest.class);
        verify(iamClient).putRolePolicy(putRolePolicyRequestCaptor.capture());
        var putRolePolicyRequest = putRolePolicyRequestCaptor.getValue();
        var expectedRolePolicy = CROSS_ACCOUNT_ROLE_POLICY_TEMPLATE
                .replace(PLACEHOLDER_SOURCE_BUCKET, sourceBucket)
                .replace(PLACEHOLDER_SOURCE_OBJECT, sourceObject)
                .replace(PLACEHOLDER_DESTINATION_BUCKET, destinationBucket)
                .replace(PLACEHOLDER_DESTINATION_OBJECT, destinationObject);
        assertThat(putRolePolicyRequest.policyDocument()).isEqualTo(expectedRolePolicy);
        
        // verify bucket policy fetched for destination bucket
        verify(s3Client).getBucketPolicy(argThat((GetBucketPolicyRequest request) -> request.bucket().equals(destinationBucket)));
        
        // verify bucket policy updated with correct statement
        var putBucketPolicyRequestCaptor = ArgumentCaptor.forClass(PutBucketPolicyRequest.class);
        verify(s3Client).putBucketPolicy(putBucketPolicyRequestCaptor.capture());
        var putBucketPolicyRequest = putBucketPolicyRequestCaptor.getValue();
        assertThat(putBucketPolicyRequest.bucket()).isEqualTo(destinationBucket);
        verifyBucketPolicy(putBucketPolicyRequest.policy(), policyStatementSid, roleArn);
        
        // verify correct role assumed
        verify(stsClient).assumeRole(argThat((AssumeRoleRequest request) -> request.roleArn().equals(roleArn)));
    }
    
    @Test
    void provision_onError_shouldReturnFailedFuture() throws JsonProcessingException {
        var definition = resourceDefinition();
        
        var secretToken = new AwsSecretToken("accessKeyId", "secretAccessKey");
        when(vault.resolveSecret(definition.getDestinationKeyName())).thenReturn(typeManager.getMapper().writeValueAsString(secretToken));
        
        when(iamClient.getUser()).thenReturn(failedFuture(new RuntimeException("error")));
        
        var response = provisioner.provision(definition, Policy.Builder.newInstance().build());
        
        assertThat(response).failsWithin(1, SECONDS);
    }
    
    @Test
    void deprovision_shouldDeprovisionResources() throws JsonProcessingException {
        var resource = provisionedResource();
        
        var secretToken = new AwsSecretToken("accessKeyId", "secretAccessKey");
        when(vault.resolveSecret(resource.getDestinationKeyName())).thenReturn(typeManager.getMapper().writeValueAsString(secretToken));
        
        var getBucketPolicyResponse = getNoneEmptyBucketPolicyResponse();
        when(s3Client.getBucketPolicy(any(GetBucketPolicyRequest.class))).thenReturn(completedFuture(getBucketPolicyResponse));
        
        var deleteBucketPolicyResponse = DeleteBucketPolicyResponse.builder().build();
        when(s3Client.deleteBucketPolicy(any(DeleteBucketPolicyRequest.class))).thenReturn(completedFuture(deleteBucketPolicyResponse));
        
        var deleteRolePolicyResponse = DeleteRolePolicyResponse.builder().build();
        when(iamClient.deleteRolePolicy(any(DeleteRolePolicyRequest.class))).thenReturn(completedFuture(deleteRolePolicyResponse));
        
        var deleteRoleResponse = DeleteRoleResponse.builder().build();
        when(iamClient.deleteRole(any(DeleteRoleRequest.class))).thenReturn(completedFuture(deleteRoleResponse));
        
        var deprovisionedResource = provisioner.deprovision(resource, Policy.Builder.newInstance().build()).join().getContent();
        
        assertThat(deprovisionedResource.getProvisionedResourceId()).isEqualTo(resource.getId());
        
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
    void deprovision_otherBucketPolicyStatements_shouldOnlyRemoveProvisionedStatement() throws JsonProcessingException {
        var resource = provisionedResource();
        
        var secretToken = new AwsSecretToken("accessKeyId", "secretAccessKey");
        when(vault.resolveSecret(resource.getDestinationKeyName())).thenReturn(typeManager.getMapper().writeValueAsString(secretToken));
        
        var getBucketPolicyResponse = getBucketPolicyResponseWithMultipleStatements();
        when(s3Client.getBucketPolicy(any(GetBucketPolicyRequest.class))).thenReturn(completedFuture(getBucketPolicyResponse));
        
        var putBucketPolicyResponse = PutBucketPolicyResponse.builder().build();
        when(s3Client.putBucketPolicy(any(PutBucketPolicyRequest.class))).thenReturn(completedFuture(putBucketPolicyResponse));
        
        var deleteRolePolicyResponse = DeleteRolePolicyResponse.builder().build();
        when(iamClient.deleteRolePolicy(any(DeleteRolePolicyRequest.class))).thenReturn(completedFuture(deleteRolePolicyResponse));
        
        var deleteRoleResponse = DeleteRoleResponse.builder().build();
        when(iamClient.deleteRole(any(DeleteRoleRequest.class))).thenReturn(completedFuture(deleteRoleResponse));
        
        var deprovisionedResource = provisioner.deprovision(resource, Policy.Builder.newInstance().build()).join().getContent();
        
        assertThat(deprovisionedResource.getProvisionedResourceId()).isEqualTo(resource.getId());
        
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
    void deprovision_onError_shouldReturnFailedFuture() throws JsonProcessingException {
        var resource = provisionedResource();
        
        var secretToken = new AwsSecretToken("accessKeyId", "secretAccessKey");
        when(vault.resolveSecret(resource.getDestinationKeyName())).thenReturn(typeManager.getMapper().writeValueAsString(secretToken));
        
        when(s3Client.getBucketPolicy(any(GetBucketPolicyRequest.class))).thenReturn(failedFuture(new RuntimeException("error")));
        
        var response = provisioner.deprovision(resource, Policy.Builder.newInstance().build());
        
        assertThat(response).failsWithin(1, SECONDS);
    }
    
    private void verifyBucketPolicy(String policy, String statementSid, String roleArn) throws JsonProcessingException {
        var typeReference = new TypeReference<HashMap<String, Object>>() {};
        var policyJson = Json.createObjectBuilder(typeManager.readValue(policy, typeReference)).build();
        var statements = policyJson.getJsonArray(S3_BUCKET_POLICY_STATEMENT);
        
        assertThat(statements).hasSize(1);
        
        var expectedStatementJson = BUCKET_POLICY_STATEMENT_TEMPLATE
                .replace(PLACEHOLDER_STATEMENT_SID, statementSid)
                .replace(PLACEHOLDER_ROLE_ARN, roleArn)
                .replace(PLACEHOLDER_DESTINATION_BUCKET, destinationBucket);
        var expectedStatement = typeManager.getMapper().readTree(expectedStatementJson);
        var statement = typeManager.getMapper().readTree(statements.get(0).toString());
        
        assertThat(statement).isEqualTo(expectedStatement);
    }
    
    private S3CopyResourceDefinition resourceDefinition() {
        return S3CopyResourceDefinition.Builder.newInstance()
                .id("test")
                .transferProcessId("tp-id")
                .destinationRegion("eu-central-1")
                .destinationBucketName(destinationBucket)
                .destinationObjectName(destinationObject)
                .destinationKeyName("destination-key-name")
                .bucketPolicyStatementSid(policyStatementSid)
                .sourceDataAddress(DataAddress.Builder.newInstance()
                        .type(S3BucketSchema.TYPE)
                        .property(S3BucketSchema.REGION, "eu-central-1")
                        .property(S3BucketSchema.BUCKET_NAME, sourceBucket)
                        .property(S3BucketSchema.OBJECT_NAME, sourceObject)
                        .build())
                .build();
    }
    
    private S3CopyProvisionedResource provisionedResource() {
        return S3CopyProvisionedResource.Builder.newInstance()
                .id("test")
                .resourceDefinitionId("test")
                .transferProcessId("tp-id")
                .resourceName("test")
                .destinationRegion("eu-central-1")
                .destinationBucketName(destinationBucket)
                .destinationKeyName("destination-key-name")
                .bucketPolicyStatementSid(policyStatementSid)
                .sourceAccountRoleName(roleName)
                .build();
    }
    
    private GetUserResponse getUserResponse() {
        return GetUserResponse.builder()
                .user(User.builder()
                        .arn(userArn)
                        .build())
                .build();
    }
    
    private CreateRoleResponse createRoleResponse() {
        return CreateRoleResponse.builder()
                .role(Role.builder()
                        .roleId("roleId")
                        .roleName(roleName)
                        .arn(roleArn)
                        .build())
                .build();
    }
    
    private GetBucketPolicyResponse getBucketPolicyResponse() {
        return GetBucketPolicyResponse.builder()
                .policy(EMPTY_BUCKET_POLICY)
                .build();
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
    
    private AssumeRoleResponse assumeRoleResponse() {
        return AssumeRoleResponse.builder()
                .credentials(Credentials.builder()
                        .accessKeyId(roleAccessKeyId)
                        .secretAccessKey(roleSecretAccessKey)
                        .sessionToken(roleSessionToken)
                        .expiration(Instant.now().plusSeconds(86400))
                        .build())
                .build();
    }
}
