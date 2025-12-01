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
import org.eclipse.edc.aws.s3.AwsTemporarySecretToken;
import org.eclipse.edc.aws.s3.S3ClientRequest;
import org.eclipse.edc.aws.s3.spi.S3BucketSchema;
import org.eclipse.edc.connector.dataplane.spi.provision.ProvisionResource;
import org.eclipse.edc.json.JacksonTypeManager;
import org.eclipse.edc.participantcontext.single.spi.SingleParticipantContextSupplier;
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
import software.amazon.awssdk.services.iam.model.CreateRoleRequest;
import software.amazon.awssdk.services.iam.model.CreateRoleResponse;
import software.amazon.awssdk.services.iam.model.GetUserResponse;
import software.amazon.awssdk.services.iam.model.PutRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.PutRolePolicyResponse;
import software.amazon.awssdk.services.iam.model.Role;
import software.amazon.awssdk.services.iam.model.User;
import software.amazon.awssdk.services.s3.S3AsyncClient;
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
import static org.eclipse.edc.dataplane.provision.aws.s3.copy.util.S3CopyPolicyUtils.STATEMENT_ATTRIBUTE;
import static org.eclipse.edc.dataplane.provision.aws.s3.copy.util.S3CopyPolicyUtils.bucketPolicyStatement;
import static org.eclipse.edc.dataplane.provision.aws.s3.copy.util.S3CopyPolicyUtils.crossAccountRolePolicy;
import static org.eclipse.edc.dataplane.provision.aws.s3.copy.util.S3CopyPolicyUtils.emptyBucketPolicy;
import static org.eclipse.edc.dataplane.provision.aws.s3.copy.util.S3CopyPolicyUtils.roleTrustPolicy;
import static org.eclipse.edc.junit.assertions.AbstractResultAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class S3CopyProvisionerTest {
    
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
    private final String userArn = "arn:aws:iam::123456789123:user/userName";
    private final String roleArn = "arn:aws:iam::123456789123:role/roleName";
    private final String roleName = "roleName";
    private final String roleAccessKeyId = "123";
    private final String roleSecretAccessKey = "456";
    private final String roleSessionToken = "789";
    private final SingleParticipantContextSupplier participantContextSupplier = Mockito.mock();

    private S3CopyProvisioner provisioner;

    @BeforeEach
    void setUp() {
        when(clientProvider.iamAsyncClient(any(S3ClientRequest.class))).thenReturn(iamClient);
        when(clientProvider.stsAsyncClient(any(S3ClientRequest.class))).thenReturn(stsClient);
        when(clientProvider.s3AsyncClient(any(S3ClientRequest.class))).thenReturn(s3Client);
        var participantContext = ParticipantContext.Builder.newInstance().participantContextId("participantContextId").identity("any").build();
        when(participantContextSupplier.get()).thenReturn(ServiceResult.success(participantContext));
        
        provisioner = new S3CopyProvisioner(clientProvider, vault,
                typeManager, mock(), "componentId", 3600, RetryPolicy.ofDefaults(), participantContextSupplier);
    }
    
    @Test
    void provision_shouldProvisionResources() throws Exception {
        var resource = provisionResource();
        
        var secretToken = new AwsSecretToken("accessKeyId", "secretAccessKey");
        when(vault.resolveSecret("participantContextId", ((DataAddress) resource.getProperty("newDestination")).getKeyName()))
                .thenReturn(typeManager.getMapper().writeValueAsString(secretToken));
        
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
        
        var provisionResponse = provisioner.provision(resource);

        assertThat(provisionResponse).succeedsWithin(1, SECONDS).satisfies(result -> {
            assertThat(result).isSucceeded().satisfies(provisioned -> {
                var destination = provisioned.getDataAddress();
                assertThat(destination.getKeyName()).isNotBlank();
                assertThat(provisioned.getProperty(S3BucketSchema.ROLE_NAME)).isEqualTo(roleName);
                verify(vault).storeSecret(any(), any(), argThat(json -> {
                    var deserialized = typeManager.readValue(json, AwsTemporarySecretToken.class);
                    assertThat(deserialized.accessKeyId()).isEqualTo(roleAccessKeyId);
                    assertThat(deserialized.secretAccessKey()).isEqualTo(roleSecretAccessKey);
                    assertThat(deserialized.sessionToken()).isEqualTo(roleSessionToken);
                    return true;
                }));
            });
        });
        
        // verify trust policy of created role
        var createRoleRequestCaptor = ArgumentCaptor.forClass(CreateRoleRequest.class);
        verify(iamClient).createRole(createRoleRequestCaptor.capture());
        var createRoleRequest = createRoleRequestCaptor.getValue();
        var expectedTrustPolicy = roleTrustPolicy(userArn);
        assertThat(toObjectNode(createRoleRequest.assumeRolePolicyDocument())).isEqualTo(toObjectNode(expectedTrustPolicy));
        
        // verify role policy of created role
        var putRolePolicyRequestCaptor = ArgumentCaptor.forClass(PutRolePolicyRequest.class);
        verify(iamClient).putRolePolicy(putRolePolicyRequestCaptor.capture());
        var putRolePolicyRequest = putRolePolicyRequestCaptor.getValue();
        var expectedRolePolicy = crossAccountRolePolicy(sourceBucket, sourceObject, destinationBucket, destinationObject);
        assertThat(toObjectNode(putRolePolicyRequest.policyDocument())).isEqualTo(toObjectNode(expectedRolePolicy));
        
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
    void provision_onError_shouldReturnFailedFuture() throws Exception {
        var resource = provisionResource();
        
        var secretToken = new AwsSecretToken("accessKeyId", "secretAccessKey");
        when(vault.resolveSecret("participantContextId", ((DataAddress) resource.getProperty("newDestination")).getKeyName()))
                .thenReturn(typeManager.getMapper().writeValueAsString(secretToken));
        
        when(iamClient.getUser()).thenReturn(failedFuture(new RuntimeException("error")));
        
        var response = provisioner.provision(resource);
        
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
    
    private ProvisionResource provisionResource() {
        return ProvisionResource.Builder.newInstance()
                .flowId("tp-id")
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
                .policy(emptyBucketPolicy().toString())
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
    
    private ObjectNode toObjectNode(String json) throws Exception {
        return (ObjectNode) typeManager.getMapper().readTree(json);
    }
    
    private ObjectNode toObjectNode(JsonObject jsonObject) throws Exception {
        return toObjectNode(jsonObject.toString());
    }
}
