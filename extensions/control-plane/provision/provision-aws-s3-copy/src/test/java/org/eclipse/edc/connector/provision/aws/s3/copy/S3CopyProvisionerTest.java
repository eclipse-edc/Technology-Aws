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
import static org.assertj.core.api.Assertions.assertThat;
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
    private String destinationBucket = "destination-bucket";
    private String policyStatementSid = "sid-123";
    private String userArn = "arn:aws:iam::123456789123:user/userName";
    private String roleArn = "arn:aws:iam::123456789123:role/roleName";
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
            assertThat(resource.getSourceAccountRole()).isEqualTo(createRoleResponse().role());
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
                .replace(PLACEHOLDER_DESTINATION_BUCKET, destinationBucket);
        assertThat(putRolePolicyRequest.policyDocument()).isEqualTo(expectedRolePolicy);
        
        // verify bucket policy fetched for destination bucket
        verify(s3Client).getBucketPolicy(argThat((GetBucketPolicyRequest request) -> request.bucket().equals(destinationBucket)));
        
        // verify bucket policy updated with correct statement
        var putBucketPolicyRequestCaptor = ArgumentCaptor.forClass(PutBucketPolicyRequest.class);
        verify(s3Client).putBucketPolicy(putBucketPolicyRequestCaptor.capture());
        var putBucketPolicyRequest = putBucketPolicyRequestCaptor.getValue();
        assertThat(putBucketPolicyRequest.bucket()).isEqualTo(destinationBucket);
        verifyBucketPolicy(putBucketPolicyRequest.policy());
        
        // verify correct role assumed
        verify(stsClient).assumeRole(argThat((AssumeRoleRequest request) -> request.roleArn().equals(roleArn)));
    }
    
    private void verifyBucketPolicy(String policy) throws JsonProcessingException {
        var typeReference = new TypeReference<HashMap<String, Object>>() {};
        var policyJson = Json.createObjectBuilder(typeManager.readValue(policy, typeReference)).build();
        var statements = policyJson.getJsonArray(S3_BUCKET_POLICY_STATEMENT);
        
        assertThat(statements).hasSize(1);
        
        var expectedStatementJson = BUCKET_POLICY_STATEMENT_TEMPLATE
                .replace(PLACEHOLDER_STATEMENT_SID, policyStatementSid)
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
                .destinationKeyName("destination-key-name")
                .bucketPolicyStatementSid(policyStatementSid)
                .sourceDataAddress(DataAddress.Builder.newInstance()
                        .type(S3BucketSchema.TYPE)
                        .property(S3BucketSchema.REGION, "eu-central-1")
                        .property(S3BucketSchema.BUCKET_NAME, sourceBucket)
                        .property(S3BucketSchema.OBJECT_NAME, "source-object")
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
                        .roleName("roleName")
                        .arn(roleArn)
                        .build())
                .build();
    }
    
    private GetBucketPolicyResponse getBucketPolicyResponse() {
        return GetBucketPolicyResponse.builder()
                .policy(EMPTY_BUCKET_POLICY)
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
