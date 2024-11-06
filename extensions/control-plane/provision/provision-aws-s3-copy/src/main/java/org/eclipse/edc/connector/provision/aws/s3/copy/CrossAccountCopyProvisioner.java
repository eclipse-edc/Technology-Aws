package org.eclipse.edc.connector.provision.aws.s3.copy;

import com.fasterxml.jackson.core.type.TypeReference;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import org.eclipse.edc.aws.s3.AwsClientProvider;
import org.eclipse.edc.aws.s3.AwsTemporarySecretToken;
import org.eclipse.edc.aws.s3.S3ClientRequest;
import org.eclipse.edc.connector.controlplane.transfer.spi.provision.Provisioner;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.DeprovisionedResource;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.ProvisionResponse;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.ProvisionedResource;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.ResourceDefinition;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.response.StatusResult;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.util.string.StringUtils;
import software.amazon.awssdk.services.iam.IamAsyncClient;
import software.amazon.awssdk.services.iam.model.CreateRoleRequest;
import software.amazon.awssdk.services.iam.model.CreateRoleResponse;
import software.amazon.awssdk.services.iam.model.DeleteRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.DeleteRolePolicyResponse;
import software.amazon.awssdk.services.iam.model.DeleteRoleRequest;
import software.amazon.awssdk.services.iam.model.DeleteRoleResponse;
import software.amazon.awssdk.services.iam.model.GetUserResponse;
import software.amazon.awssdk.services.iam.model.PutRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.Role;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetBucketPolicyRequest;
import software.amazon.awssdk.services.s3.model.GetBucketPolicyResponse;
import software.amazon.awssdk.services.s3.model.PutBucketPolicyRequest;
import software.amazon.awssdk.services.s3.model.PutBucketPolicyResponse;
import software.amazon.awssdk.services.sts.StsAsyncClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static java.util.Optional.ofNullable;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.BUCKET_NAME;
import static org.eclipse.edc.connector.provision.aws.s3.copy.util.S3CopyTemplates.BUCKET_POLICY_STATEMENT_TEMPLATE;
import static org.eclipse.edc.connector.provision.aws.s3.copy.util.S3CopyTemplates.CROSS_ACCOUNT_ROLE_POLICY_TEMPLATE;
import static org.eclipse.edc.connector.provision.aws.s3.copy.util.S3CopyTemplates.CROSS_ACCOUNT_ROLE_TRUST_POLICY_TEMPLATE;
import static org.eclipse.edc.connector.provision.aws.s3.copy.util.S3CopyTemplates.EMPTY_BUCKET_POLICY;

public class CrossAccountCopyProvisioner implements Provisioner<CrossAccountCopyResourceDefinition, CrossAccountCopyProvisionedResource> {
    
    private final AwsClientProvider clientProvider;
    private final Vault vault;
    private final RetryPolicy<Object> retryPolicy;
    private final TypeManager typeManager;
    private final Monitor monitor;
    
    public CrossAccountCopyProvisioner(AwsClientProvider clientProvider, Vault vault, RetryPolicy<Object> retryPolicy, TypeManager typeManager, Monitor monitor) {
        this.clientProvider = clientProvider;
        this.vault = vault;
        this.retryPolicy = retryPolicy;
        this.typeManager = typeManager;
        this.monitor = monitor;
    }
    
    @Override
    public boolean canProvision(ResourceDefinition resourceDefinition) {
        return resourceDefinition instanceof CrossAccountCopyResourceDefinition;
    }
    
    @Override
    public boolean canDeprovision(ProvisionedResource provisionedResource) {
        return provisionedResource instanceof CrossAccountCopyProvisionedResource;
    }
    
    @Override
    public CompletableFuture<StatusResult<ProvisionResponse>> provision(CrossAccountCopyResourceDefinition resourceDefinition, Policy policy) {
        var iamClient = clientProvider.iamAsyncClient();
        //TODO region for sts should be configurable -> choose region closest to where EDC deployed
        var stsClient = clientProvider.stsAsyncClient("eu-central-1");
        
        var secretToken = getTemporarySecretToken(resourceDefinition.getDestinationKeyName());
        var s3ClientRequest = S3ClientRequest.from(resourceDefinition.getDestinationRegion(), null, secretToken);
        var s3Client = clientProvider.s3AsyncClient(s3ClientRequest);
        
        monitor.debug("S3 CrossAccountCopyProvisioner: getting IAM user");
        return iamClient.getUser()
                .thenCompose(response -> createRole(iamClient, resourceDefinition, response))
                .thenCompose(response -> putRolePolicy(iamClient, resourceDefinition, response))
                .thenCompose(provisionDetails -> getDestinationBucketPolicy(s3Client, resourceDefinition, provisionDetails))
                .thenCompose(provisionDetails -> updateDestinationBucketPolicy(s3Client, resourceDefinition, provisionDetails))
                .thenCompose(role -> assumeRole(stsClient, resourceDefinition, role))
                .thenApply(provisionResponse -> provisioningSucceeded(resourceDefinition, provisionResponse));
    }
    
    private CompletableFuture<CreateRoleResponse> createRole(IamAsyncClient iamClient,
                                                             CrossAccountCopyResourceDefinition resourceDefinition,
                                                             GetUserResponse getUserResponse) {
        var roleName = roleIdentifier(resourceDefinition);
        var trustPolicy = CROSS_ACCOUNT_ROLE_TRUST_POLICY_TEMPLATE
                .replace("{{user-arn}}", getUserResponse.user().arn());
        
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            var createRoleRequest = CreateRoleRequest.builder()
                    .roleName(roleName)
                    .description(format("Role for EDC transfer: %s", roleName))
                    .assumeRolePolicyDocument(trustPolicy)
                    .build();
    
            monitor.debug(format("S3 CrossAccountCopyProvisioner: creating IAM role '%s'", roleName));
            return iamClient.createRole(createRoleRequest);
        });
    }
    
    private CompletableFuture<CrossAccountCopyProvisionSteps> putRolePolicy(IamAsyncClient iamClient,
                                                                            CrossAccountCopyResourceDefinition resourceDefinition,
                                                                            CreateRoleResponse createRoleResponse) {
        var rolePolicy = CROSS_ACCOUNT_ROLE_POLICY_TEMPLATE
                .replace("{{source-bucket}}", resourceDefinition.getSourceDataAddress().getStringProperty(BUCKET_NAME))
                .replace("{{destination-bucket}}", resourceDefinition.getDestinationBucketName());
        
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            Role role = createRoleResponse.role();
            var putRolePolicyRequest = PutRolePolicyRequest.builder()
                    .roleName(createRoleResponse.role().roleName())
                    .policyName(roleIdentifier(resourceDefinition))
                    .policyDocument(rolePolicy)
                    .build();
    
            monitor.debug("S3 CrossAccountCopyProvisioner: putting IAM role policy");
            return iamClient.putRolePolicy(putRolePolicyRequest)
                    .thenApply(policyResponse -> new CrossAccountCopyProvisionSteps(role));
        });
    }
    
    private CompletableFuture<CrossAccountCopyProvisionSteps> getDestinationBucketPolicy(S3AsyncClient s3Client,
                                                                                         CrossAccountCopyResourceDefinition resourceDefinition,
                                                                                         CrossAccountCopyProvisionSteps provisionDetails) {
        var getBucketPolicyRequest = GetBucketPolicyRequest.builder()
                .bucket(resourceDefinition.getDestinationBucketName())
                .build();
        
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            monitor.debug("S3 CrossAccountCopyProvisioner: getting destination bucket policy");
            return s3Client.getBucketPolicy(getBucketPolicyRequest)
                    .handle((result, ex) -> {
                        if (ex == null) {
                            provisionDetails.setBucketPolicy(result.policy());
                            return provisionDetails;
                        } else {
                            provisionDetails.setBucketPolicy(EMPTY_BUCKET_POLICY);
                            return provisionDetails;
                        }
                    });
        });
    }
    
    private CompletableFuture<CrossAccountCopyProvisionSteps> updateDestinationBucketPolicy(S3AsyncClient s3Client,
                                                                                            CrossAccountCopyResourceDefinition resourceDefinition,
                                                                                            CrossAccountCopyProvisionSteps provisionDetails) {
        var bucketPolicyStatement = BUCKET_POLICY_STATEMENT_TEMPLATE
                .replace("{{sid}}", resourceDefinition.getBucketPolicyStatementSid())
                .replace("{{source-account-role-arn}}", provisionDetails.getRole().arn())
                .replace("{{sink-bucket-name}}", resourceDefinition.getDestinationBucketName());
        
        var typeReference = new TypeReference<HashMap<String,Object>>() {};
        var statementJson = Json.createObjectBuilder(typeManager.readValue(bucketPolicyStatement, typeReference)).build();
        var policyJson = Json.createObjectBuilder(typeManager.readValue(provisionDetails.getBucketPolicy(), typeReference)).build();
        
        var statements = Json.createArrayBuilder(policyJson.getJsonArray("Statement"))
                .add(statementJson)
                .build();
        var updatedBucketPolicy = Json.createObjectBuilder(policyJson)
                .add("Statement", statements)
                .build().toString();
        
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            monitor.debug("S3 CrossAccountCopyProvisioner: updating destination bucket policy");
            var putBucketPolicyRequest = PutBucketPolicyRequest.builder()
                    .bucket(resourceDefinition.getDestinationBucketName())
                    .policy(updatedBucketPolicy)
                    .build();
            return s3Client.putBucketPolicy(putBucketPolicyRequest)
                    .thenApply(response -> provisionDetails);
        });
    }
    
    private CompletableFuture<S3ProvisionResponse> assumeRole(StsAsyncClient stsClient,
                                                              CrossAccountCopyResourceDefinition resourceDefinition,
                                                              CrossAccountCopyProvisionSteps provisionDetails) {
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            var role = provisionDetails.getRole();
            var assumeRoleRequest = AssumeRoleRequest.builder()
                    .roleArn(role.arn())
                    .roleSessionName(roleIdentifier(resourceDefinition))
                    .build();
            
            monitor.debug(format("S3 CrossAccountCopyProvisioner: assuming role '%s'", role.arn()));
            return stsClient.assumeRole(assumeRoleRequest)
                    .thenApply(response -> new S3ProvisionResponse(role, response.credentials()));
        });
    }
    
    private StatusResult<ProvisionResponse> provisioningSucceeded(CrossAccountCopyResourceDefinition resourceDefinition,
                                                                  S3ProvisionResponse provisionResponse) {
        var identifier = roleIdentifier(resourceDefinition);
        var provisionedResource = CrossAccountCopyProvisionedResource.Builder.newInstance()
                .id(identifier)
                .resourceName(identifier)
                .resourceDefinitionId(resourceDefinition.getId())
                .transferProcessId(resourceDefinition.getTransferProcessId())
                .dataAddress(resourceDefinition.getSourceDataAddress())
                .sourceAccountRole(provisionResponse.getRole())
                .destinationRegion(resourceDefinition.getDestinationRegion())
                .destinationBucketName(resourceDefinition.getDestinationBucketName())
                .destinationKeyName(resourceDefinition.getDestinationKeyName())
                .bucketPolicyStatementSid(resourceDefinition.getBucketPolicyStatementSid())
                .build();
        
        var credentials = provisionResponse.getCredentials();
        var secretToken = new AwsTemporarySecretToken(credentials.accessKeyId(), credentials.secretAccessKey(),
                credentials.sessionToken(), credentials.expiration().toEpochMilli());
    
        monitor.debug("S3 CrossAccountCopyProvisioner: completing provisioning");
        return StatusResult.success(ProvisionResponse.Builder.newInstance()
                .resource(provisionedResource)
                .secretToken(secretToken)
                .build());
    }
    
    private String roleIdentifier(CrossAccountCopyResourceDefinition resourceDefinition) {
        return format("edc-transfer-role_%s", resourceDefinition.getTransferProcessId());
    }
    
    @Override
    public CompletableFuture<StatusResult<DeprovisionedResource>> deprovision(CrossAccountCopyProvisionedResource provisionedResource, Policy policy) {
        var secretToken = getTemporarySecretToken(provisionedResource.getDestinationKeyName());
        var s3ClientRequest = S3ClientRequest.from(provisionedResource.getDestinationRegion(), null, secretToken);
        var s3Client = clientProvider.s3AsyncClient(s3ClientRequest);
    
        var iamClient = clientProvider.iamAsyncClient();
        var roleName = provisionedResource.getSourceAccountRole().roleName();
        
        var getBucketPolicyRequest = GetBucketPolicyRequest.builder()
                .bucket(provisionedResource.getDestinationBucketName())
                .build();
    
        monitor.debug("S3 CrossAccountCopyProvisioner: getting destination bucket policy");
        return s3Client.getBucketPolicy(getBucketPolicyRequest)
                .thenCompose(response -> updateBucketPolicy(s3Client, provisionedResource, response))
                .thenCompose(response -> deleteRolePolicy(iamClient, roleName))
                .thenCompose(response -> deleteRole(iamClient, roleName))
                .thenApply(response -> StatusResult.success(DeprovisionedResource.Builder.newInstance()
                        .provisionedResourceId(provisionedResource.getId())
                        .build()));
    }
    
    private CompletableFuture<PutBucketPolicyResponse> updateBucketPolicy(S3AsyncClient s3Client,
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
                .build().toString();
        
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            monitor.debug("S3 CrossAccountCopyProvisioner: updating destination bucket policy");
            var putBucketPolicyRequest = PutBucketPolicyRequest.builder()
                    .bucket(provisionedResource.getDestinationBucketName())
                    .policy(updatedBucketPolicy)
                    .build();
            return s3Client.putBucketPolicy(putBucketPolicyRequest);
        });
    }
    
    private CompletableFuture<DeleteRolePolicyResponse> deleteRolePolicy(IamAsyncClient iamClient, String roleName) {
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            var deleteRolePolicyRequest = DeleteRolePolicyRequest.builder()
                    .roleName(roleName)
                    .policyName(roleName)
                    .build();
    
            monitor.debug("S3 CrossAccountCopyProvisioner: deleting IAM role policy");
            return iamClient.deleteRolePolicy(deleteRolePolicyRequest);
        });
    }
    
    private CompletableFuture<DeleteRoleResponse> deleteRole(IamAsyncClient iamClient, String roleName) {
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            monitor.debug("S3 CrossAccountCopyProvisioner: deleting IAM role");
            var deleteRoleRequest = DeleteRoleRequest.builder()
                    .roleName(roleName)
                    .build();
            
            return iamClient.deleteRole(deleteRoleRequest);
        });
    }
    
    private AwsTemporarySecretToken getTemporarySecretToken(String secretKeyName) {
        return ofNullable(secretKeyName)
                .filter(keyName -> !StringUtils.isNullOrBlank(keyName))
                .map(vault::resolveSecret)
                .filter(secret -> !StringUtils.isNullOrBlank(secret))
                .map(secret -> typeManager.readValue(secret, AwsTemporarySecretToken.class))
                .orElse(null);
    }
}
