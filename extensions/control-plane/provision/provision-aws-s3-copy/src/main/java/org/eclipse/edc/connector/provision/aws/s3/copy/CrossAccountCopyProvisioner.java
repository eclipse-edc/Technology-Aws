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

import dev.failsafe.RetryPolicy;
import org.eclipse.edc.aws.s3.AwsClientProvider;
import org.eclipse.edc.aws.s3.AwsTemporarySecretToken;
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
import software.amazon.awssdk.awscore.exception.AwsServiceException;

import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;

public class CrossAccountCopyProvisioner implements Provisioner<CrossAccountCopyResourceDefinition, CrossAccountCopyProvisionedResource> {
    
    private final AwsClientProvider clientProvider;
    private final Vault vault;
    private final RetryPolicy<Object> retryPolicy;
    private final TypeManager typeManager;
    private final Monitor monitor;
    private final String componentId;
    private final int maxRoleSessionDuration;
    
    public CrossAccountCopyProvisioner(AwsClientProvider clientProvider, Vault vault,
                                       RetryPolicy<Object> retryPolicy, TypeManager typeManager,
                                       Monitor monitor, String componentId, int maxRetries,
                                       int maxRoleSessionDuration) {
        this.clientProvider = clientProvider;
        this.vault = vault;
        this.typeManager = typeManager;
        this.monitor = monitor;
        this.componentId = componentId;
        this.maxRoleSessionDuration = maxRoleSessionDuration;
        this.retryPolicy = RetryPolicy.builder(retryPolicy.getConfig())
                .withMaxRetries(maxRetries)
                .handle(AwsServiceException.class)
                .build();
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
        return CrossAccountCopyProvisionPipeline.Builder.newInstance()
                .clientProvider(clientProvider)
                .vault(vault)
                .retryPolicy(retryPolicy)
                .typeManager(typeManager)
                .monitor(monitor)
                .componentId(componentId)
                .maxRoleSessionDuration(maxRoleSessionDuration)
                .build()
                .provision(resourceDefinition)
                .thenApply(response -> provisioningSucceeded(resourceDefinition, response));
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
                .sourceAccountRole(provisionResponse.role())
                .destinationRegion(resourceDefinition.getDestinationRegion())
                .destinationBucketName(resourceDefinition.getDestinationBucketName())
                .destinationKeyName(resourceDefinition.getDestinationKeyName())
                .bucketPolicyStatementSid(resourceDefinition.getBucketPolicyStatementSid())
                .build();
        
        var credentials = provisionResponse.credentials();
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
        return CrossAccountCopyDeprovisionPipeline.Builder.newInstance()
                .clientProvider(clientProvider)
                .vault(vault)
                .retryPolicy(retryPolicy)
                .typeManager(typeManager)
                .monitor(monitor)
                .build()
                .deprovision(provisionedResource)
                .thenApply(StatusResult::success);
    }
}
