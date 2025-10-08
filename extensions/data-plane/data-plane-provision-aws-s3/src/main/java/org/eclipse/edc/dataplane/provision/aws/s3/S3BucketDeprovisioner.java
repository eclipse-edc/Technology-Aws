/*
 *  Copyright (c) 2025 Think-it GmbH
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Think-it GmbH - initial API and implementation
 *
 */

package org.eclipse.edc.dataplane.provision.aws.s3;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import org.eclipse.edc.aws.s3.AwsClientProvider;
import org.eclipse.edc.aws.s3.S3ClientRequest;
import org.eclipse.edc.aws.s3.spi.S3BucketSchema;
import org.eclipse.edc.connector.dataplane.spi.provision.DeprovisionedResource;
import org.eclipse.edc.connector.dataplane.spi.provision.Deprovisioner;
import org.eclipse.edc.connector.dataplane.spi.provision.ProvisionResource;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.response.StatusResult;
import org.eclipse.edc.spi.security.Vault;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.iam.IamAsyncClient;
import software.amazon.awssdk.services.iam.model.DeleteRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.DeleteRolePolicyResponse;
import software.amazon.awssdk.services.iam.model.DeleteRoleRequest;
import software.amazon.awssdk.services.iam.model.DeleteRoleResponse;

import java.util.concurrent.CompletableFuture;

/**
 * Deprovisions S3 bucket role.
 */
public class S3BucketDeprovisioner implements Deprovisioner {

    private final AwsClientProvider clientProvider;
    private final Monitor monitor;
    private final Vault vault;
    private final RetryPolicy<Object> retryPolicy;

    public S3BucketDeprovisioner(AwsClientProvider clientProvider, Monitor monitor, Vault vault, RetryPolicy<Object> retryPolicy, S3BucketProvisionerConfiguration configuration) {
        this.clientProvider = clientProvider;
        this.monitor = monitor;
        this.vault = vault;
        this.retryPolicy = RetryPolicy.builder(retryPolicy.getConfig())
                .withMaxRetries(configuration.maxRetries())
                .handle(AwsServiceException.class)
                .build();
    }

    @Override
    public String supportedType() {
        return S3BucketSchema.TYPE;
    }

    @Override
    public CompletableFuture<StatusResult<DeprovisionedResource>> deprovision(ProvisionResource resource) {
        var clientRequest = S3ClientRequest.from(
                resource.getDataAddress().getStringProperty(S3BucketSchema.REGION),
                resource.getDataAddress().getStringProperty(S3BucketSchema.ENDPOINT_OVERRIDE)
        );
        var iamClient = clientProvider.iamAsyncClient(clientRequest);

        var roleName = (String) resource.getProvisionedResource().getProperty(S3BucketSchema.ROLE_NAME);

        return deleteRolePolicy(iamClient, roleName)
                .thenCompose(deleteRolePolicyResponse -> deleteRole(iamClient, roleName))
                .thenCompose(deleteSecretResponse -> deleteSecret(resource))
                .thenApply(ignore -> StatusResult.success(DeprovisionedResource.Builder.from(resource).build()));
    }

    private CompletableFuture<DeleteRoleResponse> deleteRole(IamAsyncClient iamClient, String role) {
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            monitor.debug("delete role");
            return iamClient.deleteRole(DeleteRoleRequest.builder().roleName(role).build());
        });
    }

    private CompletableFuture<DeleteRolePolicyResponse> deleteRolePolicy(IamAsyncClient iamClient, String roleName) {
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            monitor.debug("deleting inline policies for Role " + roleName);
            return iamClient.deleteRolePolicy(DeleteRolePolicyRequest.builder().roleName(roleName).policyName(roleName).build());
        });
    }

    private CompletableFuture<?> deleteSecret(ProvisionResource resource) {
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            monitor.debug("delete secret from vault");
            var response = vault.deleteSecret(resource.getDataAddress().getKeyName());
            return CompletableFuture.completedFuture(response);
        });
    }


}


