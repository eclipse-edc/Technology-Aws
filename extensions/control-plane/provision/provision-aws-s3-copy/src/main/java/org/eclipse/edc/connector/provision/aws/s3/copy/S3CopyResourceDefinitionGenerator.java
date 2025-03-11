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

import org.eclipse.edc.connector.controlplane.transfer.spi.provision.ProviderResourceDefinitionGenerator;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.ResourceDefinition;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcess;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.jetbrains.annotations.Nullable;

import static java.util.UUID.randomUUID;
import static org.eclipse.edc.aws.s3.copy.lib.S3CopyUtils.applicableForS3CopyTransfer;
import static org.eclipse.edc.aws.s3.copy.lib.S3CopyUtils.getDestinationFileName;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.BUCKET_NAME;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.ENDPOINT_OVERRIDE;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.FOLDER_NAME;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.OBJECT_NAME;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.REGION;
import static org.eclipse.edc.connector.provision.aws.s3.copy.util.S3CopyProvisionUtils.resourceIdentifier;

/**
 * Generates information for provisioning AWS resources for a cross-account copy of S3 objects.
 */
public class S3CopyResourceDefinitionGenerator implements ProviderResourceDefinitionGenerator {
    
    @Override
    public @Nullable ResourceDefinition generate(TransferProcess transferProcess, DataAddress assetAddress, Policy policy) {
        var bucketPolicyStatementSid = resourceIdentifier(transferProcess.getId());
        
        var destination = transferProcess.getDataDestination();

        var destinationKey = destination.getStringProperty(OBJECT_NAME) != null ?
                destination.getStringProperty(OBJECT_NAME) : transferProcess.getContentDataAddress().getStringProperty(OBJECT_NAME);
        var destinationFileName = getDestinationFileName(destinationKey, destination.getStringProperty(FOLDER_NAME));
        
        return S3CopyResourceDefinition.Builder.newInstance()
                .id(randomUUID().toString())
                .endpointOverride(destination.getStringProperty(ENDPOINT_OVERRIDE))
                .destinationRegion(destination.getStringProperty(REGION))
                .destinationBucketName(destination.getStringProperty(BUCKET_NAME))
                .destinationObjectName(destinationFileName)
                .destinationKeyName(destination.getKeyName())
                .bucketPolicyStatementSid(bucketPolicyStatementSid)
                .sourceDataAddress(transferProcess.getContentDataAddress())
                .build();
    }
    
    @Override
    public boolean canGenerate(TransferProcess transferProcess, DataAddress assetAddress, Policy policy) {
        return applicableForS3CopyTransfer(transferProcess.getContentDataAddress(), transferProcess.getDataDestination());
    }
}
