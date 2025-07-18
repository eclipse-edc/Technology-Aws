/*
 *  Copyright (c) 2020, 2021 Microsoft Corporation
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Microsoft Corporation - initial API and implementation
 *       ZF Friedrichshafen AG - improvements (refactoring of generate method)
 *       SAP SE - refactoring
 *       Cofinity-X - fix exception in canGenerate for PULL transfers
 *
 */

package org.eclipse.edc.connector.provision.aws.s3;

import org.eclipse.edc.aws.s3.spi.S3BucketSchema;
import org.eclipse.edc.connector.controlplane.transfer.spi.provision.ConsumerResourceDefinitionGenerator;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.ResourceDefinition;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcess;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.security.Vault;
import software.amazon.awssdk.regions.Region;

import static java.util.UUID.randomUUID;

/**
 * Generates S3 buckets on the consumer (requesting connector) that serve as data destinations.
 */
public class S3ConsumerResourceDefinitionGenerator implements ConsumerResourceDefinitionGenerator {

    private final Vault vault;

    public S3ConsumerResourceDefinitionGenerator(Vault vault) {
        this.vault = vault;
    }

    @Override
    public ResourceDefinition generate(TransferProcess transferProcess, Policy policy) {

        var dataDestination = transferProcess.getDataDestination();
        var endpointOverride = dataDestination.getStringProperty(S3BucketSchema.ENDPOINT_OVERRIDE);
        if (dataDestination.getStringProperty(S3BucketSchema.REGION) == null) {
            // FIXME generate region from policy engine
            return S3BucketResourceDefinition.Builder.newInstance().id(randomUUID().toString()).bucketName(dataDestination.getStringProperty(S3BucketSchema.BUCKET_NAME)).regionId(Region.US_EAST_1.id()).build();
        }
        var id = randomUUID().toString();

        var resourceDefinition = S3BucketResourceDefinition.Builder.newInstance()
                .id(id)
                .bucketName(dataDestination.getStringProperty(S3BucketSchema.BUCKET_NAME))
                .regionId(dataDestination.getStringProperty(S3BucketSchema.REGION))
                .endpointOverride(endpointOverride)
                .objectName(dataDestination.getStringProperty(S3BucketSchema.OBJECT_NAME));

        var accessKeyId = dataDestination.getStringProperty(S3BucketSchema.ACCESS_KEY_ID);
        var secretAccessKey = dataDestination.getStringProperty(S3BucketSchema.SECRET_ACCESS_KEY);

        if (accessKeyId != null && secretAccessKey != null) {
            resourceDefinition.accessKeyId(accessKeyId);
            vault.storeSecret(S3BucketSchema.SECRET_ACCESS_ALIAS_PREFIX + id, secretAccessKey);
        }

        return resourceDefinition.build();
    }

    @Override
    public boolean canGenerate(TransferProcess dataRequest, Policy policy) {
        return dataRequest.getDataDestination() != null &&
                S3BucketSchema.TYPE.equals(dataRequest.getDestinationType());
    }
}
