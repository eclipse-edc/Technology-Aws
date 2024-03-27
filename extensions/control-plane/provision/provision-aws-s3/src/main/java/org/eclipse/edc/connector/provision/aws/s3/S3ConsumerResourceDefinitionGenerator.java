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
 *
 */

package org.eclipse.edc.connector.provision.aws.s3;

import org.eclipse.edc.aws.s3.S3BucketSchema;
import org.eclipse.edc.connector.transfer.spi.provision.ConsumerResourceDefinitionGenerator;
import org.eclipse.edc.connector.transfer.spi.types.ResourceDefinition;
import org.eclipse.edc.connector.transfer.spi.types.TransferProcess;
import org.eclipse.edc.policy.model.Policy;
import software.amazon.awssdk.regions.Region;

import static java.util.UUID.randomUUID;

/**
 * Generates S3 buckets on the consumer (requesting connector) that serve as data destinations.
 */
public class S3ConsumerResourceDefinitionGenerator implements ConsumerResourceDefinitionGenerator {

    @Override
    public ResourceDefinition generate(TransferProcess transferProcess, Policy policy) {
        if (transferProcess.getDataDestination().getStringProperty(S3BucketSchema.REGION) == null) {
            // FIXME generate region from policy engine
            return S3BucketResourceDefinition.Builder.newInstance().id(randomUUID().toString()).bucketName(transferProcess.getDataDestination().getStringProperty(S3BucketSchema.BUCKET_NAME)).regionId(Region.US_EAST_1.id()).build();
        }
        var destination = transferProcess.getDataDestination();
        var id = randomUUID().toString();

        return S3BucketResourceDefinition.Builder.newInstance().id(id).bucketName(destination.getStringProperty(S3BucketSchema.BUCKET_NAME)).regionId(destination.getStringProperty(S3BucketSchema.REGION)).build();
    }

    @Override
    public boolean canGenerate(TransferProcess dataRequest, Policy policy) {
        return S3BucketSchema.TYPE.equals(dataRequest.getDestinationType());
    }
}
