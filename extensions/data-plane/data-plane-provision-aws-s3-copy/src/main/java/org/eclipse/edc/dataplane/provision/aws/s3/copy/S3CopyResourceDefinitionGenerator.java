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

import org.eclipse.edc.aws.s3.spi.S3BucketSchema;
import org.eclipse.edc.connector.dataplane.spi.DataFlow;
import org.eclipse.edc.connector.dataplane.spi.provision.ProvisionResource;
import org.eclipse.edc.connector.dataplane.spi.provision.ResourceDefinitionGenerator;
import org.eclipse.edc.spi.types.domain.DataAddress;

import static org.eclipse.edc.aws.s3.copy.lib.S3CopyUtils.applicableForS3CopyTransfer;
import static org.eclipse.edc.aws.s3.copy.lib.S3CopyUtils.getDestinationKey;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.FOLDER_NAME;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.OBJECT_NAME;
import static org.eclipse.edc.dataplane.provision.aws.s3.copy.AwsS3CopyProvisionExtension.S3_COPY_PROVISION_TYPE;

/**
 * Generates information for provisioning AWS resources for a cross-account copy of S3 objects.
 */
public class S3CopyResourceDefinitionGenerator implements ResourceDefinitionGenerator {

    @Override
    public String supportedType() {
        return S3BucketSchema.TYPE;
    }

    @Override
    public ProvisionResource generate(DataFlow dataFlow) {
        var source = dataFlow.getSource();
        var destination = dataFlow.getDestination();
        if (!applicableForS3CopyTransfer(source, destination)) {
            return null;
        }

        var destinationFileName = destination.getStringProperty(OBJECT_NAME) != null ?
                destination.getStringProperty(OBJECT_NAME) : source.getStringProperty(OBJECT_NAME);
        var destinationKey = getDestinationKey(destinationFileName, destination.getStringProperty(FOLDER_NAME));

        var newDestination = DataAddress.Builder.newInstance()
                .properties(destination.getProperties())
                .property(OBJECT_NAME, destinationKey)
                .build();
        return ProvisionResource.Builder.newInstance()
                .flowId(dataFlow.getId())
                .type(S3_COPY_PROVISION_TYPE)
                .dataAddress(source)
                .property("newDestination", newDestination)
                .build();
    }
}
