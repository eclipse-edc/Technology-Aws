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

import org.eclipse.edc.aws.s3.spi.S3BucketSchema;
import org.eclipse.edc.connector.dataplane.spi.DataFlow;
import org.eclipse.edc.connector.dataplane.spi.provision.ProvisionResource;
import org.eclipse.edc.connector.dataplane.spi.provision.ResourceDefinitionGenerator;

/**
 * Generates S3 buckets on the consumer (requesting connector) that serve as data destinations.
 */
public class S3ConsumerProvisionResourceGenerator implements ResourceDefinitionGenerator {

    @Override
    public String supportedType() {
        return S3BucketSchema.TYPE;
    }

    @Override
    public ProvisionResource generate(DataFlow dataFlow) {
        return ProvisionResource.Builder.newInstance()
                .flowId(dataFlow.getId())
                .type(S3BucketSchema.TYPE)
                .dataAddress(dataFlow.getDestination())
                .build();
    }
}
