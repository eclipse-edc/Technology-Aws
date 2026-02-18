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

import dev.failsafe.RetryPolicy;
import org.eclipse.edc.aws.s3.AwsClientProvider;
import org.eclipse.edc.connector.dataplane.spi.provision.ProvisionerManager;
import org.eclipse.edc.connector.dataplane.spi.provision.ResourceDefinitionGeneratorManager;
import org.eclipse.edc.runtime.metamodel.annotation.Configuration;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.spi.types.TypeManager;

/**
 * Provides provisioners for data transfer backed by AWS services.
 */
@Extension(value = DataPlaneProvisionAwsS3Extension.NAME)
public class DataPlaneProvisionAwsS3Extension implements ServiceExtension {

    public static final String NAME = "Data Plane Provision AWS S3";

    @Configuration
    private S3BucketProvisionerConfiguration configuration;

    @Inject
    private Vault vault;
    @Inject
    private AwsClientProvider clientProvider;
    @Inject
    private ProvisionerManager provisionerManager;
    @Inject
    private RetryPolicy<Object> retryPolicy;
    @Inject
    private ResourceDefinitionGeneratorManager resourceDefinitionGeneratorManager;
    @Inject
    private TypeManager typeManager;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void initialize(ServiceExtensionContext context) {
        resourceDefinitionGeneratorManager.registerConsumerGenerator(new S3ConsumerProvisionResourceGenerator());

        provisionerManager.register(new S3BucketProvisioner(clientProvider,
                context.getMonitor().withPrefix("S3BucketProvisioner"), vault, retryPolicy, configuration, typeManager));
        provisionerManager.register(new S3BucketDeprovisioner(clientProvider,
                context.getMonitor().withPrefix("S3BucketDeprovisioner"), vault, retryPolicy, configuration));
    }

}


