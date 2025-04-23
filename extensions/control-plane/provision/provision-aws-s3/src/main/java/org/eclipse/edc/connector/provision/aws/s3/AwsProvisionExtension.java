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
 *
 */

package org.eclipse.edc.connector.provision.aws.s3;

import dev.failsafe.RetryPolicy;
import org.eclipse.edc.aws.s3.AwsClientProvider;
import org.eclipse.edc.aws.s3.AwsTemporarySecretToken;
import org.eclipse.edc.connector.controlplane.transfer.spi.provision.ProvisionManager;
import org.eclipse.edc.connector.controlplane.transfer.spi.provision.Provisioner;
import org.eclipse.edc.connector.controlplane.transfer.spi.provision.ResourceManifestGenerator;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.runtime.metamodel.annotation.Setting;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.spi.types.TypeManager;

/**
 * Provides data transfer {@link Provisioner}s backed by AWS services.
 */
@Extension(value = AwsProvisionExtension.NAME)
public class AwsProvisionExtension implements ServiceExtension {

    public static final String NAME = "AWS Provision";
    @Setting
    private static final String PROVISION_MAX_RETRY = "edc.aws.provision.retry.retries.max";
    @Setting
    private static final String PROVISION_MAX_ROLE_SESSION_DURATION = "edc.aws.provision.role.duration.session.max";
    @Inject
    private Vault vault;
    @Inject
    private Monitor monitor;
    @Inject
    private AwsClientProvider clientProvider;

    @Inject
    private TypeManager typeManager;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void initialize(ServiceExtensionContext context) {
        monitor = context.getMonitor();

        var provisionManager = context.getService(ProvisionManager.class);

        var retryPolicy = (RetryPolicy<Object>) context.getService(RetryPolicy.class);

        int maxRetries = context.getSetting(PROVISION_MAX_RETRY, 10);
        int roleMaxSessionDuration = context.getSetting(PROVISION_MAX_ROLE_SESSION_DURATION, 3600);
        var provisionerConfiguration = new S3BucketProvisionerConfiguration(maxRetries, roleMaxSessionDuration);
        var s3BucketProvisioner = new S3BucketProvisioner(clientProvider, monitor, vault, retryPolicy, provisionerConfiguration);
        provisionManager.register(s3BucketProvisioner);

        // register the generator
        var manifestGenerator = context.getService(ResourceManifestGenerator.class);
        manifestGenerator.registerGenerator(new S3ConsumerResourceDefinitionGenerator());

        registerTypes(typeManager);
    }

    private void registerTypes(TypeManager typeManager) {
        typeManager.registerTypes(S3BucketProvisionedResource.class, S3BucketResourceDefinition.class, AwsTemporarySecretToken.class);
    }


}


