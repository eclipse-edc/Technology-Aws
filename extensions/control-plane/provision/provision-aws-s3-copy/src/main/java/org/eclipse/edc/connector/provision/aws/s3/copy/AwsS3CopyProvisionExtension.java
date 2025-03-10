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
 * Provides {@link Provisioner}s for preparing a cross-account copy of S3 objects.
 */
@Extension(value = AwsS3CopyProvisionExtension.NAME)
public class AwsS3CopyProvisionExtension implements ServiceExtension {

    public static final String NAME = "AWS S3 Copy Provision";
    
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
    @Inject
    private ResourceManifestGenerator manifestGenerator;
    @Inject
    private ProvisionManager provisionManager;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void initialize(ServiceExtensionContext context) {
        monitor = context.getMonitor();
    
        // register resource definition generator
        manifestGenerator.registerGenerator(new S3CopyResourceDefinitionGenerator());
        
        // register provisioner
        var retryPolicy = (RetryPolicy<Object>) context.getService(RetryPolicy.class);
        int maxRetries = context.getSetting(PROVISION_MAX_RETRY, 10);
        int maxRoleSessionDuration = context.getSetting(PROVISION_MAX_ROLE_SESSION_DURATION, 3600);
        
        var provisioner = new S3CopyProvisioner(clientProvider, vault, retryPolicy, typeManager, monitor, context.getComponentId(), maxRetries, maxRoleSessionDuration);
        provisionManager.register(provisioner);

        registerTypes(typeManager);
    }

    @Override
    public void shutdown() {
        try {
            clientProvider.shutdown();
        } catch (Exception e) {
            monitor.severe("Error closing AWS client provider", e);
        }
    }

    private void registerTypes(TypeManager typeManager) {
        typeManager.registerTypes(S3CopyProvisionedResource.class, S3CopyResourceDefinition.class, AwsTemporarySecretToken.class);
    }


}


