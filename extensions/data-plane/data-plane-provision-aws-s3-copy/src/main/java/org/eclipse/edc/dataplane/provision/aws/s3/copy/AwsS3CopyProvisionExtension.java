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

import dev.failsafe.RetryPolicy;
import org.eclipse.edc.aws.s3.AwsClientProvider;
import org.eclipse.edc.connector.dataplane.spi.provision.ProvisionerManager;
import org.eclipse.edc.connector.dataplane.spi.provision.ResourceDefinitionGeneratorManager;
import org.eclipse.edc.participantcontext.single.spi.SingleParticipantContextSupplier;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.runtime.metamodel.annotation.Setting;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.spi.types.TypeManager;
import software.amazon.awssdk.awscore.exception.AwsServiceException;

/**
 * Provides provisioners for preparing a cross-account copy of S3 objects.
 */
@Extension(value = AwsS3CopyProvisionExtension.NAME)
public class AwsS3CopyProvisionExtension implements ServiceExtension {

    public static final String NAME = "AWS S3 Copy Data Plane Provision";

    public static final String S3_COPY_PROVISION_TYPE = "S3Copy";
    
    @Setting(key = "edc.aws.provision.retry.retries.max", description = "Maximum number of retries for AWS requests performed during (de)provisioning.", defaultValue = "" + 10)
    private int maxRetries;
    @Setting(key = "edc.aws.provision.role.duration.session.max", description = "Maximum session duration for roles created during provisioning in seconds.", defaultValue = "" + 3600)
    private int maxRoleSessionDuration;
    
    @Inject
    private Vault vault;
    @Inject
    private Monitor monitor;
    @Inject
    private AwsClientProvider clientProvider;
    @Inject
    private TypeManager typeManager;
    @Inject
    private RetryPolicy<Object> retryPolicy;
    @Inject
    private ResourceDefinitionGeneratorManager manifestGenerator;
    @Inject
    private ProvisionerManager provisionerManager;
    @Inject
    private SingleParticipantContextSupplier singleParticipantContextSupplier;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void initialize(ServiceExtensionContext context) {
        manifestGenerator.registerProviderGenerator(new S3CopyResourceDefinitionGenerator());

        var retryPolicy = RetryPolicy.builder(this.retryPolicy.getConfig())
                .withMaxRetries(0)
                .handle(AwsServiceException.class)
                .build();

        provisionerManager.register(new S3CopyProvisioner(clientProvider, vault, typeManager,
                monitor.withPrefix(S3CopyProvisioner.class.getSimpleName()),
                context.getComponentId(), maxRoleSessionDuration, retryPolicy, singleParticipantContextSupplier));
        provisionerManager.register(new S3CopyDeprovisioner(clientProvider, vault, typeManager,
                monitor.withPrefix(S3CopyDeprovisioner.class.getSimpleName()), retryPolicy, singleParticipantContextSupplier));
    }

}


