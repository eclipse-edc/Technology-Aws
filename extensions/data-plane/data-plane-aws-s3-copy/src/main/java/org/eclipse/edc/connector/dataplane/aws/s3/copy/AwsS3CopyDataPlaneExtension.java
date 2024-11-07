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

package org.eclipse.edc.connector.dataplane.aws.s3.copy;

import org.eclipse.edc.aws.s3.AwsClientProvider;
import org.eclipse.edc.connector.dataplane.selector.spi.DataPlaneSelectorService;
import org.eclipse.edc.connector.dataplane.selector.spi.instance.DataPlaneInstance;
import org.eclipse.edc.connector.dataplane.spi.registry.TransferServiceRegistry;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.validator.spi.DataAddressValidatorRegistry;
import org.eclipse.edc.web.spi.configuration.context.ControlApiUrl;

import java.util.Set;

public class AwsS3CopyDataPlaneExtension implements ServiceExtension {
    
    @Inject
    private AwsClientProvider clientProvider;
    @Inject
    private TransferServiceRegistry registry;
    @Inject
    private Monitor monitor;
    @Inject
    private Vault vault;
    @Inject
    private TypeManager typeManager;
    @Inject
    private DataAddressValidatorRegistry validator;
    
    @Inject
    private DataPlaneSelectorService dataPlaneSelectorService;
    @Inject
    private ControlApiUrl controlApiUrl;
    
    @Override
    public void initialize(ServiceExtensionContext context) {
        var s3CopyTransferService = new AwsS3CopyTransferService(clientProvider, monitor, vault, typeManager, validator);
        registry.registerTransferService(s3CopyTransferService);
        
        //TODO hardcoded values
        //register the data plane instance
        var instance = DataPlaneInstance.Builder.newInstance()
                .id(context.getComponentId())
                .url(controlApiUrl.get().toString() + "/v1/dataflows")
                .allowedSourceTypes(Set.of("AmazonS3"))
                .allowedDestTypes(Set.of("AmazonS3"))
                .allowedTransferType("AmazonS3-PUSH")
                .build();
        dataPlaneSelectorService.addInstance(instance).onSuccess(it -> {
                    monitor.info("AWS-S3-copy data plane registered to control plane");
                })
                .orElseThrow(f -> new EdcException("Cannot register AWS-S3-copy data plane to the control plane: " + f.getFailureDetail()));
    }
}
