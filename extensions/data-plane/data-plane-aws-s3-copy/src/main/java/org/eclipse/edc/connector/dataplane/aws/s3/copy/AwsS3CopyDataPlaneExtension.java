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
import org.eclipse.edc.aws.s3.spi.S3BucketSchema;
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
import org.eclipse.edc.spi.types.domain.transfer.FlowType;
import org.eclipse.edc.validator.spi.DataAddressValidatorRegistry;
import org.eclipse.edc.web.spi.configuration.context.ControlApiUrl;

import java.util.Set;

import static java.lang.String.format;

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
        
        // register the data plane instance
        var instance = DataPlaneInstance.Builder.newInstance()
                .id(context.getComponentId())
                .url(controlApiUrl.get().toString() + "/v1/dataflows")
                .allowedSourceTypes(Set.of(S3BucketSchema.TYPE))
                .allowedDestTypes(Set.of(S3BucketSchema.TYPE))
                .allowedTransferType(format("%s-%s", S3BucketSchema.TYPE, FlowType.PUSH.name()))
                .build();
        dataPlaneSelectorService.addInstance(instance)
                .onSuccess(it -> monitor.info("AWS-S3-copy data plane registered to control plane."))
                .orElseThrow(f -> new EdcException(format("Cannot register AWS-S3-copy data plane to the control plane: %s.", f.getFailureDetail())));
    }
}
