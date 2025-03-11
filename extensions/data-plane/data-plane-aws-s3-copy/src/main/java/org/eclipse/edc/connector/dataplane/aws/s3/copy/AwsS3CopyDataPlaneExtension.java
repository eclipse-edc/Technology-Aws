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

package org.eclipse.edc.connector.dataplane.aws.s3.copy;

import org.eclipse.edc.aws.s3.AwsClientProvider;
import org.eclipse.edc.connector.dataplane.spi.registry.TransferServiceRegistry;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.runtime.metamodel.annotation.Setting;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.validator.spi.DataAddressValidatorRegistry;

/**
 * Provides a {@link org.eclipse.edc.connector.dataplane.spi.pipeline.TransferService} for
 * executing a cross-account copy of S3 objects.
 */
@Extension(value = AwsS3CopyDataPlaneExtension.NAME)
public class AwsS3CopyDataPlaneExtension implements ServiceExtension {
    
    public static final String NAME = "AWS S3 Copy Data Plane";
    
    private static final int DEFAULT_CHUNK_SIZE_IN_MB = 500;
    
    @Setting(key = "edc.dataplane.aws.sink.chunk.size.mb", defaultValue = "" + DEFAULT_CHUNK_SIZE_IN_MB)
    private int chunkSizeInMb;
    
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
    
    @Override
    public String name() {
        return NAME;
    }
    
    @Override
    public void initialize(ServiceExtensionContext context) {
        var s3CopyTransferService = new AwsS3CopyTransferService(clientProvider, vault, typeManager, validator, monitor, chunkSizeInMb);
        registry.registerTransferService(s3CopyTransferService);
    }
}
