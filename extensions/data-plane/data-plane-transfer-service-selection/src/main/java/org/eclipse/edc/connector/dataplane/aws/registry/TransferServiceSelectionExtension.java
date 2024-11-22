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

package org.eclipse.edc.connector.dataplane.aws.registry;

import org.eclipse.edc.connector.dataplane.framework.registry.TransferServiceSelectionStrategy;
import org.eclipse.edc.connector.dataplane.spi.pipeline.PipelineService;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.runtime.metamodel.annotation.Provider;
import org.eclipse.edc.spi.system.ServiceExtension;

@Extension(value = TransferServiceSelectionExtension.NAME)
public class TransferServiceSelectionExtension implements ServiceExtension {
    
    public static final String NAME = "AWS S3 Copy Transfer Service Selection";
    
    @Inject
    private TransferServiceSelectionStrategy transferServiceSelectionStrategy;
    
    @Override
    public String name() {
        return NAME;
    }
    
    @Provider
    public TransferServiceSelectionStrategy transferServiceSelectionStrategy() {
        return (request, transferServices) -> {
            var list = transferServices.toList();
            var service = list.stream()
                    .filter(ts -> !(ts instanceof PipelineService))
                    .findFirst();
            return service.orElse(this.transferServiceSelectionStrategy.chooseTransferService(request, list.stream()));
        };
    }
}
