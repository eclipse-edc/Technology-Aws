/*
 *  Copyright (c) 2022 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
 *
 */

package org.eclipse.edc.connector.dataplane.aws.s3;

import org.eclipse.edc.aws.s3.spi.S3BucketSchema;
import org.eclipse.edc.connector.dataplane.spi.iam.PublicEndpointGeneratorService;
import org.eclipse.edc.connector.dataplane.spi.pipeline.PipelineService;
import org.eclipse.edc.junit.extensions.DependencyInjectionExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.function.Function;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(DependencyInjectionExtension.class)
class DataPlaneS3ExtensionTest {

    private final PipelineService pipelineService = mock();
    private final PublicEndpointGeneratorService generatorService = mock();

    @BeforeEach
    void setup(ServiceExtensionContext context) {
        context.registerService(PipelineService.class, pipelineService);
        context.registerService(PublicEndpointGeneratorService.class, generatorService);
    }

    @Test
    void shouldProvidePipelineServices(DataPlaneS3Extension extension, ServiceExtensionContext context) {
        extension.initialize(context);

        verify(pipelineService).registerFactory(isA(S3DataSinkFactory.class));
        verify(pipelineService).registerFactory(isA(S3DataSourceFactory.class));
    }

    @Test
    void shouldInvokePublicEndpointGeneratorService(DataPlaneS3Extension extension, ServiceExtensionContext context) {
        extension.initialize(context);

        verify(generatorService).addGeneratorFunction(eq(S3BucketSchema.TYPE), isA(Function.class));
    }
}
