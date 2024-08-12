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

import org.eclipse.edc.aws.s3.AwsClientProvider;
import org.eclipse.edc.aws.s3.spi.S3BucketSchema;
import org.eclipse.edc.connector.dataplane.spi.Endpoint;
import org.eclipse.edc.connector.dataplane.spi.iam.PublicEndpointGeneratorService;
import org.eclipse.edc.connector.dataplane.spi.pipeline.PipelineService;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.runtime.metamodel.annotation.Setting;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.validator.spi.DataAddressValidatorRegistry;

import java.util.concurrent.Executors;

@Extension(value = DataPlaneS3Extension.NAME)
public class DataPlaneS3Extension implements ServiceExtension {

    public static final String NAME = "Data Plane S3 Storage";

    private static final int DEFAULT_CHUNK_SIZE_IN_MB = 500; // 500MB chunk size

    @Setting(value = "The maximum chunk of stream to be read, in mb", defaultValue = DEFAULT_CHUNK_SIZE_IN_MB + "", type = "int")
    private static final String EDC_DATAPLANE_S3_SINK_CHUNK_SIZE_MB = "edc.dataplane.aws.sink.chunk.size.mb";

    @Setting(value = "Base url of the public API endpoint without the trailing slash.")
    private static final String PUBLIC_ENDPOINT = "edc.dataplane.api.public.baseurl";

    @Inject
    private PipelineService pipelineService;

    @Inject
    private AwsClientProvider awsClientProvider;

    @Inject
    private Vault vault;

    @Inject
    private TypeManager typeManager;

    @Inject
    private DataAddressValidatorRegistry validator;

    @Inject
    private PublicEndpointGeneratorService generatorService;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void initialize(ServiceExtensionContext context) {
        var executorService = Executors.newFixedThreadPool(10); // TODO make configurable

        var chunkSizeInMb = context.getSetting(EDC_DATAPLANE_S3_SINK_CHUNK_SIZE_MB, DEFAULT_CHUNK_SIZE_IN_MB);
        var chunkSizeInBytes = 1024 * 1024 * chunkSizeInMb;
        if (chunkSizeInBytes < 1) {
            throw new IllegalArgumentException("Chunk size must be greater than zero! Actual value is: " + chunkSizeInBytes);
        }
        var monitor = context.getMonitor();

        var endpoint = Endpoint.url(context.getSetting(PUBLIC_ENDPOINT, null));
        generatorService.addGeneratorFunction(S3BucketSchema.TYPE, dataAddress -> endpoint);

        var sourceFactory = new S3DataSourceFactory(awsClientProvider, monitor, vault, typeManager, validator);
        pipelineService.registerFactory(sourceFactory);

        var sinkFactory = new S3DataSinkFactory(awsClientProvider, executorService, monitor, vault, typeManager, chunkSizeInBytes, validator);
        pipelineService.registerFactory(sinkFactory);
    }

}
