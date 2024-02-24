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

import org.eclipse.edc.connector.api.client.spi.transferprocess.TransferProcessApiClient;
import org.eclipse.edc.connector.dataplane.spi.pipeline.PipelineService;
import org.eclipse.edc.junit.extensions.EdcExtension;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;

@ExtendWith(EdcExtension.class)
class DataPlaneS3ExtensionTest {

    @BeforeEach
    void setup(EdcExtension extension) {
        extension.registerServiceMock(TransferProcessApiClient.class, mock(TransferProcessApiClient.class));
    }

    @Test
    void shouldProvidePipelineServices(PipelineService pipelineService) {
        var request = DataFlowStartMessage.Builder.newInstance()
                .processId(UUID.randomUUID().toString())
                .sourceDataAddress(TestFunctions.s3DataAddressWithCredentials())
                .destinationDataAddress(TestFunctions.s3DataAddressWithCredentials())
                .build();

        var result = pipelineService.validate(request);

        assertThat(result.succeeded()).isTrue();
    }
}