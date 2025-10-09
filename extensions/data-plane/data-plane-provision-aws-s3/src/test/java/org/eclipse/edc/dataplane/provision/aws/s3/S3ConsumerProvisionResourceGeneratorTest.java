/*
 *  Copyright (c) 2025 Think-it GmbH
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Think-it GmbH - initial API and implementation
 *
 */

package org.eclipse.edc.dataplane.provision.aws.s3;

import org.eclipse.edc.aws.s3.spi.S3BucketSchema;
import org.eclipse.edc.connector.dataplane.spi.DataFlow;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;


public class S3ConsumerProvisionResourceGeneratorTest {

    private final S3ConsumerProvisionResourceGenerator generator = new S3ConsumerProvisionResourceGenerator();

    @Test
    void shouldGenerateProvisionResource() {
        var destination = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE)
                .property(S3BucketSchema.BUCKET_NAME, "test-name")
                .property(S3BucketSchema.REGION, Region.EU_WEST_2.id())
                .build();
        var dataFlow = DataFlow.Builder.newInstance()
                .destination(destination)
                .assetId(UUID.randomUUID().toString())
                .build();

        var resource = generator.generate(dataFlow);

        assertThat(resource.getId()).satisfies(UUID::fromString);
        assertThat(resource.getDataAddress()).isEqualTo(destination);
    }

}
