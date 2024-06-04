/*
 *  Copyright (c) 2022 Fraunhofer Institute for Software and Systems Engineering
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Fraunhofer Institute for Software and Systems Engineering - initial API and implementation
 *
 */

package org.eclipse.edc.connector.provision.aws.s3;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.eclipse.edc.json.JacksonTypeManager;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class S3BucketResourceDefinitionTest {

    @Test
    void toBuilder_verifyEqualResourceDefinition() {
        var definition = S3BucketResourceDefinition.Builder.newInstance()
                .id("id")
                .transferProcessId("tp-id")
                .regionId("region")
                .bucketName("bucket")
                .build();
        var builder = definition.toBuilder();
        var rebuiltDefinition = builder.build();

        assertThat(rebuiltDefinition).usingRecursiveComparison().isEqualTo(definition);
    }

    @Test
    void deserialization() throws JsonProcessingException {
        var objectMapper = new JacksonTypeManager().getMapper();

        var definition = S3BucketResourceDefinition.Builder.newInstance()
                .id("resourceDefinitionId")
                .transferProcessId("transferProcessId")
                .regionId("regionId")
                .bucketName("bucketName")
                .build();

        var json = objectMapper.writeValueAsString(definition);

        var deserialized = objectMapper.readValue(json, S3BucketResourceDefinition.class);

        assertThat(deserialized).usingRecursiveComparison().isEqualTo(definition);
    }
}
