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

package org.eclipse.edc.connector.provision.aws.s3.copy;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.eclipse.edc.json.JacksonTypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class S3CopyResourceDefinitionTest {
    
    @Test
    void toBuilder_shouldBeEqual() {
        var definition = definition();
        
        var builder = definition.toBuilder();
        var rebuilt = builder.build();
        
        assertThat(rebuilt).usingRecursiveComparison().isEqualTo(definition);
    }
    
    @Test
    void serialize_deserialize_shouldBeEqual() throws JsonProcessingException {
        var definition = definition();
        var mapper = new JacksonTypeManager().getMapper();
        
        var serialized = mapper.writeValueAsString(definition);
        var deserialized = mapper.readValue(serialized, S3CopyResourceDefinition.class);
        
        assertThat(deserialized).usingRecursiveComparison().isEqualTo(definition);
    }
    
    private S3CopyResourceDefinition definition() {
        return S3CopyResourceDefinition.Builder.newInstance()
                .id("test")
                .transferProcessId("tp-id")
                .destinationRegion("region")
                .destinationBucketName("bucket")
                .destinationObjectName("object")
                .destinationKeyName("key")
                .bucketPolicyStatementSid("sid")
                .sourceDataAddress(DataAddress.Builder.newInstance()
                        .type("AmazonS3")
                        .build())
                .build();
    }
}
