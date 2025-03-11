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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class S3CopyProvisionedResourceTest {
    
    @Test
    void serialize_deserialize_shouldBeEqual() throws JsonProcessingException {
        var resource = S3CopyProvisionedResource.Builder.newInstance()
                .id("test")
                .resourceDefinitionId("test")
                .transferProcessId("tp-id")
                .resourceName("test")
                .destinationRegion("region")
                .destinationBucketName("bucket")
                .destinationKeyName("destination-key-name")
                .bucketPolicyStatementSid("sid")
                .sourceAccountRoleName("roleName")
                .build();
        var mapper = new JacksonTypeManager().getMapper();
    
        var serialized = mapper.writeValueAsString(resource);
        var deserialized = mapper.readValue(serialized, S3CopyProvisionedResource.class);
    
        assertThat(deserialized).usingRecursiveComparison().isEqualTo(resource);
    }
    
}
