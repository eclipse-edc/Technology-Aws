/*
 *  Copyright (c) 2020, 2021 Microsoft Corporation
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Microsoft Corporation - initial API and implementation
 *       Fraunhofer-Gesellschaft zur Förderung der angewandten Forschung e.V. - add toBuilder method
 *
 */

package org.eclipse.edc.connector.provision.aws.s3;


import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.ResourceDefinition;

import java.util.Objects;

/**
 * An S3 bucket and access credentials to be provisioned.
 */
@JsonDeserialize(builder = S3BucketResourceDefinition.Builder.class)
public class S3BucketResourceDefinition extends ResourceDefinition {
    private String regionId;
    private String bucketName;
    private String endpointOverride;
    private String accessKeyId;
    private String objectName;

    private S3BucketResourceDefinition() {
    }

    public String getRegionId() {
        return regionId;
    }

    public String getBucketName() {
        return bucketName;
    }

    @Override
    public Builder toBuilder() {
        return initializeBuilder(new Builder())
                .regionId(regionId)
                .bucketName(bucketName)
                .objectName(objectName)
                .accessKeyId(accessKeyId);
    }

    public String getEndpointOverride() {
        return endpointOverride;
    }

    public String getObjectName() {
        return objectName;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    @JsonPOJOBuilder(withPrefix = "")
    public static class Builder extends ResourceDefinition.Builder<S3BucketResourceDefinition, Builder> {

        private Builder() {
            super(new S3BucketResourceDefinition());
        }

        public static Builder newInstance() {
            return new Builder();
        }

        public Builder regionId(String regionId) {
            resourceDefinition.regionId = regionId;
            return this;
        }

        public Builder bucketName(String bucketName) {
            resourceDefinition.bucketName = bucketName;
            return this;
        }

        public Builder endpointOverride(String endpointOverride) {
            resourceDefinition.endpointOverride = endpointOverride;
            return this;
        }

        public Builder accessKeyId(String accessKeyId) {
            resourceDefinition.accessKeyId = accessKeyId;
            return this;
        }

        public Builder objectName(String objectName) {
            resourceDefinition.objectName = objectName;
            return this;
        }

        @Override
        protected void verify() {
            super.verify();
            Objects.requireNonNull(resourceDefinition.regionId, "regionId");
            Objects.requireNonNull(resourceDefinition.bucketName, "bucketName");
        }
    }

}
