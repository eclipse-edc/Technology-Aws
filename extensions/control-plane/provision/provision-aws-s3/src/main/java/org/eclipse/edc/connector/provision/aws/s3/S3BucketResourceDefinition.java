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
 *       Fraunhofer Institute for Software and Systems Engineering - add toBuilder method
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
                .bucketName(bucketName);
    }

    public String getEndpointOverride() {
        return endpointOverride;
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

        @Override
        protected void verify() {
            super.verify();
            Objects.requireNonNull(resourceDefinition.regionId, "regionId");
            Objects.requireNonNull(resourceDefinition.bucketName, "bucketName");
        }
    }

}
