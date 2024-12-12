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

package org.eclipse.edc.connector.provision.aws.s3.copy;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.ResourceDefinition;
import org.eclipse.edc.spi.types.domain.DataAddress;

import java.util.Objects;

@JsonDeserialize(builder = S3CopyResourceDefinition.Builder.class)
public class S3CopyResourceDefinition extends ResourceDefinition {
    
    private String endpointOverride;
    private String destinationRegion;
    private String destinationBucketName;
    private String destinationKeyName;
    private String bucketPolicyStatementSid;
    private DataAddress sourceDataAddress;
    
    public String getEndpointOverride() {
        return endpointOverride;
    }
    
    public String getDestinationRegion() {
        return destinationRegion;
    }
    
    public String getDestinationBucketName() {
        return destinationBucketName;
    }
    
    public String getDestinationKeyName() {
        return destinationKeyName;
    }
    
    public String getBucketPolicyStatementSid() {
        return bucketPolicyStatementSid;
    }
    
    public DataAddress getSourceDataAddress() {
        return sourceDataAddress;
    }
    
    @Override
    public Builder toBuilder() {
        return initializeBuilder(new Builder()
                .endpointOverride(endpointOverride)
                .destinationRegion(destinationRegion)
                .destinationBucketName(destinationBucketName)
                .destinationKeyName(destinationKeyName)
                .bucketPolicyStatementSid(bucketPolicyStatementSid)
                .sourceDataAddress(sourceDataAddress));
    }
    
    @JsonPOJOBuilder(withPrefix = "")
    public static class Builder extends ResourceDefinition.Builder<S3CopyResourceDefinition, Builder> {
        
        private Builder() {
            super(new S3CopyResourceDefinition());
        }
        
        public static Builder newInstance() {
            return new Builder();
        }
        
        public Builder endpointOverride(String endpointOverride) {
            resourceDefinition.endpointOverride = endpointOverride;
            return this;
        }
        
        public Builder destinationRegion(String destinationRegion) {
            resourceDefinition.destinationRegion = destinationRegion;
            return this;
        }
        
        public Builder destinationBucketName(String destinationBucketName) {
            resourceDefinition.destinationBucketName = destinationBucketName;
            return this;
        }
        
        public Builder destinationKeyName(String destinationKeyName) {
            resourceDefinition.destinationKeyName = destinationKeyName;
            return this;
        }
        
        public Builder bucketPolicyStatementSid(String bucketPolicyStatementSid) {
            resourceDefinition.bucketPolicyStatementSid = bucketPolicyStatementSid;
            return this;
        }
        
        public Builder sourceDataAddress(DataAddress dataAddress) {
            resourceDefinition.sourceDataAddress = dataAddress;
            return this;
        }
        
        @Override
        protected void verify() {
            super.verify();
            Objects.requireNonNull(resourceDefinition.destinationRegion, "destinationRegion");
            Objects.requireNonNull(resourceDefinition.destinationBucketName, "destinationBucketName");
            Objects.requireNonNull(resourceDefinition.destinationKeyName, "destinationKeyName");
            Objects.requireNonNull(resourceDefinition.bucketPolicyStatementSid, "bucketPolicyStatementSid");
            Objects.requireNonNull(resourceDefinition.sourceDataAddress, "dataAddress");
        }
    }
}
