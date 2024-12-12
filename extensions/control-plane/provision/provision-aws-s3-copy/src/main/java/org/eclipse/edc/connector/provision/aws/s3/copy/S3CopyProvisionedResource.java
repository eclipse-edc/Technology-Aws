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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.eclipse.edc.aws.s3.spi.S3BucketSchema;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.ProvisionedContentResource;
import software.amazon.awssdk.services.iam.model.Role;

@JsonDeserialize(builder = S3CopyProvisionedResource.Builder.class)
public class S3CopyProvisionedResource extends ProvisionedContentResource {
    
    private String sourceAccountRoleName;
    private String destinationRegion;
    private String destinationBucketName;
    private String destinationKeyName;
    private String bucketPolicyStatementSid;
    private String endpointOverride;
    
    public String getSourceAccountRoleName() {
        return sourceAccountRoleName;
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
    
    public String getEndpointOverride() {
        return endpointOverride;
    }
    
    @JsonPOJOBuilder(withPrefix = "")
    public static class Builder extends ProvisionedContentResource.Builder<S3CopyProvisionedResource, Builder> {
        
        private Builder() {
            super(new S3CopyProvisionedResource());
            dataAddressBuilder.type(S3BucketSchema.TYPE);
        }
        
        @JsonCreator
        public static Builder newInstance() {
            return new Builder();
        }
        
        public Builder sourceAccountRoleName(String roleName) {
            provisionedResource.sourceAccountRoleName = roleName;
            return this;
        }
        
        public Builder destinationRegion(String region) {
            provisionedResource.destinationRegion = region;
            return this;
        }
        
        public Builder destinationBucketName(String bucketName) {
            provisionedResource.destinationBucketName = bucketName;
            return this;
        }
        
        public Builder destinationKeyName(String destinationKeyName) {
            provisionedResource.destinationKeyName = destinationKeyName;
            return this;
        }
        
        public Builder bucketPolicyStatementSid(String statementSid) {
            provisionedResource.bucketPolicyStatementSid = statementSid;
            return this;
        }
        
        public Builder endpointOverride(String endpointOverride) {
            provisionedResource.endpointOverride = endpointOverride;
            return this;
        }
    }
}
