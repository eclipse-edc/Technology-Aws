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

import software.amazon.awssdk.services.iam.model.Role;

/**
 * DTO for passing information about already provisioned resources between the steps of the
 * {@link S3CopyProvisionPipeline}.
 */
public class S3CopyProvisionSteps {
    
    private Role role;
    private String bucketPolicy;
    
    public S3CopyProvisionSteps(Role role) {
        this.role = role;
    }
    
    public Role getRole() {
        return role;
    }
    
    public void setRole(Role role) {
        this.role = role;
    }
    
    public String getBucketPolicy() {
        return bucketPolicy;
    }
    
    public void setBucketPolicy(String bucketPolicy) {
        this.bucketPolicy = bucketPolicy;
    }
}
