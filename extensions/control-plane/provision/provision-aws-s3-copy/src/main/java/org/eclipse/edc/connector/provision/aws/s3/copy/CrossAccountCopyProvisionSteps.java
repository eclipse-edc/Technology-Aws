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

import software.amazon.awssdk.services.iam.model.Role;

public class CrossAccountCopyProvisionSteps {
    
    private Role role;
    private String bucketPolicy;
    
    public CrossAccountCopyProvisionSteps(Role role) {
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
