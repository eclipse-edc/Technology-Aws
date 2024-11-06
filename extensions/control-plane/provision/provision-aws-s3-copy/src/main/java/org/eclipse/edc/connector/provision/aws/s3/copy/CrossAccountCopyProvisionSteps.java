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
