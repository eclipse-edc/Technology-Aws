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

package org.eclipse.edc.connector.provision.aws.s3.copy.util;

public class S3CopyConstants {
    
    private S3CopyConstants() {}
    
    public static final String S3_BUCKET_POLICY_STATEMENT  = "Statement";
    
    public static final String S3_BUCKET_POLICY_STATEMENT_SID  = "Sid";
    
    public static final String PLACEHOLDER_USER_ARN  = "{{user-arn}}";
    public static final String PLACEHOLDER_SOURCE_BUCKET  = "{{source-bucket}}";
    public static final String PLACEHOLDER_DESTINATION_BUCKET  = "{{destination-bucket}}";
    public static final String PLACEHOLDER_STATEMENT_SID  = "{{sid}}";
    public static final String PLACEHOLDER_ROLE_ARN  = "{{role-arn}}";
    
}
