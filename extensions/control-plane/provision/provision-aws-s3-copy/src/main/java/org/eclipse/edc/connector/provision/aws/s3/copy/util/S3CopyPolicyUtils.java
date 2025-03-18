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

package org.eclipse.edc.connector.provision.aws.s3.copy.util;

import jakarta.json.Json;
import jakarta.json.JsonObject;

import static java.lang.String.format;

/**
 * Provides constants used in AWS policies as well as methods for generating IAM role and S3 bucket
 * policies required during the provisioning for S3-to-S3 copy transfers.
 */
public class S3CopyPolicyUtils {

    private S3CopyPolicyUtils() { }
    
    /** Key of the statement attribute in AWS policies. */
    public static final String STATEMENT_ATTRIBUTE = "Statement";
    
    /** Key of the SID attribute in S3 bucket policy statements. */
    public static final String BUCKET_POLICY_STATEMENT_SID_ATTRIBUTE = "Sid";
    
    private static final String VERSION_ATTRIBUTE = "Version";
    private static final String EFFECT_ATTRIBUTE = "Effect";
    private static final String PRINCIPAL_ATTRIBUTE = "Principal";
    private static final String ACTION_ATTRIBUTE = "Action";
    private static final String RESOURCE_ATTRIBUTE = "Resource";
    private static final String CONDITION_ATTRIBUTE = "Condition";
    private static final String AWS_ATTRIBUTE = "AWS";
    
    private static final String VERSION = "2012-10-17";
    private static final String EFFECT_ALLOW = "Allow";
    
    private static final String S3_BUCKET_ARN_TEMPLATE = "arn:aws:s3:::%s";
    private static final String S3_OBJECT_ARN_TEMPLATE = "arn:aws:s3:::%s/%s";
    private static final String S3_ALL_OBJECTS_ARN_TEMPLATE = "arn:aws:s3:::%s/*";
    
    private static final String PERMISSION_STS_ASSUME_ROLE = "sts:AssumeRole";
    private static final String PERMISSION_S3_LIST_BUCKET = "s3:ListBucket";
    private static final String PERMISSION_S3_GET_OBJECT = "s3:GetObject";
    private static final String PERMISSION_S3_GET_OBJECT_TAGGING = "s3:GetObjectTagging";
    private static final String PERMISSION_S3_GET_OBJECT_VERSION = "s3:GetObjectVersion";
    private static final String PERMISSION_S3_GET_OBJECT_VERSION_TAGGING = "s3:GetObjectVersionTagging";
    private static final String PERMISSION_S3_PUT_OBJECT = "s3:PutObject";
    private static final String PERMISSION_S3_PUT_OBJECT_ACL = "s3:PutObjectAcl";
    private static final String PERMISSION_S3_PUT_OBJECT_TAGGING = "s3:PutObjectTagging";
    
    /**
     * Generates the trust policy of an IAM role, allowing a given user to assume the role.
     *
     * @param userArn ARN of the user
     * @return the trust policy
     */
    public static JsonObject roleTrustPolicy(String userArn) {
        return Json.createObjectBuilder()
                .add(VERSION_ATTRIBUTE, VERSION)
                .add(STATEMENT_ATTRIBUTE, Json.createArrayBuilder()
                    .add(Json.createObjectBuilder()
                            .add(EFFECT_ATTRIBUTE, EFFECT_ALLOW)
                            .add(PRINCIPAL_ATTRIBUTE, Json.createObjectBuilder()
                                    .add(AWS_ATTRIBUTE, userArn)
                                    .build())
                            .add(ACTION_ATTRIBUTE, PERMISSION_STS_ASSUME_ROLE)
                            .add(CONDITION_ATTRIBUTE, Json.createObjectBuilder().build())
                        .build())
                    .build())
                .build();
    }
    
    /**
     * Generates the role policy of an IAM role for a cross-account copy of S3 objects. Grants
     * required permissions for reading the source object and writing the destination object.
     *
     * @param sourceBucket name of the source bucket
     * @param sourceObject name of the source object
     * @param destinationBucket name of the destination bucket
     * @param destinationObject name of the destination object
     * @return the role policy
     */
    public static JsonObject crossAccountRolePolicy(String sourceBucket, String sourceObject, String destinationBucket, String destinationObject) {
        return Json.createObjectBuilder()
                .add(VERSION_ATTRIBUTE, VERSION)
                .add(STATEMENT_ATTRIBUTE, Json.createArrayBuilder()
                        .add(Json.createObjectBuilder()
                                .add(EFFECT_ATTRIBUTE, EFFECT_ALLOW)
                                .add(ACTION_ATTRIBUTE, Json.createArrayBuilder()
                                        .add(PERMISSION_S3_LIST_BUCKET)
                                        .add(PERMISSION_S3_GET_OBJECT)
                                        .add(PERMISSION_S3_GET_OBJECT_TAGGING)
                                        .add(PERMISSION_S3_GET_OBJECT_VERSION)
                                        .add(PERMISSION_S3_GET_OBJECT_VERSION_TAGGING)
                                        .build())
                                .add(RESOURCE_ATTRIBUTE, Json.createArrayBuilder()
                                        .add(format(S3_BUCKET_ARN_TEMPLATE, sourceBucket))
                                        .add(format(S3_OBJECT_ARN_TEMPLATE, sourceBucket, sourceObject))
                                        .build())
                                .build())
                        .add(Json.createObjectBuilder()
                                .add(EFFECT_ATTRIBUTE, EFFECT_ALLOW)
                                .add(ACTION_ATTRIBUTE, Json.createArrayBuilder()
                                        .add(PERMISSION_S3_LIST_BUCKET)
                                        .add(PERMISSION_S3_PUT_OBJECT)
                                        .add(PERMISSION_S3_PUT_OBJECT_ACL)
                                        .add(PERMISSION_S3_PUT_OBJECT_TAGGING)
                                        .add(PERMISSION_S3_GET_OBJECT_TAGGING)
                                        .add(PERMISSION_S3_GET_OBJECT_VERSION)
                                        .add(PERMISSION_S3_GET_OBJECT_VERSION_TAGGING)
                                        .build())
                                .add(RESOURCE_ATTRIBUTE, Json.createArrayBuilder()
                                        .add(format(S3_BUCKET_ARN_TEMPLATE, destinationBucket))
                                        .add(format(S3_OBJECT_ARN_TEMPLATE, destinationBucket, destinationObject))
                                        .build())
                                .build())
                        .build())
                .build();
    }
    
    /**
     * Generates an empty S3 bucket policy, i.e. a policy without any statements.
     *
     * @return the empty bucket policy
     */
    public static JsonObject emptyBucketPolicy() {
        return Json.createObjectBuilder()
                .add(VERSION_ATTRIBUTE, VERSION)
                .add(STATEMENT_ATTRIBUTE, Json.createArrayBuilder()
                        .build())
                .build();
    }
    
    /**
     * Generates a statement for an S3 bucket policy, which allows a given role to write objects
     * into the bucket.
     *
     * @param sid the identifier of the statement
     * @param roleArn the role ARN
     * @param destinationBucket the destination bucket
     * @return the bucket policy statement
     */
    public static JsonObject bucketPolicyStatement(String sid, String roleArn, String destinationBucket) {
        return Json.createObjectBuilder()
                .add(BUCKET_POLICY_STATEMENT_SID_ATTRIBUTE, sid)
                .add(EFFECT_ATTRIBUTE, EFFECT_ALLOW)
                .add(PRINCIPAL_ATTRIBUTE, Json.createObjectBuilder()
                        .add(AWS_ATTRIBUTE, roleArn)
                        .build())
                .add(ACTION_ATTRIBUTE, Json.createArrayBuilder()
                        .add(PERMISSION_S3_LIST_BUCKET)
                        .add(PERMISSION_S3_PUT_OBJECT)
                        .add(PERMISSION_S3_PUT_OBJECT_ACL)
                        .add(PERMISSION_S3_PUT_OBJECT_TAGGING)
                        .add(PERMISSION_S3_GET_OBJECT_TAGGING)
                        .add(PERMISSION_S3_GET_OBJECT_VERSION)
                        .add(PERMISSION_S3_GET_OBJECT_VERSION_TAGGING)
                        .build())
                .add(RESOURCE_ATTRIBUTE, Json.createArrayBuilder()
                        .add(format(S3_BUCKET_ARN_TEMPLATE, destinationBucket))
                        .add(format(S3_ALL_OBJECTS_ARN_TEMPLATE, destinationBucket))
                        .build())
                .build();
    }
}
