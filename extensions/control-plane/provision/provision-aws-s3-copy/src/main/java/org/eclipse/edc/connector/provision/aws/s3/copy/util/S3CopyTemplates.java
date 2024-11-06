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

public class S3CopyTemplates {

    private S3CopyTemplates() { }

    public static final String CROSS_ACCOUNT_ROLE_TRUST_POLICY_TEMPLATE = "{\n" +
            "    \"Version\": \"2012-10-17\",\n" +
            "    \"Statement\": [\n" +
            "        {\n" +
            "            \"Effect\": \"Allow\",\n" +
            "            \"Principal\": {\n" +
            "                \"AWS\": \"{{user-arn}}\"\n" +
            "            },\n" +
            "            \"Action\": \"sts:AssumeRole\",\n" +
            "            \"Condition\": {}\n" +
            "        }\n" +
            "    ]\n" +
            "}";

    //TODO restrict to specific files: one vs multiple files
    public static final String CROSS_ACCOUNT_ROLE_POLICY_TEMPLATE = "{\n" +
            "    \"Version\": \"2012-10-17\",\n" +
            "    \"Statement\": [\n" +
            "        {\n" +
            "            \"Effect\": \"Allow\",\n" +
            "            \"Action\": [\n" +
            "                \"s3:ListBucket\",\n" +
            "                \"s3:GetObject\",\n" +
            "                \"s3:GetObjectTagging\",\n" +
            "                \"s3:GetObjectVersion\",\n" +
            "                \"s3:GetObjectVersionTagging\"\n" +
            "            ],\n" +
            "            \"Resource\": [\n" +
            "                \"arn:aws:s3:::{{source-bucket}}\",\n" +
            "                \"arn:aws:s3:::{{source-bucket}}/*\"\n" +
            "            ]\n" +
            "        },\n" +
            "        {\n" +
            "            \"Effect\": \"Allow\",\n" +
            "            \"Action\": [\n" +
            "                \"s3:ListBucket\",\n" +
            "                \"s3:PutObject\",\n" +
            "                \"s3:PutObjectAcl\",\n" +
            "                \"s3:PutObjectTagging\",\n" +
            "                \"s3:GetObjectTagging\",\n" +
            "                \"s3:GetObjectVersion\",\n" +
            "                \"s3:GetObjectVersionTagging\"\n" +
            "            ],\n" +
            "            \"Resource\": [\n" +
            "                \"arn:aws:s3:::{{destination-bucket}}\",\n" +
            "                \"arn:aws:s3:::{{destination-bucket}}/*\"\n" +
            "            ]\n" +
            "        }\n" +
            "    ]\n" +
            "}";

    public static final String EMPTY_BUCKET_POLICY = "{\n" +
            "    \"Version\": \"2012-10-17\",\n" +
            "    \"Statement\": [\n" +
            "        \n" +
            "    ]\n" +
            "}";

    //TODO for now access to all files in the bucket, should be limited to the files to transfer: one vs multiple files
    public static final String BUCKET_POLICY_STATEMENT_TEMPLATE = " {\n" +
            "            \"Sid\": \"{{sid}}\",\n" +
            "            \"Effect\": \"Allow\",\n" +
            "            \"Principal\": {\n" +
            "                \"AWS\": \"{{source-account-role-arn}}\"\n" +
            "            },\n" +
            "            \"Action\": [\n" +
            "                \"s3:ListBucket\",\n" +
            "                \"s3:PutObject\",\n" +
            "                \"s3:PutObjectAcl\",\n" +
            "                \"s3:PutObjectTagging\",\n" +
            "                \"s3:GetObjectTagging\",\n" +
            "                \"s3:GetObjectVersion\",\n" +
            "                \"s3:GetObjectVersionTagging\"\n" +
            "            ],\n" +
            "            \"Resource\": [\n" +
            "                \"arn:aws:s3:::{{sink-bucket-name}}\",\n" +
            "                \"arn:aws:s3:::{{sink-bucket-name}}/*\"\n" +
            "            ]\n" +
            "        }";

}
