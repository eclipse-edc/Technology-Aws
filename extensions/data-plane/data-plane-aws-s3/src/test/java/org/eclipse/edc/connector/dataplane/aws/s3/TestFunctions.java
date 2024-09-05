/*
 *  Copyright (c) 2022 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Bayerische Motoren Werke Aktiengesellschaft (BMW AG) - Initial implementation
 *
 */

package org.eclipse.edc.connector.dataplane.aws.s3;

import org.eclipse.edc.aws.s3.spi.S3BucketSchema;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;

import java.util.UUID;

public class TestFunctions {

    public static final String VALID_REGION = "validRegion";
    public static final String VALID_BUCKET_NAME = "validBucketName";
    public static final String VALID_KEY_NAME = "validKeyName";
    public static final String VALID_OBJECT_NAME = "validObjectName";
    public static final String VALID_ACCESS_KEY_ID = "validAccessKeyId";
    public static final String VALID_SECRET_ACCESS_KEY = "validSecretAccessKey";

    public static DataAddress s3DataAddressWithCredentials() {
        return DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .keyName(VALID_KEY_NAME)
                .property(S3BucketSchema.BUCKET_NAME, VALID_BUCKET_NAME)
                .property(S3BucketSchema.REGION, VALID_REGION)
                .property(S3BucketSchema.OBJECT_NAME, VALID_OBJECT_NAME)
                .property(S3BucketSchema.ACCESS_KEY_ID, VALID_ACCESS_KEY_ID)
                .property(S3BucketSchema.SECRET_ACCESS_KEY, VALID_SECRET_ACCESS_KEY)
                .build();
    }

    public static DataAddress s3DataAddressWithoutCredentials() {
        return DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .keyName(VALID_KEY_NAME)
                .property(S3BucketSchema.BUCKET_NAME, VALID_BUCKET_NAME)
                .property(S3BucketSchema.REGION, VALID_REGION)
                .property(S3BucketSchema.OBJECT_NAME, VALID_OBJECT_NAME)
                .build();
    }

    public static DataFlowStartMessage.Builder createRequest(String type) {
        return DataFlowStartMessage.Builder.newInstance()
                .id(UUID.randomUUID().toString())
                .processId(UUID.randomUUID().toString())
                .sourceDataAddress(createDataAddress(type).build())
                .destinationDataAddress(createDataAddress(type).build());
    }

    public static DataAddress.Builder createDataAddress(String type) {
        return DataAddress.Builder.newInstance().type(type);
    }

}
