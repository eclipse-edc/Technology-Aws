/*
 *  Copyright (c) 2023 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       ZF Friedrichshafen AG - initial implementation
 *
 */

package org.eclipse.edc.aws.s3;

import org.eclipse.edc.connector.transfer.spi.types.SecretToken;

public record S3ClientRequest(String region, String endpointOverride, SecretToken secretToken) {

    public static S3ClientRequest from(String region, String endpointOverride) {
        return new S3ClientRequest(region, endpointOverride, null);
    }

    public static S3ClientRequest from(String region, String endpointOverride, SecretToken secretToken) {
        return new S3ClientRequest(region, endpointOverride, secretToken);
    }

}
