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
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public class S3ClientRequest {

    private final String region;

    private final String endpointOverride;

    private final SecretToken secretToken;

    private S3ClientRequest(@NotNull String region, String endpointOverride, SecretToken secretToken) {
        this.region = region;
        this.endpointOverride = endpointOverride;
        this.secretToken = secretToken;
    }

    public static S3ClientRequest from(String region, String endpointOverride) {
        return new S3ClientRequest(region, endpointOverride, null);
    }

    public static S3ClientRequest from(String region, String endpointOverride, SecretToken secretToken) {
        return new S3ClientRequest(region, endpointOverride, secretToken);
    }

    public String getRegion() {
        return region;
    }

    public String getEndpointOverride() {
        return endpointOverride;
    }

    public SecretToken getSecretToken() {
        return secretToken;
    }

    @Override
    public String toString() {
        return "S3ClientRequest{" +
            "region='" + region + '\'' +
            ", endpointOverride='" + endpointOverride + '\'' +
            ", secretToken=" + secretToken +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        S3ClientRequest that = (S3ClientRequest) o;
        return Objects.equals(region, that.region) && Objects.equals(endpointOverride, that.endpointOverride) &&
            Objects.equals(secretToken, that.secretToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(region, endpointOverride, secretToken);
    }
}
