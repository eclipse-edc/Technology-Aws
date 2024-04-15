/*
 *  Copyright (c) 2020, 2021 Microsoft Corporation
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Microsoft Corporation - initial API and implementation
 *
 */

package org.eclipse.edc.aws.s3;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.SecretToken;

import java.util.Objects;

public class AwsSecretToken implements SecretToken {
    private final String accessKeyId;
    private final String secretAccessKey;

    public AwsSecretToken(@JsonProperty("accessKeyId") String accessKeyId, @JsonProperty("secretAccessKey") String secretAccessKey) {
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
    }

    @Override
    public long getExpiration() {
        return 0;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AwsSecretToken that = (AwsSecretToken) o;
        return Objects.equals(accessKeyId, that.accessKeyId) && Objects.equals(secretAccessKey, that.secretAccessKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accessKeyId, secretAccessKey);
    }
}
