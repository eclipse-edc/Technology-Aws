/*
 *  Copyright (c) 2024 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
 *
 */

package org.eclipse.edc.connector.dataplane.aws.s3.exceptions;

import org.eclipse.edc.spi.EdcException;

public class S3DataSourceException extends EdcException {

    public S3DataSourceException(String message) {
        super(message);
    }

    public S3DataSourceException(String message, Throwable cause) {
        super(message, cause);
    }

    public S3DataSourceException(Throwable cause) {
        super(cause);
    }

    public S3DataSourceException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
