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
 *       Bayerische Motoren Werke Aktiengesellschaft (BMW AG) - Initial implementation
 *
 */

package org.eclipse.edc.aws.s3.validation;

import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.validator.spi.ValidationResult;
import org.eclipse.edc.validator.spi.Validator;

import java.util.Objects;
import java.util.stream.Stream;

import static org.eclipse.edc.aws.s3.S3BucketSchema.ACCESS_KEY_ID;
import static org.eclipse.edc.aws.s3.S3BucketSchema.SECRET_ACCESS_KEY;
import static org.eclipse.edc.validator.spi.Violation.violation;

/**
 * Validator for AmazonS3 DataAddress type when credentials are in the data address (Optional)
 */
public class S3DataAddressCredentialsValidator implements Validator<DataAddress> {
    @Override
    public ValidationResult validate(DataAddress dataAddress) {
        var violations = Stream.of(ACCESS_KEY_ID, SECRET_ACCESS_KEY)
                .map(it -> {
                    var value = dataAddress.getStringProperty(it);
                    if (value == null || value.isBlank()) {
                        return violation("'%s' is a mandatory attribute".formatted(it), it, value);
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .toList();

        if (violations.isEmpty()) {
            return ValidationResult.success();
        }

        return ValidationResult.failure(violations);
    }
}
