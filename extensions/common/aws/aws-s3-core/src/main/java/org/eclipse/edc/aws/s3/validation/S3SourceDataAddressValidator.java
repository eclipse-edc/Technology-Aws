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
 *       Bayerische Motoren Werke Aktiengesellschaft (BMW AG) - Initial implementation
 *
 */

package org.eclipse.edc.aws.s3.validation;

import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.validator.spi.ValidationResult;
import org.eclipse.edc.validator.spi.Validator;
import org.eclipse.edc.validator.spi.Violation;

import java.util.ArrayList;
import java.util.stream.Stream;

import static org.eclipse.edc.aws.s3.S3BucketSchema.BUCKET_NAME;
import static org.eclipse.edc.aws.s3.S3BucketSchema.KEY_NAME;
import static org.eclipse.edc.aws.s3.S3BucketSchema.KEY_PREFIX;
import static org.eclipse.edc.aws.s3.S3BucketSchema.OBJECT_NAME;
import static org.eclipse.edc.aws.s3.S3BucketSchema.OBJECT_PREFIX;
import static org.eclipse.edc.aws.s3.S3BucketSchema.REGION;
import static org.eclipse.edc.validator.spi.Violation.violation;

/**
 * Validator for AmazonS3 DataAddress type
 */
public class S3SourceDataAddressValidator implements Validator<DataAddress> {

    @Override
    public ValidationResult validate(DataAddress dataAddress) {
        var violations = new ArrayList<Violation>();

        Stream.of(BUCKET_NAME, REGION).forEach(it -> {
            var value = dataAddress.getStringProperty(it);
            if (value == null || value.isBlank()) {
                violations.add(violation("'%s' is a mandatory attribute".formatted(it), it, value));
            }
        });

        if (Stream.of(OBJECT_NAME, KEY_NAME, OBJECT_PREFIX, KEY_PREFIX)
                .map(dataAddress::getStringProperty)
                .allMatch(it -> (it == null || it.isBlank()))) {
            violations.add(violation("Either the '%s' or '%s' attribute must be provided."
                    .formatted(OBJECT_NAME, OBJECT_PREFIX), OBJECT_NAME, null));
        }

        if (violations.isEmpty()) {
            return ValidationResult.success();
        }

        return ValidationResult.failure(violations);
    }

}
