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
import org.eclipse.edc.validator.spi.ValidationFailure;
import org.eclipse.edc.validator.spi.Violation;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.edc.aws.s3.S3BucketSchema.BUCKET_NAME;
import static org.eclipse.edc.aws.s3.S3BucketSchema.REGION;
import static org.eclipse.edc.aws.s3.S3BucketSchema.TYPE;
import static org.eclipse.edc.junit.assertions.AbstractResultAssert.assertThat;

public class S3DestinationDataAddressValidatorTest {

    private final S3DestinationDataAddressValidator validator = new S3DestinationDataAddressValidator();

    @Test
    void should_pass_when_data_address_is_valid() {
        var dataAddress = DataAddress.Builder.newInstance()
                .type(TYPE)
                .property(BUCKET_NAME, "bucketName")
                .property(REGION, "region")
                .build();

        var result = validator.validate(dataAddress);

        assertThat(result).isSucceeded();
    }

    @Test
    void should_fail_when_required_fields_are_missing() {
        var dataAddress = DataAddress.Builder.newInstance()
                .type(TYPE)
                .build();

        var result = validator.validate(dataAddress);

        assertThat(result).isFailed()
                .extracting(ValidationFailure::getViolations)
                .satisfies(violations -> assertThat(violations)
                        .extracting(Violation::path)
                        .containsExactlyInAnyOrder(BUCKET_NAME, REGION));
    }
}
