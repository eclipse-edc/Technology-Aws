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

package org.eclipse.edc.aws.s3.validator;

import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.validator.spi.ValidationFailure;
import org.eclipse.edc.validator.spi.Violation;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.BUCKET_NAME;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.OBJECT_NAME;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.OBJECT_PREFIX;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.REGION;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.TYPE;
import static org.eclipse.edc.junit.assertions.AbstractResultAssert.assertThat;

public class S3SourceDataAddressValidatorTest {

    private final S3SourceDataAddressValidator validator = new S3SourceDataAddressValidator();

    @Test
    void should_pass_when_data_address_is_valid_with_object_name() {
        var dataAddress = DataAddress.Builder.newInstance()
                .type(TYPE)
                .property(BUCKET_NAME, "bucketName")
                .property(REGION, "region")
                .property(OBJECT_NAME, "objectName")
                .build();

        var result = validator.validate(dataAddress);

        assertThat(result).isSucceeded();
    }

    @Test
    void should_pass_when_data_address_is_valid_with_object_prefix() {
        var dataAddress = DataAddress.Builder.newInstance()
                .type(TYPE)
                .property(BUCKET_NAME, "bucketName")
                .property(REGION, "region")
                .property(OBJECT_PREFIX, "objectPrefix")
                .build();

        var result = validator.validate(dataAddress);

        assertThat(result).isSucceeded();
    }

    @Test
    void should_fail_when_both_object_name_and_prefix_are_missing() {
        var dataAddress = DataAddress.Builder.newInstance()
                .type(TYPE)
                .property(BUCKET_NAME, "bucketName")
                .property(REGION, "region")
                .build();

        var result = validator.validate(dataAddress);

        assertThat(result).isFailed()
                .extracting(ValidationFailure::getViolations)
                .satisfies(violations -> assertThat(violations)
                        .extracting(Violation::path)
                        .contains(OBJECT_NAME));
    }

    @Test
    void should_fail_when_region_or_bucketName_is_missing() {
        var dataAddress = DataAddress.Builder.newInstance()
                .type(TYPE)
                .property(OBJECT_NAME, "objectName")
                .build();

        var result = validator.validate(dataAddress);

        assertThat(result).isFailed()
                .extracting(ValidationFailure::getViolations)
                .satisfies(violations -> assertThat(violations)
                        .extracting(Violation::path)
                        .containsExactlyInAnyOrder(BUCKET_NAME, REGION));
    }
}
