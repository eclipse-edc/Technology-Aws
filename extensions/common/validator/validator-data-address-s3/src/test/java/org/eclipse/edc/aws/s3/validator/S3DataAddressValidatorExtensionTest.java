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

package org.eclipse.edc.aws.s3.validator;


import org.eclipse.edc.aws.s3.spi.S3BucketSchema;
import org.eclipse.edc.junit.extensions.DependencyInjectionExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.validator.spi.DataAddressValidatorRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(DependencyInjectionExtension.class)
public class S3DataAddressValidatorExtensionTest {

    private final DataAddressValidatorRegistry registry = mock();

    @BeforeEach
    void setup(ServiceExtensionContext context) {
        context.registerService(DataAddressValidatorRegistry.class, registry);
    }

    @Test
    void initialize(S3DataAddressValidatorExtension extension, ServiceExtensionContext context) {
        extension.initialize(context);
        assertThat(extension.name()).isEqualTo(S3DataAddressValidatorExtension.NAME);
        verify(registry).registerSourceValidator(eq(S3BucketSchema.TYPE), isA(S3SourceDataAddressValidator.class));
        verify(registry).registerDestinationValidator(eq(S3BucketSchema.TYPE), isA(S3DestinationDataAddressValidator.class));

    }
}
