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
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.validator.spi.DataAddressValidatorRegistry;

import static org.eclipse.edc.aws.s3.validator.S3DataAddressValidatorExtension.NAME;

@Extension(NAME)
public class S3DataAddressValidatorExtension implements ServiceExtension {
    public static final String NAME = "DataAddress S3 Validator";

    @Inject
    private DataAddressValidatorRegistry validatorRegistry;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void initialize(ServiceExtensionContext context) {
        var sourceValidator = new S3SourceDataAddressValidator();
        var destinationValidator = new S3DestinationDataAddressValidator();
        validatorRegistry.registerSourceValidator(S3BucketSchema.TYPE, sourceValidator);
        validatorRegistry.registerDestinationValidator(S3BucketSchema.TYPE, destinationValidator);
    }
}
