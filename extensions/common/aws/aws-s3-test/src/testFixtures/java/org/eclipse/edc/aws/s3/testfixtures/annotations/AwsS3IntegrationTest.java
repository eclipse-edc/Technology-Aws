/*
 *  Copyright (c) 2022 Microsoft Corporation
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

package org.eclipse.edc.aws.s3.testfixtures.annotations;

import org.eclipse.edc.junit.annotations.IntegrationTest;
import org.junit.jupiter.api.Tag;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for AWS S3 integration testing.  It applies specific Junit Tag.
 *
 * @deprecated not used anymore, please use {@link org.eclipse.edc.junit.annotations.EndToEndTest}.
 */
@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
@IntegrationTest
@Tag("AwsS3IntegrationTest")
@Deprecated(since = "0.7.1")
public @interface AwsS3IntegrationTest {
}
