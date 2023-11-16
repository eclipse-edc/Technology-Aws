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

plugins {
    `java-library`
}

dependencies {
    api(libs.edc.spi.transfer)
    api(libs.edc.spi.validation)

    api(libs.failsafe.core)
    api(libs.aws.iam)
    api(libs.aws.s3)
    api(libs.aws.sts)

    testImplementation(libs.edc.junit)
}


