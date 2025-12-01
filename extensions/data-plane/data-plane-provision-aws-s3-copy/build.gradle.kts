/*
 *  Copyright (c) 2025 Cofinity-X
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Cofinity-X - initial API and implementation
 *
 */

plugins {
    `java-library`
}

dependencies {
    api(libs.edc.spi.participant.context.single)
    implementation(libs.edc.spi.dataplane)
    implementation(libs.edc.lib.util)
    implementation(project(":extensions:common:aws:aws-s3-core"))
    implementation(project(":extensions:common:s3-copy-lib"))

    testImplementation(testFixtures(project(":extensions:common:aws:aws-s3-test")))
    testImplementation(libs.edc.spi.jsonld)
}


